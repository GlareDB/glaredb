use crate::messages::*;
use anyhow::{anyhow, Result};
use bytes::{BufMut, BytesMut};
use postgres_protocol::message::{backend::Message, frontend};
use postgres_protocol::IsNull;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

/// Walk the directory, running each test file against some Postgres compatible
/// server.
///
/// Each file will open a unique connection, with each test case in that file
/// being ran sequentially using that connection.
pub fn walk(
    dir: String,
    addr: String,
    options: HashMap<String, String>,
    password: Option<String>,
    timeout: Duration,
) {
    datadriven::walk(&dir, |file| {
        let mut conn = PgConn::connect(&addr, &options, &password, timeout).unwrap();
        file.run(|testcase| match testcase.directive.as_str() {
            "send" => run_send(&mut conn, &testcase.args, &testcase.input),
            "until" => run_until(&mut conn, &testcase.args, &testcase.input, timeout),
            unknown => panic!("unknown directive: {}", unknown),
        });
    });
}

/// Run a "send" directive.
///
/// No output is expected for this directive.
fn run_send(conn: &mut PgConn, _args: &HashMap<String, Vec<String>>, input: &str) -> String {
    for line in input.lines() {
        let mut ss = line.splitn(2, ' ');
        let typ = ss.next().unwrap();
        let json = ss.next().unwrap_or("{}");

        conn.write(|buf| match typ {
            "Query" => {
                let val: Query = serde_json::from_str(json)?;
                frontend::query(&val.query, buf)?;
                Ok(())
            }
            "Parse" => {
                let val: Parse = serde_json::from_str(json)?;
                frontend::parse(&val.name.unwrap_or_default(), &val.query, Vec::new(), buf)?;
                Ok(())
            }
            "Bind" => {
                let val: Bind = serde_json::from_str(json)?;
                frontend::bind(
                    &val.portal.unwrap_or_default(),
                    &val.statement.unwrap_or_default(),
                    Vec::new(),
                    val.values.unwrap_or_default(),
                    |v, buf| {
                        buf.put_slice(v.as_bytes());
                        Ok(IsNull::No)
                    },
                    val.result_formats.unwrap_or_default(),
                    buf,
                )
                .map_err(|e| match e {
                    frontend::BindError::Conversion(e) => anyhow!("conversion: {:?}", e),
                    frontend::BindError::Serialization(e) => anyhow!("serialization: {:?}", e),
                })?;
                Ok(())
            }
            "Execute" => {
                let val: Execute = serde_json::from_str(json)?;
                frontend::execute(
                    &val.portal.unwrap_or_default(),
                    val.max_rows.unwrap_or(0),
                    buf,
                )?;
                Ok(())
            }
            "Sync" => {
                frontend::sync(buf);
                Ok(())
            }
            unknown => panic!("unknown type: {}", unknown),
        })
        .unwrap();
    }
    "".to_string()
}

/// Run the "until" directive.
///
/// Continually reads messages from the connection until either all expected
/// have been read. Panics on timeout when reading any one message.
fn run_until(
    conn: &mut PgConn,
    _args: &HashMap<String, Vec<String>>,
    input: &str,
    timeout: Duration,
) -> String {
    let mut human_strings = Vec::new();
    for expected_typ in input.lines() {
        loop {
            let (id, msg) = conn.read_message(timeout).unwrap();
            let human = SerializedMessage::try_from((id, msg)).unwrap();
            human_strings.push(human.to_string());
            if human.typ == expected_typ {
                break;
            }
        }
    }

    human_strings.join("\n") + "\n"
}

struct PgConn {
    conn: TcpStream,
    read_buf: BytesMut,
    write_buf: BytesMut,
}

impl PgConn {
    /// Connect to a given postgres compatible server, going through the initial
    /// startup flow.
    ///
    /// Providing a password will initiate the cleartext password flow.
    /// Providing a password when the server isn't expecting one will result in
    /// an error.
    fn connect(
        addr: &str,
        options: &HashMap<String, String>,
        password: &Option<String>,
        timeout: Duration,
    ) -> Result<Self> {
        let conn = TcpStream::connect(addr)?;
        conn.set_write_timeout(Some(timeout))?;
        conn.set_read_timeout(Some(timeout))?;
        let mut pg = PgConn {
            conn,
            read_buf: BytesMut::new(),
            write_buf: BytesMut::new(),
        };

        pg.write(|buf| {
            frontend::startup_message(
                options
                    .iter()
                    .map(|(key, val)| (key.as_str(), val.as_str())),
                buf,
            )?;
            Ok(())
        })?;

        if let Some(password) = password {
            let (id, msg) = pg.read_message(timeout)?;
            match msg {
                Message::AuthenticationCleartextPassword => {
                    pg.write(|buf| {
                        frontend::password_message(password.as_bytes(), buf)?;
                        Ok(())
                    })?;
                }
                _ => {
                    return Err(anyhow!(
                        "received unexpected message during authentication: {}",
                        id
                    ))
                }
            }
        }

        let (id, msg) = pg.read_message(timeout)?;
        match msg {
            Message::AuthenticationOk => (),
            _ => {
                return Err(anyhow!(
                    "received unexpected message during startup: {}",
                    id
                ))
            }
        }

        // We may get additional messages back, just need to wait for the first
        // "ReadyForQuery" before we know we can move forward.
        loop {
            let (_, msg) = pg.read_message(timeout)?;
            if let Message::ReadyForQuery(_) = msg {
                break;
            }
        }

        Ok(pg)
    }

    /// Write to the connection with the write buffer filled by the provided
    /// function.
    fn write<F>(&mut self, f: F) -> Result<()>
    where
        F: Fn(&mut BytesMut) -> Result<()>,
    {
        self.write_buf.clear();
        f(&mut self.write_buf)?;
        self.conn.write_all(&self.write_buf)?;
        Ok(())
    }

    /// Read a message from the backend.
    ///
    /// Returns the message type identifier as well for debuggability.
    fn read_message(&mut self, timeout: Duration) -> Result<(char, Message)> {
        let now = Instant::now();
        let mut buf = [0; 1024];
        loop {
            if now.elapsed() > timeout {
                return Err(anyhow!("timeout exceeded: {:?}", timeout));
            }

            let id = *self.read_buf.first().unwrap_or(&0);
            if let Some(msg) = Message::parse(&mut self.read_buf)? {
                return Ok((id as char, msg));
            }

            let n = self.conn.read(&mut buf)?;
            self.read_buf.extend_from_slice(&buf[0..n]);
        }
    }
}
