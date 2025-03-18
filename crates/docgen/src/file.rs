use std::fmt::Write as _;
use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::sync::LazyLock;

use glaredb_error::{DbError, Result};
use regex::Regex;
use tracing::info;

use crate::section::SectionWriter;
use crate::session::DocsSession;

static DOCSGEN_START_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"<!--\s*DOCSGEN_START\s+([a-zA-Z0-9_]+)\s*-->").unwrap());

static DOCSGEN_END_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"<!--\s*DOCSGEN_END\s*-->").unwrap());

fn expand_path(path: &str) -> String {
    format!("{}/../../{}", env!("CARGO_MANIFEST_DIR"), path)
}

#[derive(Debug)]
pub struct DocFile {
    pub path: &'static str,
    pub sections: &'static [(&'static str, &'static dyn SectionWriter)],
}

impl DocFile {
    pub fn overwrite(&self, session: &DocsSession) -> Result<()> {
        let path = expand_path(self.path);
        info!(%path, "expanded path");

        let file = fs::OpenOptions::new().read(true).write(true).open(&path)?;

        let lines: Vec<String> = BufReader::new(&file).lines().collect::<Result<_, _>>()?;

        // Write to buffer instead of file directly in case we error early.
        let mut buf = String::new();

        let mut in_docsgen_section = false;

        for (idx, line) in lines.iter().enumerate() {
            match DOCSGEN_START_REGEX.captures(line) {
                Some(captures) => {
                    if in_docsgen_section {
                        return Err(DbError::new("Cannot nest docsgen sections")
                            .with_field("line_number", idx + 1));
                    }
                    in_docsgen_section = true;

                    let section_name = captures.get(1).unwrap().as_str();

                    let section = self
                        .sections
                        .iter()
                        .find_map(|(name, section)| {
                            if *name == section_name {
                                Some(section)
                            } else {
                                None
                            }
                        })
                        .ok_or_else(|| {
                            DbError::new(format!("Missing docs section: {section_name}"))
                        })?;

                    // Write original line + extra newline
                    writeln!(buf, "{}", line)?;
                    writeln!(buf)?;

                    // Write out section.
                    section.write(session, &mut buf)?;
                }
                None => {
                    if DOCSGEN_END_REGEX.is_match(line.as_str()) {
                        if !in_docsgen_section {
                            return Err(DbError::new(
                                "Found DOCSGEN_END tag when not in a docsgen section",
                            )
                            .with_field("line_number", idx + 1));
                        }

                        in_docsgen_section = false;

                        // Write extra newline + original line
                        writeln!(buf)?;
                        writeln!(buf, "{}", line)?;
                    } else {
                        // Only write out stuff outside of the docgen section.
                        // We already wrote the new output, so we need to
                        // discard the old stuff.
                        if !in_docsgen_section {
                            writeln!(buf, "{}", line)?;
                        }
                    }
                }
            }
        }

        if in_docsgen_section {
            return Err(DbError::new(
                "Reached end of file, still in docsgen section",
            ));
        }

        let file = fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)?;

        let mut writer = BufWriter::new(file);
        writer.write_all(buf.as_bytes())?;
        writer.flush()?;

        Ok(())
    }
}
