use crate::{
    raw::RawTerminalWriter,
    vt100::{self, cursor_right},
};
use rayexec_error::Result;
use std::io;

/// Incoming key event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyEvent {
    Backspace,
    Enter,
    Left,
    Right,
    Up,
    Down,
    Home,
    End,
    Tab,
    BackTab,
    Delete,
    Insert,
    CtrlC,
    Char(char),
    Unknown,
}

/// Signal as a response to a key input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal<'a> {
    /// Keep sending input.
    KeepEditing,

    /// Input completed.
    InputCompleted(&'a str),

    /// User requested exit.
    Exit,
}

// TODO: Proper multi-line editing. Currently does some funky formatting when
// pressing enter and it's not the end of a query.
// TODO: History.
#[derive(Debug)]
pub struct LineEditor<W: io::Write> {
    /// Current cursor position.
    pos: usize,

    /// Previous cursor position.
    prev_pos: usize,

    /// Current line length.
    len: usize,

    /// Number of columns in the terminal.
    cols: usize,

    /// Where we'll be writing the output during editing.
    writer: W,

    /// Output buffer containing the user's input.
    buffer: String,

    /// Previous number of rows.
    prev_rows: usize,

    /// Prompt to write at the beginning of every line.
    prompt: &'static str,

    /// If the user hti CTRL-C on the previous input.
    ///
    /// Used to exit after the user hits CTRL-C twice in succession. Get's reset
    /// on any other input.
    did_ctrl_c: bool,
}

impl<W: io::Write> LineEditor<W> {
    pub fn new(writer: W, prompt: &'static str, initial_cols: usize) -> Self {
        LineEditor {
            pos: 0,
            prev_pos: 0,
            len: 0,
            cols: initial_cols,
            writer,
            buffer: String::new(),
            prev_rows: 0,
            prompt,
            did_ctrl_c: false,
        }
    }

    /// Get a mutable reference to the writer.
    pub fn raw_writer(&mut self) -> RawTerminalWriter<W> {
        RawTerminalWriter::new(&mut self.writer)
    }

    pub fn set_cols(&mut self, cols: usize) {
        self.cols = cols
    }

    pub fn get_cols(&self) -> usize {
        self.cols
    }

    pub fn edit_start(&mut self) -> Result<()> {
        self.buffer.clear();
        self.pos = 0;
        self.prev_pos = 0;
        self.len = 0;
        self.prev_rows = 0;

        write!(self.writer, "{}", self.prompt)?;
        self.writer.flush()?;

        Ok(())
    }

    pub fn consume_text(&mut self, text: &str) -> Result<()> {
        for ch in text.chars() {
            self.edit_insert(ch)?;
        }
        Ok(())
    }

    pub fn consume_key(&mut self, key: KeyEvent) -> Result<Signal> {
        if matches!(key, KeyEvent::CtrlC) {
            if self.did_ctrl_c {
                return Ok(Signal::Exit);
            }
        } else {
            self.did_ctrl_c = false;
        }

        match key {
            KeyEvent::Char(c) => {
                self.edit_insert(c)?;
            }
            KeyEvent::Backspace => {
                self.edit_backspace()?;
            }
            KeyEvent::Enter => {
                if self.is_complete() {
                    return Ok(Signal::InputCompleted(&self.buffer));
                }
                self.edit_enter()?;
            }
            KeyEvent::Left => {
                self.edit_move_left()?;
            }
            KeyEvent::Right => {
                self.edit_move_right()?;
            }
            KeyEvent::CtrlC => {
                self.did_ctrl_c = true;
                write!(self.writer, "\r\n")?;
                self.edit_start()?;
            }
            other => unimplemented!("{other:?}"),
        }
        Ok(Signal::KeepEditing)
    }

    fn edit_enter(&mut self) -> Result<()> {
        if self.len > 0 {
            self.edit_insert('\n')?;
        }
        Ok(())
    }

    fn edit_insert(&mut self, ch: char) -> Result<()> {
        if self.len == self.pos {
            self.buffer.push(ch);
        } else {
            self.buffer.insert(self.pos, ch);
        }
        self.len += 1;
        self.pos += 1;
        self.refresh()?;

        Ok(())
    }

    fn edit_backspace(&mut self) -> Result<()> {
        if self.pos > 0 && self.len > 0 {
            self.buffer.remove(self.pos - 1);
            self.pos -= 1;
            self.len -= 1;
            self.refresh()?;
        }
        Ok(())
    }

    fn edit_move_left(&mut self) -> Result<()> {
        if self.pos > 0 {
            self.pos -= 1;
            self.refresh()?;
        }
        Ok(())
    }

    fn edit_move_right(&mut self) -> Result<()> {
        if self.pos < self.len {
            self.pos += 1;
            self.refresh()?;
        }
        Ok(())
    }

    fn refresh(&mut self) -> Result<()> {
        let mut rows = self.rows();
        let relative_row = self.row_position();

        self.prev_rows = rows;

        // Go to last row.
        let row_diff = self.prev_rows.saturating_sub(relative_row);
        if row_diff > 0 {
            vt100::cursor_down(&mut self.writer, row_diff)?;
        }

        // Clear each row and go up.
        for _ in 0..self.prev_rows.saturating_sub(1) {
            write!(self.writer, "\r{}", vt100::CLEAR_LINE_CURSOR_RIGHT)?;
            vt100::cursor_up(&mut self.writer, 1)?;
        }

        // Clear current line.
        write!(self.writer, "\r{}", vt100::CLEAR_LINE_CURSOR_RIGHT)?;

        // Write prompt.
        write!(self.writer, "{}", self.prompt)?;

        // Write buffer out.
        write!(self.writer, "{}", self.buffer)?;

        // Insert newline if we're at the end of the line.
        if self.pos > 0 && self.pos == self.len && (self.pos + self.prompt.len()) % self.cols == 0 {
            write!(self.writer, "\r\n")?;
            rows += 1;
            self.prev_rows = rows;
        }

        // Move cursor to right row.
        let row_diff = rows.saturating_sub(relative_row);
        if row_diff > 0 {
            vt100::cursor_up(&mut self.writer, row_diff)?;
        }

        // Move cursor to right column.
        let col = (self.prompt.len() + self.pos) % self.cols;
        write!(self.writer, "\r")?;
        cursor_right(&mut self.writer, col)?;

        self.prev_pos = self.pos;

        self.writer.flush()?;

        Ok(())
    }

    fn is_complete(&self) -> bool {
        let trimmed = self.buffer.trim();
        trimmed.ends_with(';') || trimmed.starts_with('/')
    }

    /// Compute number of rows of the current buffer.
    const fn rows(&self) -> usize {
        (self.prompt.len() + self.len + self.cols - 1) / self.cols
    }

    /// Get the current row relative to the cursor.
    const fn row_position(&self) -> usize {
        (self.prompt.len() + self.prev_pos + self.cols) / self.cols
    }
}
