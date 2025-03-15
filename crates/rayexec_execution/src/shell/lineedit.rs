use std::{fmt, io};

use rayexec_error::Result;

use super::highlighter::HighlightState;
use super::{debug, vt100};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TermSize {
    pub cols: usize,
}

/// Line editor inspired by linenoise.
#[derive(Debug)]
pub struct LineEditor<W: io::Write> {
    /// Prompt prepended to each line.
    prompt: &'static str,
    /// Continuation string to use for multiline editing.
    continuation: &'static str,
    /// Buffer containing the user-provided text.
    buffer: TextBuffer,
    /// Writer accepting formatted output containing terminal codes.
    writer: W,
    /// Current terminal size.
    size: TermSize,
    /// Current cursor position.
    pos: usize,
    /// Number of lines we called `refresh` with previously.
    ///
    /// This is used to clear the old line. We don't use the current number of
    /// lines since the might have just entered in an '\n' which wouldn't be
    /// reflected in the output.
    prev_lines: usize,
    /// If the user hti CTRL-C on the previous input.
    ///
    /// Used to exit after the user hits CTRL-C twice in succession. Get's reset
    /// on any other input.
    did_ctrl_c: bool,
    /// Highlight keywords.
    highlighter: HighlightState,
}

impl<W> LineEditor<W>
where
    W: io::Write,
{
    pub fn new(
        writer: W,
        prompt: &'static str,
        continuation: &'static str,
        size: TermSize,
    ) -> Self {
        debug::log(|| "init");
        LineEditor {
            prompt,
            continuation,
            buffer: TextBuffer::new(),
            writer,
            size,
            pos: 0,
            prev_lines: 1, // We never have less than one line.
            did_ctrl_c: false,
            highlighter: HighlightState::new(),
        }
    }

    pub fn set_size(&mut self, size: TermSize) {
        self.size = size;
    }

    pub fn get_size(&self) -> TermSize {
        self.size
    }

    pub fn writer_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    pub fn consume_key(&mut self, key: KeyEvent) -> Result<Signal> {
        if matches!(key, KeyEvent::CtrlC) {
            if self.did_ctrl_c {
                write!(self.writer, "{}", vt100::CRLF)?;
                self.writer.flush()?;

                return Ok(Signal::Exit);
            }
        } else {
            self.did_ctrl_c = false;
        }

        match key {
            KeyEvent::Char(c) => {
                self.edit_insert_char(c, true)?;
            }
            KeyEvent::Backspace => {
                self.edit_backspace()?;
            }
            KeyEvent::Enter => {
                if self.is_complete() {
                    return Ok(Signal::InputCompleted(&self.buffer.as_ref()));
                }
                self.edit_enter()?;
            }
            KeyEvent::Left => {
                self.edit_move_left()?;
            }
            KeyEvent::Right => {
                self.edit_move_right()?;
            }
            KeyEvent::Up => {
                self.edit_move_up()?;
            }
            KeyEvent::Down => {
                self.edit_move_down()?;
            }
            KeyEvent::CtrlC => {
                self.did_ctrl_c = true;
                write!(self.writer, "{}", vt100::CRLF)?;
                self.edit_start()?;
            }
            _ => (),
        }
        Ok(Signal::KeepEditing)
    }

    pub fn consume_text(&mut self, text: &str) -> Result<()> {
        for ch in text.chars() {
            self.edit_insert_char(ch, false)?;
        }
        self.refresh()
    }

    pub fn edit_start(&mut self) -> Result<()> {
        self.buffer.clear();
        self.pos = 0;
        self.prev_lines = 1;

        Self::write_prompt(&mut self.writer, self.prompt)?;
        self.writer.flush()?;

        Ok(())
    }

    fn edit_enter(&mut self) -> Result<()> {
        self.edit_insert_char('\n', true)
    }

    fn edit_move_up(&mut self) -> Result<()> {
        let line = self.buffer.current_line(self.pos);
        if line == 0 {
            // TODO: History
            return Ok(());
        }

        // Relative position in the current line. We'll use this to try to line
        // up the relative position in the previous line.
        let pos_rel = self.pos - self.buffer.spans[line].start;

        let prev = self.buffer.spans[line - 1];
        self.pos = usize::min(prev.start + prev.len, prev.start + pos_rel);

        self.refresh()?;

        Ok(())
    }

    fn edit_move_down(&mut self) -> Result<()> {
        let line = self.buffer.current_line(self.pos);
        if line == self.buffer.spans.len() - 1 {
            // TODO: History
            return Ok(());
        }

        // See above.
        let pos_rel = self.pos - self.buffer.spans[line].start;

        let next = self.buffer.spans[line + 1];
        self.pos = usize::min(next.start + next.len, next.start + pos_rel);

        self.refresh()?;

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
        if self.pos < self.buffer.current.len() {
            self.pos += 1;
            self.refresh()?;
        }
        Ok(())
    }

    /// Insert a character.
    fn edit_insert_char(&mut self, ch: char, refresh: bool) -> Result<()> {
        self.buffer.insert_char(self.pos, ch);
        self.pos += 1;

        if refresh {
            self.refresh()?;
        }

        Ok(())
    }

    fn edit_backspace(&mut self) -> Result<()> {
        if self.pos == 0 {
            return Ok(());
        }

        self.buffer.remove_char(self.pos);
        self.pos -= 1;
        self.refresh()?;

        Ok(())
    }

    /// Refresh the current "line" we're on (which may actually be multiple
    /// lines).
    fn refresh(&mut self) -> Result<()> {
        // Position relative to the buffer.
        let (pos_line, pos_col) = self.buffer.current_line_and_column(self.pos);
        debug::log(|| format!("position: line: {pos_line}, col: {pos_col}"));

        let row_diff = self.buffer.spans.len() - 1 - pos_line;
        debug::log(|| format!("row_diff: {row_diff}"));
        if row_diff > 0 {
            // Cursor not on the last line, need to move it to the end.
            vt100::cursor_down(&mut self.writer, row_diff)?;
        }

        // Clear previous rows.
        for _ in 0..self.prev_lines - 1 {
            // Clear current line.
            write!(self.writer, "{}", vt100::CR)?;
            write!(self.writer, "{}", vt100::CLEAR_LINE_CURSOR_RIGHT)?;
            vt100::cursor_up(&mut self.writer, 1)?;
        }

        // Clear current line.
        write!(self.writer, "{}", vt100::CR)?;
        write!(self.writer, "{}", vt100::CLEAR_LINE_CURSOR_RIGHT)?;

        let buffer = &mut self.buffer;
        // Tokenize the current query for highlighting.
        self.highlighter.tokenize(&buffer.current);

        let mut lines = buffer.lines();

        // Write the prompt with the first line.
        Self::write_prompt(&mut self.writer, self.prompt)?;
        let first = lines.next().unwrap();
        let h_first = self.highlighter.next_chunk_highlight(first);
        for h_str in h_first {
            h_str.write_vt100(&mut self.writer)?;
        }

        // Now write each following line with a continuation string.
        for line in lines {
            // Don't highlight continuations.
            self.highlighter.clear_highlight(&mut self.writer)?;
            write!(self.writer, "{}", vt100::CRLF)?;
            write!(self.writer, "{}", self.continuation)?;

            // Highlight each line
            let h_line = self.highlighter.next_chunk_highlight(line);
            for h_str in h_line {
                h_str.write_vt100(&mut self.writer)?;
            }
        }

        // We're done highlighting.
        self.highlighter.clear_highlight(&mut self.writer)?;

        // TODO: Need to account for automatic wrapping for lines longer than
        // terminal width.
        self.prev_lines = self.buffer.spans.len();

        // Now move cursor to correct line/column
        let line = pos_line; // TODO: Compute overflow.

        let col = if line == 0 {
            // Adjust using width of "normal" prompt.
            pos_col + self.prompt.len()
        } else {
            // Adjust using width of continuation.
            pos_col + self.continuation.len()
        };
        debug::log(|| format!("updated: line: {line}, col: {col}"));

        // Row diff relative to the last line indicating how many lines to move
        // up.
        let row_diff = self.buffer.spans.len() - 1 - line;
        if row_diff > 0 {
            vt100::cursor_up(&mut self.writer, row_diff)?;
        }

        // Column
        write!(self.writer, "{}", vt100::CR)?;
        vt100::cursor_right(&mut self.writer, col)?;

        self.writer.flush()?;

        Ok(())
    }

    fn is_complete(&self) -> bool {
        let trimmed = self.buffer.as_ref().trim();
        trimmed.ends_with(';') || trimmed.starts_with('/')
    }

    fn write_prompt(writer: &mut W, prompt: &str) -> Result<()> {
        write!(writer, "{}{prompt}{}", vt100::MODE_BOLD, vt100::MODES_OFF)?;
        Ok(())
    }
}

/// Represents a complete line in the user-provided text.
///
/// The end of a span should either be a newline or the current end of user
/// input.
///
/// Spans include the newline since a user should be able to position their
/// cursor on at the very end of the line on an "empty" space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LineSpan {
    start: usize,
    len: usize,
}

impl LineSpan {
    const TEXT_EMPTY: LineSpan = LineSpan { start: 0, len: 0 };

    fn split(&self, at_relative: usize) -> (LineSpan, LineSpan) {
        let left = LineSpan {
            start: self.start,
            len: at_relative,
        };

        let right = LineSpan {
            start: self.start + at_relative,
            len: self.len - at_relative,
        };

        (left, right)
    }
}

#[derive(Debug)]
struct TextBuffer {
    spans: Vec<LineSpan>,
    current: String,
}

impl TextBuffer {
    fn new() -> Self {
        TextBuffer {
            spans: vec![LineSpan::TEXT_EMPTY],
            current: String::new(),
        }
    }

    fn clear(&mut self) {
        self.spans.clear();
        self.spans.push(LineSpan::TEXT_EMPTY);

        self.current.clear();
    }

    /// Get the line for the given position.
    fn current_line(&self, pos: usize) -> usize {
        self.spans
            .iter()
            .position(|s| {
                let lower = s.start;
                let upper = s.start + s.len;

                pos >= lower && pos < upper
            })
            .unwrap_or_else(|| self.spans.len() - 1)
    }

    fn current_line_and_column(&self, pos: usize) -> (usize, usize) {
        let line = self.current_line(pos);
        let col = pos - self.spans[line].start;
        (line, col)
    }

    fn insert_char(&mut self, pos: usize, ch: char) {
        if pos == self.current.len() {
            self.spans.last_mut().unwrap().len += 1;
            self.current.push(ch);

            if ch == '\n' {
                debug::log(|| "hello?");
                // Start a new line.
                self.spans.push(LineSpan {
                    start: pos + 1,
                    len: 0,
                });
            }
        } else {
            unimplemented!()
            // // Need to find which line we're currently in.
            // let span_pos = self.current_line(pos);

            // // Need to split span into two.
            // let span = self.spans[span_pos];
            // let at_relative = pos - span.start;
            // let (left, right) = span.split(at_relative);

            // self.spans[span_pos] = left;
            // self.spans.insert(span_pos + 1, right);

            // // Adjust all spans after the on we just inserted to account for the
            // // new character.
            // for span in &mut self.spans[span_pos + 1..] {
            //     span.start += 1;
            // }

            // self.buffer.push(ch);
        }
    }

    fn remove_char(&mut self, pos: usize) {
        if pos == 0 {
            // Avoid popping the only span.
            //
            // This check should be higher up somewhere though.
            return;
        }

        if pos == self.current.len() {
            let last = self.spans.last().unwrap();
            if last.len == 0 {
                // We're deleting through a newline, just remove this span.
                self.spans.pop().unwrap();
            }

            // We want to remove from this span even if we removed one as this
            // would remove the "newline" position.
            self.spans.last_mut().unwrap().len -= 1;
            self.current.remove(pos - 1);
        } else {
            // TODO: ...
        }
    }

    fn lines(&self) -> LineIter {
        LineIter {
            buffer: self,
            curr: 0,
        }
    }

    fn line_str(&self, line_idx: usize) -> Option<&str> {
        if line_idx >= self.spans.len() {
            return None;
        }

        let span = self.spans[line_idx];
        let mut line = &self.current[span.start..(span.start + span.len)];

        // Trim newline characters from the end. We're going to be the ones
        // responsible for writing the newlines.
        line = line.trim_end_matches('\n');

        Some(line)
    }
}

impl AsRef<str> for TextBuffer {
    fn as_ref(&self) -> &str {
        self.current.as_str()
    }
}

impl fmt::Display for TextBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.current)
    }
}

/// Iterate over each line in a text buffer.
#[derive(Debug)]
struct LineIter<'a> {
    buffer: &'a TextBuffer,
    curr: usize,
}

impl<'a> Iterator for LineIter<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        let line = self.buffer.line_str(self.curr)?;
        self.curr += 1;
        Some(line)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to insert the string into the text buffer.
    ///
    /// Returns the updated position.
    fn insert_str(buf: &mut TextBuffer, mut pos: usize, s: &str) -> usize {
        for ch in s.chars() {
            buf.insert_char(pos, ch);
            pos += 1;
        }
        pos
    }

    #[test]
    fn text_buffer_insert_single_line() {
        let mut buf = TextBuffer::new();
        insert_str(&mut buf, 0, "select 1;");

        assert_eq!(Some("select 1;"), buf.line_str(0));
        assert_eq!(None, buf.line_str(1));

        assert_eq!(0, buf.current_line(0));
        assert_eq!(0, buf.current_line(3));
        assert_eq!(0, buf.current_line(8));
    }

    #[test]
    fn text_buffer_insert_multiple_lines() {
        let mut buf = TextBuffer::new();
        let pos = insert_str(&mut buf, 0, "select");
        _ = insert_str(&mut buf, pos, "\n1;");

        assert_eq!(Some("select"), buf.line_str(0));
        assert_eq!(Some("1;"), buf.line_str(1));
        assert_eq!(None, buf.line_str(2));

        assert_eq!((0, 0), buf.current_line_and_column(0));
        assert_eq!((0, 5), buf.current_line_and_column(5));
        assert_eq!((0, 6), buf.current_line_and_column(6)); // On the "newline"
        assert_eq!((1, 0), buf.current_line_and_column(7));
        assert_eq!((1, 1), buf.current_line_and_column(8));

        // 1 beyond the end. This would indicate the user's cursor is at the
        // very end of the text, but no line actually contains it. For the
        // purpose of computing line/colum, we want to say that this cursor is
        // in the last line.
        assert_eq!((1, 2), buf.current_line_and_column(9));
    }

    #[test]
    fn text_buffer_delete_from_line() {
        let mut buf = TextBuffer::new();
        let pos = insert_str(&mut buf, 0, "select 1;");
        assert_eq!(9, pos);

        buf.remove_char(9);
        buf.remove_char(8);
        buf.remove_char(7);

        assert_eq!(Some("select"), buf.line_str(0));
    }

    #[test]
    fn text_buffer_delete_through_newline() {
        let mut buf = TextBuffer::new();
        let pos = insert_str(&mut buf, 0, "select");
        let pos = insert_str(&mut buf, pos, "\n1;");
        assert_eq!(9, pos);

        buf.remove_char(9);
        buf.remove_char(8);
        buf.remove_char(7);
        buf.remove_char(6);

        assert_eq!(Some("selec"), buf.line_str(0));
        assert_eq!(None, buf.line_str(1));
    }
}
