use std::{fmt, io};

use rayexec_error::Result;

use super::highlighter::HighlightState;
use super::{debug, vt100};

// TODO: Bug in refresh.
//
// ```
// glaredb> select 1,
//      ... 'oh hey this is a long line, what
//      ... happens when we wrap'
// ```
//
// The second select item above is soft-wrapped. When we position the cursor
// anywhere in the overflow (wrapped) line, then press up, we move to the first
// line. Then moving back down positions the cursor at the wrong offset,
// probably not taking into account prompt width somewhere.

// TODO: History
//
// Just need to add the logic to the relevant up/down methods.

/// Incoming key event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyEvent {
    Backspace,
    ShiftEnter, // Don't have native support in terminals for detecting this.
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

// TODO: Ring buffer, persistence.
#[derive(Debug)]
struct History {
    cursor: usize,
    items: Vec<String>,
}

impl History {
    fn new() -> Self {
        History {
            cursor: 0,
            items: Vec::new(),
        }
    }

    fn push_item(&mut self, item: &str) {
        if let Some(last) = self.items.last() {
            if last == item {
                // Deduplicate, don't store in history.
                return;
            }
        }

        self.items.push(item.into());
        self.cursor = self.items.len() - 1;
    }

    fn next_item_back(&mut self) -> Option<&str> {
        let item = self.items[self.cursor].as_str();
        if self.cursor > 0 {
            self.cursor -= 1;
        }

        Some(item)
    }

    fn next_item_front(&mut self) -> Option<&str> {
        let item = self.items[self.cursor].as_str();
        if self.cursor < self.items.len() - 1 {
            self.cursor += 1;
        }

        Some(item)
    }
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
    /// Current cursor position relative to the input text.
    text_pos: usize,
    /// Row index for where we previously rendered the cursor.
    rendered_cursor_row: usize,
    /// Max number of lines we've edited thus far for a query.
    max_lines: usize,
    /// If the user hti CTRL-C on the previous input.
    ///
    /// Used to exit after the user hits CTRL-C twice in succession. Get's reset
    /// on any other input.
    did_ctrl_c: bool,
    /// Highlight keywords.
    highlighter: HighlightState,
    /// History buffer.
    history: History,
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
            text_pos: 0,
            rendered_cursor_row: 0,
            max_lines: 1, // We never have less than one line.
            did_ctrl_c: false,
            highlighter: HighlightState::new(),
            history: History::new(),
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
            KeyEvent::Char(c) => self.edit_insert_char(c, true)?,
            KeyEvent::Backspace => self.edit_backspace()?,
            KeyEvent::ShiftEnter => {
                self.edit_move_to_end()?;
                self.history.push_item(self.buffer.as_ref());
                return Ok(Signal::InputCompleted(&self.buffer.as_ref()));
            }
            KeyEvent::Enter => {
                if self.is_complete() {
                    self.edit_move_to_end()?;
                    self.history.push_item(self.buffer.as_ref());
                    return Ok(Signal::InputCompleted(&self.buffer.as_ref()));
                }
                self.edit_enter()?;
            }
            KeyEvent::Left => self.edit_move_left()?,
            KeyEvent::Right => self.edit_move_right()?,
            KeyEvent::Up => self.edit_move_up()?,
            KeyEvent::Down => self.edit_move_down()?,
            KeyEvent::End => self.edit_move_to_end()?,
            KeyEvent::CtrlC => {
                self.did_ctrl_c = true;

                // Move to end of input.
                self.edit_move_to_end()?;

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

    fn consume_history(&mut self, text: &str) -> Result<()> {
        self.buffer.clear();
        self.text_pos = 0;
        self.rendered_cursor_row = 0;

        // Note we don't want to reset max lines, as we want to properly
        // overwrite all lines. The resulting query might be fewer lines, but we
        // won't see anything from the previous query.

        self.consume_text(text)
    }

    pub fn edit_start(&mut self) -> Result<()> {
        self.buffer.clear();
        self.text_pos = 0;
        self.rendered_cursor_row = 0;
        self.max_lines = 1;

        Self::write_prompt(&mut self.writer, self.prompt)?;
        self.writer.flush()?;

        Ok(())
    }

    fn edit_move_to_end(&mut self) -> Result<()> {
        if self.text_pos == self.buffer.current.len() {
            return Ok(());
        }
        self.text_pos = self.buffer.current.len();
        self.refresh()
    }

    fn edit_enter(&mut self) -> Result<()> {
        self.edit_insert_char('\n', true)
    }

    fn edit_move_up(&mut self) -> Result<()> {
        let line = self.buffer.current_line(self.text_pos);
        if line == 0 {
            if let Some(item) = self.history.next_item_back() {
                // TODO: Avoid the clone.
                let item = item.to_string();
                self.consume_history(&item)?;
                return Ok(());
            }

            // No more history in that direction.
            return Ok(());
        }

        // Relative position in the current line. We'll use this to try to line
        // up the relative position in the previous line.
        //
        // If our relative position is goes beyond the end of the previous line,
        // we put the position at the end (where the newline char would be).
        let pos_rel = self.text_pos - self.buffer.spans[line].start;
        let prev = self.buffer.spans[line - 1];
        self.text_pos = usize::min(prev.start + prev.len - 1, prev.start + pos_rel);

        self.refresh()?;

        Ok(())
    }

    fn edit_move_down(&mut self) -> Result<()> {
        let line = self.buffer.current_line(self.text_pos);
        if line == self.buffer.spans.len() - 1 {
            if let Some(item) = self.history.next_item_front() {
                // TODO: Avoid the clone.
                let item = item.to_string();
                self.consume_history(&item)?;
                return Ok(());
            }

            // No more history in that direction, do nothing.
            return Ok(());
        }

        // See above.
        let pos_rel = self.text_pos - self.buffer.spans[line].start;
        let next = self.buffer.spans[line + 1];
        self.text_pos = usize::min(next.start + next.len - 1, next.start + pos_rel);

        self.refresh()?;

        Ok(())
    }

    fn edit_move_left(&mut self) -> Result<()> {
        if self.text_pos > 0 {
            self.text_pos -= 1;
            self.refresh()?;
        }
        Ok(())
    }

    fn edit_move_right(&mut self) -> Result<()> {
        if self.text_pos < self.buffer.current.len() {
            self.text_pos += 1;
            self.refresh()?;
        }
        Ok(())
    }

    /// Insert a character.
    fn edit_insert_char(&mut self, ch: char, refresh: bool) -> Result<()> {
        self.buffer.insert_char(self.text_pos, ch);
        self.text_pos += 1;

        if refresh {
            self.refresh()?;
        }

        Ok(())
    }

    fn edit_backspace(&mut self) -> Result<()> {
        if self.text_pos == 0 {
            return Ok(());
        }

        self.buffer.remove_char(self.text_pos);
        self.text_pos -= 1;
        self.refresh()?;

        Ok(())
    }

    /// Refresh the current "line" we're on (which may actually be multiple
    /// lines).
    fn refresh(&mut self) -> Result<()> {
        debug::log(|| {
            format!(
                "max_lines: {}, rendered_cursor_row: {}",
                self.max_lines, self.rendered_cursor_row
            )
        });
        // First clear out the previous lines.
        let row_diff = self.max_lines - self.rendered_cursor_row;
        debug::log(|| format!("row_diff: {row_diff}"));
        if row_diff > 0 {
            // Cursor not on the last line, need to move it to the end.
            vt100::cursor_down(&mut self.writer, row_diff)?;
        }

        // Clear previous rows.
        for _ in 0..self.max_lines - 1 {
            // Clear current line.
            write!(self.writer, "{}", vt100::CR)?;
            write!(self.writer, "{}", vt100::CLEAR_LINE_CURSOR_RIGHT)?;
            vt100::cursor_up(&mut self.writer, 1)?;
        }

        // Clear current/top line.
        write!(self.writer, "{}", vt100::CR)?;
        write!(self.writer, "{}", vt100::CLEAR_LINE_CURSOR_RIGHT)?;

        let buffer = &self.buffer;
        // Tokenize the current query for highlighting.
        self.highlighter.tokenize(&buffer.current);

        // Lines broken up by literal newlines in the input.
        let lines = buffer.lines();

        // Now get the cursor line/col relative to the text input.
        let (pos_line, pos_col) = buffer.current_line_and_column(self.text_pos);
        debug::log(|| format!("position: line: {pos_line}, col: {pos_col}"));

        let mut visual_line_count = 0;
        let mut vis_line = pos_line;
        let mut vis_col = pos_col;

        // Move the visual column to account for the prompt width.
        if pos_line == 0 {
            vis_col += self.prompt.len();
        } else {
            vis_col += self.continuation.len();
        }

        // Write all the lines.
        for (line_idx, hard_line) in lines.enumerate() {
            let prompt_width = if line_idx == 0 {
                // Write prompt for first line.
                Self::write_prompt(&mut self.writer, self.prompt)?;
                self.prompt.len()
            } else {
                // Write continuation markers for hard line breaks.
                write!(self.writer, "{}", vt100::CRLF)?;
                write!(
                    self.writer,
                    "{}{}",
                    vt100::COLOR_FG_BRIGHT_BLACK,
                    self.continuation
                )?;
                self.continuation.len()
            };

            let content_width = self.size.cols - prompt_width;

            // Split lines to ensure they fit in the terminal.
            let split = LineSplitter::new(hard_line, content_width);
            for (soft_line_idx, soft_line) in split.enumerate() {
                if soft_line_idx > 0 {
                    // Contiuation for soft line breaks.
                    write!(self.writer, "{}", vt100::CRLF)?;
                    write!(
                        self.writer,
                        "{}{}",
                        vt100::COLOR_FG_BRIGHT_BLACK,
                        self.continuation
                    )?;

                    if pos_line > line_idx {
                        // We added a soft line break, adjust the visual line to
                        // account for it.
                        vis_line += 1;
                    }

                    if pos_line == line_idx && vis_col > content_width {
                        // Adjust column to be relative to this line.
                        //
                        // Note that if we have multiple softbreaks, this will
                        // be called multiple times until the column is lined up
                        // correctly.
                        vis_col -= content_width;
                        // Adjust visual line as well since we're wrapping here.
                        vis_line += 1;
                    }
                }

                // Highlight each line
                let h_line = self.highlighter.next_chunk_highlight(soft_line);
                for h_str in h_line {
                    h_str.write_vt100_trim_nl(&mut self.writer)?;
                }

                visual_line_count += 1;
            }
        }

        debug::log(|| format!("visual_line_count: {visual_line_count}"));

        // We're done highlighting.
        self.highlighter.clear_highlight(&mut self.writer)?;

        self.max_lines = usize::max(visual_line_count, self.max_lines);
        self.rendered_cursor_row = vis_line;

        debug::log(|| format!("vis_line: {vis_line}, vis_col: {vis_col}"));

        // Row diff relative to the last line indicating how many lines to move
        // up.
        let row_diff = visual_line_count - vis_line - 1;
        if row_diff > 0 {
            vt100::cursor_up(&mut self.writer, row_diff)?;
        }

        // Column
        write!(self.writer, "{}", vt100::CR)?;
        vt100::cursor_right(&mut self.writer, vis_col)?;

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

/// Holds the raw user input, along with info about where lines begin/end.
///
/// All `pos` arguments should indicate where the cursor is relative to the
/// input.
///
/// E.g. for the following:
/// ```text
/// SELECT * FROM table;
///          ^ pos = 9
/// ```
///
/// - An insert will insert before 'F'.
/// - A backspace will remove the space before the 'F'.
///
/// And for newlines:
/// ```text
/// SELECT 1,
/// 2;
/// ```
///
/// - Position 8 is the ','. A backspace will delete the '1'.
/// - Position 9 is immediately after the comma, but still on the first line. A
///   backspace will delete the ','.
/// - Postiion 10 is the '2'. A backspace will delete the newline.
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
            // Cursor is at the end of the input.

            self.current.push(ch);
            self.spans.last_mut().unwrap().len += 1;

            if ch == '\n' {
                // Start a new line.
                self.spans.push(LineSpan {
                    start: pos + 1,
                    len: 0,
                });
            }
        } else {
            self.current.insert(pos, ch);

            let line_idx = self.current_line(pos);
            let line = &mut self.spans[line_idx];
            line.len += 1;

            // Adjust start position of all lines after this one.
            if line_idx != self.spans.len() - 1 {
                let after = &mut self.spans[line_idx + 1..];
                for line in after {
                    line.start += 1;
                }
            }

            if ch == '\n' {
                // Need to split this line in two.
                let line = &self.spans[line_idx];
                let relative = pos - line.start + 1; // Account for the newly appended '\n'.
                let (line, next) = line.split(relative);

                self.spans[line_idx] = line;
                self.spans.insert(line_idx + 1, next);
            }
        }
    }

    fn remove_char(&mut self, pos: usize) {
        if pos == 0 {
            // Avoid popping the only span.
            //
            // This check should be higher up somewhere though.
            return;
        }

        let ch = self.current.remove(pos - 1);

        let mut line_idx = self.current_line(pos);
        let line = self.spans[line_idx];

        if pos == line.start {
            // We're removing a newline char. We want to merge this line
            // into the previous line.
            debug_assert_eq!(ch, '\n');
            debug_assert!(self.spans.len() >= 2);

            let prev = &mut self.spans[line_idx - 1];
            prev.len += line.len;
            prev.len -= 1; // Account for removal of newline char.

            // Remove this line.
            self.spans.remove(line_idx);

            // We're now in the previous line.
            line_idx -= 1;
            // Adjust the start of all lines after this line.
            if line_idx != self.spans.len() - 1 {
                let after = &mut self.spans[line_idx + 1..];
                for line in after {
                    line.start -= 1;
                }
            }
        } else {
            // Otherwise we're deleting from the middle (or end) of a line.
            let line = &mut self.spans[line_idx];
            line.len -= 1;

            // Adjust the start of all lines after this line.
            if line_idx != self.spans.len() - 1 {
                let after = &mut self.spans[line_idx + 1..];
                for line in after {
                    line.start -= 1;
                }
            }
        }
    }

    fn lines(&self) -> LineIter {
        LineIter {
            buffer: self,
            curr: 0,
        }
    }

    /// Returns the line string at the given index.
    ///
    /// Whitespace is not trimmed.
    fn line_str(&self, line_idx: usize) -> Option<&str> {
        if line_idx >= self.spans.len() {
            return None;
        }

        let span = self.spans[line_idx];
        let line = &self.current[span.start..(span.start + span.len)];

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

/// Splits a line into multiple lines if it exceeds a width.
#[derive(Debug)]
struct LineSplitter<'a> {
    /// Separate bool for indicating if we're finished to allow emitting out
    /// empty strings.
    finished: bool,
    width: usize,
    rem: &'a str,
}

impl<'a> LineSplitter<'a> {
    fn new(line: &'a str, width: usize) -> Self {
        LineSplitter {
            finished: false,
            width,
            rem: line,
        }
    }
}

impl<'a> Iterator for LineSplitter<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        if self.rem.len() <= self.width {
            // Line fits into the desired width, nothing more for us to do.
            let s = self.rem;
            self.rem = "";
            self.finished = true;
            return Some(s);
        }

        // Split the line.
        let (line, rem) = self.rem.split_at(self.width);
        self.rem = rem;

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
    fn text_buffer_append_single_line() {
        let mut buf = TextBuffer::new();
        insert_str(&mut buf, 0, "select 1;");

        assert_eq!(Some("select 1;"), buf.line_str(0));
        assert_eq!(None, buf.line_str(1));

        assert_eq!(0, buf.current_line(0));
        assert_eq!(0, buf.current_line(3));
        assert_eq!(0, buf.current_line(8));
    }

    #[test]
    fn text_buffer_insert_single_line() {
        let mut buf = TextBuffer::new();
        insert_str(&mut buf, 0, "select 1;");

        buf.insert_char(7, '6');
        assert_eq!(Some("select 61;"), buf.line_str(0));
    }

    #[test]
    fn text_buffer_insert_newline() {
        let mut buf = TextBuffer::new();
        insert_str(&mut buf, 0, "select 1, 2,\n3;");

        // Cursor on the '2'
        buf.insert_char(10, '\n');

        println!("LINES: {:?}", buf.lines().collect::<Vec<_>>());

        assert_eq!(Some("select 1, \n"), buf.line_str(0));
        assert_eq!(Some("2,\n"), buf.line_str(1));
        assert_eq!(Some("3;"), buf.line_str(2));
    }

    #[test]
    fn text_buffer_append_multiple_lines() {
        let mut buf = TextBuffer::new();
        let pos = insert_str(&mut buf, 0, "select");
        _ = insert_str(&mut buf, pos, "\n1;");

        assert_eq!(Some("select\n"), buf.line_str(0));
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
    fn text_buffer_delete_from_end_of_line() {
        let mut buf = TextBuffer::new();
        let pos = insert_str(&mut buf, 0, "select 1;");
        assert_eq!(9, pos);

        buf.remove_char(9);
        buf.remove_char(8);
        buf.remove_char(7);

        assert_eq!(Some("select"), buf.line_str(0));
    }

    #[test]
    fn text_buffer_delete_from_line() {
        let mut buf = TextBuffer::new();
        let pos = insert_str(&mut buf, 0, "select 61;");
        assert_eq!(10, pos);

        buf.remove_char(8);

        assert_eq!(Some("select 1;"), buf.line_str(0));
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

    #[test]
    fn text_buffer_delete_through_newline_multiple_lines() {
        let mut buf = TextBuffer::new();
        let _ = insert_str(&mut buf, 0, "select 1,\n2,\n3;");

        // Cursor after the '2,' line, delete the line.
        buf.remove_char(12); // Remove ','
        buf.remove_char(11); // Remove '2'
        buf.remove_char(10); // Remove '\n'

        println!("LINES: {:?}", buf.lines().collect::<Vec<_>>());

        assert_eq!(Some("select 1,\n"), buf.line_str(0));
        assert_eq!(Some("3;"), buf.line_str(1));
        assert_eq!(None, buf.line_str(2));
    }

    #[test]
    fn text_buffer_delete_newline_move_line_up() {
        let mut buf = TextBuffer::new();
        let _ = insert_str(&mut buf, 0, "select 1,\n2,\n3;");

        // Cursor on the '2'
        buf.remove_char(10); // Remove '\n'

        println!("LINES: {:?}", buf.lines().collect::<Vec<_>>());
        println!("BUF: {:?}", buf.current);

        assert_eq!(Some("select 1,2,\n"), buf.line_str(0));
        assert_eq!(Some("3;"), buf.line_str(1));
        assert_eq!(None, buf.line_str(2));
    }

    #[test]
    fn text_buffer_position_multiple_empty_lines() {
        let mut buf = TextBuffer::new();
        let _ = insert_str(&mut buf, 0, "select 1,\n\n\n\n2;");

        println!("SPANS: {:?}", buf.spans);

        assert_eq!((0, 8), buf.current_line_and_column(8)); // On comma
        assert_eq!((0, 9), buf.current_line_and_column(9)); // Newline after comma
        assert_eq!((1, 0), buf.current_line_and_column(10));
        assert_eq!((2, 0), buf.current_line_and_column(11));
        assert_eq!((3, 0), buf.current_line_and_column(12));
        assert_eq!((4, 0), buf.current_line_and_column(13)); // '2'
        assert_eq!((4, 1), buf.current_line_and_column(14)); // ';'
    }

    #[test]
    fn line_splitter_fits_in_single_line() {
        let s = "mario luigi";
        let split = LineSplitter::new(s, 80);

        let got: Vec<_> = split.collect();
        let expected = vec!["mario luigi"];

        assert_eq!(expected, got);
    }

    #[test]
    fn line_splitter_split_into_multiple_lines() {
        let s = "mario luigi";
        let split = LineSplitter::new(s, 3);

        let got: Vec<_> = split.collect();
        let expected = vec!["mar", "io ", "lui", "gi"];

        assert_eq!(expected, got);
    }

    #[test]
    fn line_split_emit_empty_string() {
        // If provided an empty string to begin with.
        let split = LineSplitter::new("", 80);

        let got: Vec<_> = split.collect();
        assert_eq!(vec![""], got);
    }
}
