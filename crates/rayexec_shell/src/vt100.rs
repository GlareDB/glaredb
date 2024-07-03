//! VT100 related functionality.
//!
//! https://espterm.github.io/docs/VT100%20escape%20codes.html
#![allow(dead_code)]

use std::io;

pub fn cursor_up(w: &mut impl io::Write, n: usize) -> io::Result<()> {
    write!(w, "\x1b[{}A", n)
}

pub fn cursor_down(w: &mut impl io::Write, n: usize) -> io::Result<()> {
    write!(w, "\x1b[{}B", n)
}

pub fn cursor_left(w: &mut impl io::Write, n: usize) -> io::Result<()> {
    write!(w, "\x1b[{}D", n)
}

pub fn cursor_right(w: &mut impl io::Write, n: usize) -> io::Result<()> {
    write!(w, "\x1b[{}C", n)
}

pub const CLEAR_LINE_CURSOR_RIGHT: &str = "\x1b[0K";
pub const CLEAR_LINE_CURSOR_LEFT: &str = "\x1b[1K";
pub const CLEAR_LINE: &str = "\x1b[2K";
pub const CLEAR_SCREEN_CURSOR_DOWN: &str = "\x1b[0J";
pub const CLEAR_SCREEN_CURSOR_UP: &str = "\x1b[1J";
pub const CLEAR_SCREEN: &str = "\x1b[2J";

pub const COLOR_FG_BLACK: &str = "\x1b[30m";
pub const COLOR_FG_RED: &str = "\x1b[31m";
pub const COLOR_FG_GREEN: &str = "\x1b[32m";
pub const COLOR_FG_YELLOW: &str = "\x1b[33m";
pub const COLOR_FG_BLUE: &str = "\x1b[34m";
pub const COLOR_FG_MAGENTA: &str = "\x1b[35m";
pub const COLOR_FG_CYAN: &str = "\x1b[36m";
pub const COLOR_FG_WHITE: &str = "\x1b[37m";
pub const COLOR_FG_BRIGHT_BLACK: &str = "\x1b[90m";
pub const COLOR_FG_BRIGHT_YELLOW: &str = "\x1b[93m";
pub const COLOR_BG_BLACK: &str = "\x1b[40m";
pub const COLOR_BG_RED: &str = "\x1b[41m";
pub const COLOR_BG_GREEN: &str = "\x1b[42m";
pub const COLOR_BG_YELLOW: &str = "\x1b[43m";
pub const COLOR_BG_MAGENTA: &str = "\x1b[44m";
pub const COLOR_BG_CYAN: &str = "\x1b[46m";
pub const COLOR_BG_WHITE: &str = "\x1b[47m";
pub const COLOR_BG_BRIGHT_BLACK: &str = "\x1b[100m";
pub const COLOR_BG_BRIGHT_RED: &str = "\x1b[101m";
pub const COLOR_BG_BRIGHT_YELLOW: &str = "\x1b[103m";
pub const COLOR_BG_BRIGHT_WHITE: &str = "\x1b[107m";

pub const MODES_OFF: &str = "\x1b[0m";
pub const MODE_BOLD: &str = "\x1b[1m";
pub const MODE_UNDERLINE: &str = "\x1b[4m";
pub const MODE_BLINK: &str = "\x1b[6m";
pub const MODE_REVERSE: &str = "\x1b[7m";
