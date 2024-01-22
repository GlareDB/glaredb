/// Get the terminal's width in characters.
//
/// This can be used as the `max_width` argument when pretty formatting to
/// ensure the formatted table doesn't exceed the width of the terminal.
///
/// If the width can't be determined or is unreasonable (0), then a default
/// width of 80 is used.
pub fn term_width() -> usize {
    crossterm::terminal::size()
        .map(|(width, _)| if width == 0 { 80 } else { width as usize })
        .unwrap_or(80)
}
