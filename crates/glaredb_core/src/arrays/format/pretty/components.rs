pub const PRETTY_COMPONENTS: TableComponents = TableComponents {
    lb: '│',
    rb: '│',
    tb: '─',
    bb: '─',
    vert: '│',
    hor: '─',
    urc: '┐',
    ulc: '┌',
    brc: '┘',
    blc: '└',
    lb_int: '├',
    rb_int: '┤',
    tb_int: '┬',
    bb_int: '┴',
    intersection: '┼',
};

pub const ASCII_COMPONENTS: TableComponents = TableComponents {
    lb: '|',
    rb: '|',
    tb: '-',
    bb: '-',
    vert: '|',
    hor: '-',
    urc: '+',
    ulc: '+',
    brc: '+',
    blc: '+',
    lb_int: '+',
    rb_int: '+',
    tb_int: '+',
    bb_int: '+',
    intersection: '+',
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableComponents {
    /// Left border.
    pub lb: char,
    /// Right border.
    pub rb: char,
    /// Top border.
    pub tb: char,
    /// Bottom border.
    pub bb: char,

    /// Vertical line.
    pub vert: char,
    /// Horizontal line.
    pub hor: char,

    /// Upper right corner.
    pub urc: char,
    /// Upper left corner.
    pub ulc: char,
    /// Bottom right corner.
    pub brc: char,
    /// Bottom left corner.
    pub blc: char,

    /// Left border intersection.
    pub lb_int: char,
    /// Right border intersection.
    pub rb_int: char,
    /// Top border intersection.
    pub tb_int: char,
    /// Bottom border intersection.
    pub bb_int: char,
    /// Intersection.
    pub intersection: char,
}
