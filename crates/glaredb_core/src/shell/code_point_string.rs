use std::ops::Range;

/// String that tracks byte offsets to each char boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CodePointString {
    inner: String,
    /// Byte offsets of each char boundary.
    ///
    /// Always sorted, length = inner.chars().count()+1
    indices: Vec<usize>,
}

impl CodePointString {
    pub fn new() -> Self {
        Self::from_string(String::new())
    }

    /// Build from a String, scanning all char boundaries once.
    pub fn from_string(s: String) -> Self {
        let mut indices = Vec::with_capacity(s.chars().count() + 1);
        for (byte_idx, _ch) in s.char_indices() {
            indices.push(byte_idx);
        }
        // Now push the final end‐of‐string offset.
        indices.push(s.len());

        CodePointString { inner: s, indices }
    }

    pub fn clear(&mut self) {
        self.inner.clear();
        self.indices.clear();
        self.indices.push(0);
    }

    /// Access the underlying str.
    pub fn as_str(&self) -> &str {
        &self.inner
    }

    /// Number of chars.
    pub fn len(&self) -> usize {
        // Last element is end‑of‑string, so count = len(indices)−1
        self.indices.len().saturating_sub(1)
    }

    #[allow(unused)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the char at code‑point position `idx`.
    #[allow(unused)]
    pub fn char_at(&self, idx: usize) -> Option<char> {
        if idx >= self.len() {
            return None;
        }
        let start = self.indices[idx];
        let end = self.indices[idx + 1];
        self.inner[start..end].chars().next()
    }

    /// Push a char onto the end of the string.
    pub fn push_char(&mut self, ch: char) {
        self.inner.push(ch);
        // New end-of-string after push
        let new_end = self.inner.len();
        // Append the new boundary index
        self.indices.push(new_end);
    }

    /// Insert a `char` at code‑point index `idx`.
    pub fn insert_char(&mut self, idx: usize, ch: char) {
        // Clamp to [0..=len]
        let pos = idx.min(self.len());
        let byte_idx = self.indices[pos];
        self.inner.insert(byte_idx, ch);
        // Since utf8 for `ch` is 1–4 bytes, we need its encoding length
        let ch_len = ch.len_utf8();
        // Update all subsequent indices by ch_len
        for off in &mut self.indices[pos + 1..] {
            *off += ch_len;
        }
        // Insert the new boundary.
        self.indices.insert(pos + 1, byte_idx + ch_len);
    }

    /// Remove and return the char at code‑point index `idx`.
    pub fn remove_char(&mut self, idx: usize) -> Option<char> {
        if idx >= self.len() {
            return None;
        }
        let start = self.indices[idx];
        let end = self.indices[idx + 1];
        // Extract the char.
        let ch = self.inner[start..end].chars().next().unwrap();
        // Remove from the string
        self.inner.replace_range(start..end, "");
        let ch_len = end - start;
        // Adjust subsequent indices.
        for off in &mut self.indices[idx + 1..] {
            *off -= ch_len;
        }
        // Remove this boundary.
        self.indices.remove(idx + 1);
        Some(ch)
    }

    /// Get a str slice by code‑point range.
    pub fn slice_chars(&self, range: Range<usize>) -> Option<&str> {
        if range.start > range.end || range.end > self.len() {
            return None;
        }
        let b_start = self.indices[range.start];
        let b_end = self.indices[range.end];
        Some(&self.inner[b_start..b_end])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        let mut s = CodePointString::from_string("héllo🌍".into());
        assert_eq!(s.len(), 6); // 'h','é','l','l','o','🌍'
        assert_eq!(s.char_at(1), Some('é'));
        s.insert_char(5, '🥳');
        assert_eq!(s.as_str(), "héllo🥳🌍");

        let s = s.slice_chars(1..4).unwrap(); // "éll"
        assert_eq!("éll", s)
    }

    #[test]
    fn empty_string() {
        let s = CodePointString::from_string(String::new());
        assert_eq!(s.len(), 0);
        assert_eq!(s.as_str(), "");
        assert_eq!(s.char_at(0), None);
        assert_eq!(s.slice_chars(0..0), Some(""));
    }

    #[test]
    fn basic_construction_and_indexing() {
        let raw = "héllo🌍";
        let s = CodePointString::from_string(raw.into());
        // 7 codepoints: h,é,l,l,o,🌍
        assert_eq!(s.len(), raw.chars().count());
        assert_eq!(s.as_str(), raw);

        // char_at for each index
        for (i, ch) in raw.chars().enumerate() {
            assert_eq!(s.char_at(i), Some(ch));
        }

        // out of bounds
        assert_eq!(s.char_at(s.len()), None);
    }

    #[test]
    fn slicing_various_ranges() {
        let s = CodePointString::from_string("abcdef".into());
        assert_eq!(s.slice_chars(0..3), Some("abc"));
        assert_eq!(s.slice_chars(3..6), Some("def"));
        assert_eq!(s.slice_chars(2..4), Some("cd"));
        assert_eq!(s.slice_chars(6..6), Some("")); // empty at end
        assert_eq!(s.slice_chars(0..0), Some("")); // empty at start
        assert_eq!(s.slice_chars(4..2), None); // invalid range
        assert_eq!(s.slice_chars(0..7), None); // out of bounds
    }

    #[test]
    fn insert_at_beginning_middle_end() {
        let mut s = CodePointString::from_string("ace".into());
        // insert 'b' at pos 1
        s.insert_char(1, 'b');
        assert_eq!(s.as_str(), "abce");
        // insert at beginning
        s.insert_char(0, '0');
        assert_eq!(s.as_str(), "0abce");
        // insert at end
        let end = s.len();
        s.insert_char(end, 'Z');
        assert_eq!(s.as_str(), "0abceZ");
        // verify char_at sequence
        let expected: String = "0abceZ".chars().collect();
        for (i, ch) in expected.chars().enumerate() {
            assert_eq!(s.char_at(i), Some(ch));
        }
    }

    #[test]
    fn remove_at_beginning_middle_end() {
        let mut s = CodePointString::from_string("0abceZ".into());
        // remove first
        assert_eq!(s.remove_char(0), Some('0'));
        assert_eq!(s.as_str(), "abceZ");
        // remove middle ('b')
        assert_eq!(s.remove_char(1), Some('b'));
        assert_eq!(s.as_str(), "aceZ");
        // remove last
        let last_idx = s.len() - 1;
        assert_eq!(s.remove_char(last_idx), Some('Z'));
        assert_eq!(s.as_str(), "ace");
        // remove out-of-bounds
        assert_eq!(s.remove_char(10), None);
    }

    #[test]
    fn multi_byte_characters() {
        // Everything outside BMP
        let mut s = CodePointString::from_string("🐱🌮".into());
        assert_eq!(s.len(), 2);
        assert_eq!(s.char_at(0), Some('🐱'));
        assert_eq!(s.char_at(1), Some('🌮'));

        // insert another emoji in middle
        s.insert_char(1, '🚀');
        assert_eq!(s.as_str(), "🐱🚀🌮");
        assert_eq!(s.len(), 3);

        // remove it
        assert_eq!(s.remove_char(1), Some('🚀'));
        assert_eq!(s.as_str(), "🐱🌮");
    }

    #[test]
    fn complex_sequence_of_operations() {
        let mut s = CodePointString::from_string("start".into());
        s.insert_char(5, '!'); // "start!"
        s.insert_char(0, '*'); // "*start!"
        s.insert_char(3, '-'); // "*st-art!"
        assert_eq!(s.as_str(), "*st-art!");

        let sliced = s.slice_chars(1..4).unwrap(); // "st-"
        assert_eq!(sliced, "st-");

        // remove a few
        assert_eq!(s.remove_char(0), Some('*')); // "st-art!"
        assert_eq!(s.remove_char(3), Some('a')); // "st-rt!"
        assert_eq!(s.remove_char(s.len() - 1), Some('!')); // "st-rt"
        assert_eq!(s.as_str(), "st-rt");
    }

    #[test]
    fn push_char() {
        let mut s = CodePointString::from_string(String::new());
        assert_eq!(s.len(), 0);
        s.push_char('A');
        assert_eq!(s.len(), 1);
        assert_eq!(s.as_str(), "A");

        s.push_char('Ω');
        assert_eq!(s.len(), 2);
        assert_eq!(s.as_str(), "AΩ");
        assert_eq!(s.char_at(1), Some('Ω'));

        s.push_char('8');
        assert_eq!(s.len(), 3);
        assert_eq!(s.as_str(), "AΩ8");
        assert_eq!(s.char_at(2), Some('8'));
    }
}
