use rayexec_error::Result;

use crate::bitmap::Bitmap;

/// Union all validities.
///
/// The final bitmap will be the AND of all bitmaps.
pub(crate) fn union_validities<'a>(
    validities: impl IntoIterator<Item = Option<&'a Bitmap>>,
) -> Result<Option<Bitmap>> {
    let mut unioned: Option<Bitmap> = None;

    for bitmap in validities {
        match (&mut unioned, bitmap) {
            (Some(unioned), Some(bitmap)) => unioned.bit_and_mut(bitmap)?,
            (None, Some(bitmap)) => unioned = Some(bitmap.clone()),
            _ => (),
        }
    }

    Ok(unioned)
}

/// Concat validities.
///
/// If all validities are None, None will be returned.
pub(crate) fn concat_validities<'a>(
    validities: impl IntoIterator<Item = (usize, Option<&'a Bitmap>)>,
) -> Option<Bitmap> {
    let validities: Vec<_> = validities.into_iter().collect();
    if validities.iter().all(|(_, v)| v.is_none()) {
        return None;
    }

    let cap = validities.iter().fold(0, |cap, (len, _)| cap + len);
    let mut validity = Bitmap::with_capacity(cap);

    for (len, bitmap) in validities {
        match bitmap {
            Some(bitmap) => validity.extend(bitmap.iter()),
            None => validity.extend(std::iter::repeat(true).take(len)),
        }
    }

    Some(validity)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn union_none() {
        let got = union_validities([None, None, None]).unwrap();
        assert_eq!(None, got);
    }

    #[test]
    fn union_one_with_many_nones() {
        let a = Bitmap::from_iter([true, false, true]);
        let got = union_validities([None, Some(&a), None]).unwrap();
        assert_eq!(Some(a), got);
    }

    #[test]
    fn union_many_nones() {
        let a = Bitmap::from_iter([true, true, true]);
        let b = Bitmap::from_iter([true, false, true]);
        let c = Bitmap::from_iter([true, true, false]);
        let got = union_validities([Some(&a), Some(&b), Some(&c)]).unwrap();

        let expected = Bitmap::from_iter([true, false, false]);
        assert_eq!(Some(expected), got);
    }
}
