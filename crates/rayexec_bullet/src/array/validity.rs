use rayexec_error::Result;

use crate::bitmap::Bitmap;

/// Union all validities.
///
/// The final bitmap will be the OR of all bitmaps.
pub(crate) fn union_validities<'a>(
    validities: impl IntoIterator<Item = Option<&'a Bitmap>>,
) -> Result<Option<Bitmap>> {
    let mut unioned: Option<Bitmap> = None;

    for bitmap in validities {
        match (&mut unioned, bitmap) {
            (Some(unioned), Some(bitmap)) => unioned.bit_or_mut(bitmap)?,
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
