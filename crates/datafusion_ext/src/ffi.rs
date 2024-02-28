use std::ffi::c_void;
use std::mem::ManuallyDrop;

use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ffi::FFI_ArrowSchema;
use datafusion::logical_expr::{Signature, TypeSignature, Volatility};

#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(non_camel_case_types)]
pub struct FFI_TypeSignature {
    kind: TypeSignatureKind,
    n_data_types: usize,
    dtypes: *mut *mut FFI_ArrowSchema,
    n_args: usize,
    n_children: usize,
    children: *mut *mut FFI_TypeSignature,
    release: Option<unsafe extern "C" fn(arg1: *mut FFI_TypeSignature)>,
    private_data: *mut c_void,
}

impl FFI_TypeSignature {
    pub fn empty() -> Self {
        Self {
            kind: TypeSignatureKind::Any,
            n_data_types: 0,
            n_children: 0,
            n_args: 0,
            dtypes: std::ptr::null_mut(),
            children: std::ptr::null_mut(),
            release: None,
            private_data: std::ptr::null_mut(),
        }
    }
    pub fn is_null(&self) -> bool {
        self.private_data.is_null()
    }
}

impl Drop for FFI_TypeSignature {
    fn drop(&mut self) {
        match self.release {
            None => (),
            Some(release) => unsafe { release(self) },
        };
    }
}

unsafe impl Send for FFI_TypeSignature {}

struct TypeSignaturePrivateData {
    dtypes: Box<[*mut FFI_ArrowSchema]>,
    children: Box<[*mut FFI_TypeSignature]>,
}

unsafe extern "C" fn release_type_signature(arg1: *mut FFI_TypeSignature) {
    if arg1.is_null() {
        return;
    }

    let type_signature = &mut *arg1;
    let private_data = Box::from_raw(type_signature.private_data as *mut TypeSignaturePrivateData);

    for ptr in private_data.children.iter() {
        // take ownership back to release it.
        let _ = Box::from_raw(*ptr as *mut ManuallyDrop<FFI_TypeSignature>);
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TypeSignatureKind {
    Variadic,
    VariadicEqual,
    VariadicAny,
    Uniform,
    Exact,
    Any,
    OneOf,
    ArrayAndElement,
    ElementAndArray,
}

impl TryFrom<TypeSignature> for FFI_TypeSignature {
    type Error = ArrowError;

    fn try_from(value: TypeSignature) -> Result<Self, Self::Error> {
        match value {
            TypeSignature::Variadic(dtypes) => {
                let mut this = Self::empty();
                this.kind = TypeSignatureKind::Variadic;

                let dtypes: Box<[*mut FFI_ArrowSchema]> = dtypes
                    .into_iter()
                    .map(|dtype| {
                        FFI_ArrowSchema::try_from(dtype)
                            .map(Box::new)
                            .map(Box::into_raw)
                    })
                    .collect::<Result<Box<_>, Self::Error>>()?;


                this.n_data_types = dtypes.len();
                let mut private_data = Box::new(TypeSignaturePrivateData {
                    dtypes,
                    children: Box::new([]),
                });

                this.dtypes = private_data.dtypes.as_mut_ptr();
                this.private_data = Box::into_raw(private_data) as *mut c_void;
                this.release = Some(release_type_signature);

                Ok(this)
            }
            TypeSignature::VariadicEqual => {
                let mut this = Self::empty();
                this.kind = TypeSignatureKind::VariadicEqual;
                Ok(this)
            }
            TypeSignature::VariadicAny => {
                let mut this = Self::empty();
                this.kind = TypeSignatureKind::VariadicAny;
                Ok(this)
            }
            TypeSignature::Uniform(size, dtypes) => {
                let mut this = Self::empty();
                this.kind = TypeSignatureKind::Uniform;

                let dtypes: Box<[*mut FFI_ArrowSchema]> = dtypes
                    .into_iter()
                    .map(|dtype| {
                        FFI_ArrowSchema::try_from(dtype)
                            .map(Box::new)
                            .map(Box::into_raw)
                    })
                    .collect::<Result<Box<_>, Self::Error>>()?;


                this.n_data_types = dtypes.len();
                let mut private_data = Box::new(TypeSignaturePrivateData {
                    dtypes,
                    children: Box::new([]),
                });

                this.n_args = size;
                this.dtypes = private_data.dtypes.as_mut_ptr();
                this.private_data = Box::into_raw(private_data) as *mut c_void;
                this.release = Some(release_type_signature);

                Ok(this)
            }

            TypeSignature::Exact(dtypes) => {
                let mut this = Self::empty();
                this.kind = TypeSignatureKind::Exact;

                let dtypes: Box<[*mut FFI_ArrowSchema]> = dtypes
                    .into_iter()
                    .map(|dtype| {
                        FFI_ArrowSchema::try_from(dtype)
                            .map(Box::new)
                            .map(Box::into_raw)
                    })
                    .collect::<Result<Box<_>, Self::Error>>()?;


                this.n_data_types = dtypes.len();
                let mut private_data = Box::new(TypeSignaturePrivateData {
                    dtypes,
                    children: Box::new([]),
                });

                this.dtypes = private_data.dtypes.as_mut_ptr();
                this.private_data = Box::into_raw(private_data) as *mut c_void;
                this.release = Some(release_type_signature);

                Ok(this)
            }
            TypeSignature::Any(size) => {
                let mut this = Self::empty();
                this.kind = TypeSignatureKind::Any;
                this.n_args = size;
                Ok(this)
            }
            TypeSignature::OneOf(type_signatures) => {
                let mut this = Self::empty();
                this.kind = TypeSignatureKind::OneOf;

                let children: Box<[*mut FFI_TypeSignature]> = type_signatures
                    .into_iter()
                    .map(|type_signature| {
                        FFI_TypeSignature::try_from(type_signature)
                            .map(Box::new)
                            .map(Box::into_raw)
                    })
                    .collect::<Result<Box<_>, Self::Error>>()?;

                this.n_children = children.len();
                let mut private_data = Box::new(TypeSignaturePrivateData {
                    dtypes: Box::new([]),
                    children,
                });
                this.children = private_data.children.as_mut_ptr();
                this.private_data = Box::into_raw(private_data) as *mut c_void;
                this.release = Some(release_type_signature);
                Ok(this)
            }
            TypeSignature::ArrayAndElement => {
                let mut this = Self::empty();
                this.kind = TypeSignatureKind::ArrayAndElement;
                Ok(this)
            }
            TypeSignature::ElementAndArray => {
                let mut this = Self::empty();
                this.kind = TypeSignatureKind::ElementAndArray;
                Ok(this)
            }
        }
    }
}

impl TryFrom<&FFI_TypeSignature> for TypeSignature {
    type Error = ArrowError;
    fn try_from(value: &FFI_TypeSignature) -> Result<Self, Self::Error> {
        match value.kind {
            TypeSignatureKind::Variadic => {
                let dtypes = unsafe {
                    std::slice::from_raw_parts_mut(value.dtypes, value.n_data_types)
                        .iter()
                        .map(|ptr| {
                            let dtype = std::ptr::read(*ptr);
                            DataType::try_from(&dtype)
                        })
                        .collect::<Result<Vec<_>, Self::Error>>()?
                };
                Ok(TypeSignature::Variadic(dtypes))
            }
            TypeSignatureKind::VariadicEqual => Ok(TypeSignature::VariadicEqual),
            TypeSignatureKind::VariadicAny => Ok(TypeSignature::VariadicAny),
            TypeSignatureKind::Uniform => {
                let dtypes = unsafe {
                    std::slice::from_raw_parts_mut(value.dtypes, value.n_data_types)
                        .iter()
                        .map(|ptr| {
                            let dtype = std::ptr::read(*ptr);
                            DataType::try_from(&dtype)
                        })
                        .collect::<Result<Vec<_>, Self::Error>>()?
                };

                Ok(TypeSignature::Uniform(value.n_args, dtypes))
            }
            TypeSignatureKind::Exact => {
                let dtypes = unsafe {
                    std::slice::from_raw_parts_mut(value.dtypes, value.n_data_types)
                        .iter()
                        .map(|ptr| {
                            let dtype = std::ptr::read(*ptr);
                            DataType::try_from(&dtype)
                        })
                        .collect::<Result<Vec<_>, Self::Error>>()?
                };
                Ok(TypeSignature::Exact(dtypes))
            }
            TypeSignatureKind::Any => Ok(TypeSignature::Any(value.n_args)),
            TypeSignatureKind::OneOf => {
                let type_signatures = unsafe {
                    let pointers = std::slice::from_raw_parts_mut(value.children, value.n_children);
                    pointers
                        .iter()
                        .map(|ptr| {
                            let type_signature = std::ptr::read(*ptr);
                            TypeSignature::try_from(&type_signature)
                        })
                        .collect::<Result<Vec<_>, Self::Error>>()?
                };


                Ok(TypeSignature::OneOf(type_signatures))
            }
            TypeSignatureKind::ArrayAndElement => Ok(TypeSignature::ArrayAndElement),
            TypeSignatureKind::ElementAndArray => Ok(TypeSignature::ElementAndArray),
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(non_camel_case_types)]
pub enum FFI_Volatility {
    Immutable,
    Stable,
    Volatile,
}

impl From<FFI_Volatility> for Volatility {
    fn from(value: FFI_Volatility) -> Self {
        match value {
            FFI_Volatility::Immutable => Volatility::Immutable,
            FFI_Volatility::Stable => Volatility::Stable,
            FFI_Volatility::Volatile => Volatility::Volatile,
        }
    }
}

impl From<Volatility> for FFI_Volatility {
    fn from(value: Volatility) -> Self {
        match value {
            Volatility::Immutable => FFI_Volatility::Immutable,
            Volatility::Stable => FFI_Volatility::Stable,
            Volatility::Volatile => FFI_Volatility::Volatile,
        }
    }
}

impl From<&FFI_Volatility> for Volatility {
    fn from(value: &FFI_Volatility) -> Self {
        match value {
            FFI_Volatility::Immutable => Volatility::Immutable,
            FFI_Volatility::Stable => Volatility::Stable,
            FFI_Volatility::Volatile => Volatility::Volatile,
        }
    }
}

impl From<&Volatility> for FFI_Volatility {
    fn from(value: &Volatility) -> Self {
        match value {
            Volatility::Immutable => FFI_Volatility::Immutable,
            Volatility::Stable => FFI_Volatility::Stable,
            Volatility::Volatile => FFI_Volatility::Volatile,
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(non_camel_case_types)]
pub struct FFI_Signature {
    type_signature: *mut FFI_TypeSignature,
    volatility: *mut FFI_Volatility,
}

impl FFI_Signature {
    pub fn empty() -> Self {
        Self {
            type_signature: std::ptr::null_mut(),
            volatility: std::ptr::null_mut(),
        }
    }

    pub fn is_null(&self) -> bool {
        self.type_signature.is_null() && self.volatility.is_null()
    }
}

impl TryFrom<Signature> for FFI_Signature {
    type Error = ArrowError;

    fn try_from(value: Signature) -> Result<Self, Self::Error> {
        let type_signature = FFI_TypeSignature::try_from(value.type_signature)?;
        let volatility = FFI_Volatility::from(&value.volatility);
        Ok(Self {
            type_signature: Box::into_raw(Box::new(type_signature)),
            volatility: Box::into_raw(Box::new(volatility)),
        })
    }
}

impl TryFrom<&FFI_Signature> for Signature {
    type Error = ArrowError;

    fn try_from(value: &FFI_Signature) -> Result<Self, Self::Error> {
        if value.is_null() {
            return Err(ArrowError::CDataInterface(
                "FFI_Signature is null".to_string(),
            ));
        }
        unsafe {
            let type_signature = Box::from_raw(value.type_signature);
            let type_signature = TypeSignature::try_from(&*type_signature)?;
            let volatility = Box::from_raw(value.volatility);
            let volatility = Volatility::from(&*volatility);

            Ok(Signature {
                type_signature,
                volatility,
            })
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    fn round_trip_type_signature(type_signature: TypeSignature) {
        let type_signature_ffi = FFI_TypeSignature::try_from(type_signature).unwrap();
        let type_signature = TypeSignature::try_from(&type_signature_ffi).unwrap();
        assert_eq!(type_signature, type_signature);
    }
    fn round_trip_volatility(volatility: Volatility) {
        let volatility_ffi = FFI_Volatility::from(&volatility);
        let volatility = Volatility::from(&volatility_ffi);
        assert_eq!(volatility, volatility);
    }
    fn round_trip_signature(signature: Signature) {
        let signature_ffi = FFI_Signature::try_from(signature).unwrap();
        let signature = Signature::try_from(&signature_ffi).unwrap();
        assert_eq!(signature, signature);
    }

    #[test]
    fn test_round_trip_type_signature() {
        use DataType::*;
        use TypeSignature::*;

        round_trip_type_signature(VariadicAny);
        round_trip_type_signature(Variadic(vec![Int32, Int64]));
        round_trip_type_signature(VariadicEqual);
        round_trip_type_signature(Uniform(3, vec![Int32]));
        round_trip_type_signature(Uniform(2, vec![Int32, Int64]));
        round_trip_type_signature(Exact(vec![Int32, Int64]));
        round_trip_type_signature(Exact(vec![Int32, Utf8, DataType::new_list(Int32, true)]));
        round_trip_type_signature(Any(3));
        round_trip_type_signature(OneOf(vec![Variadic(vec![Int32, Int64]), VariadicEqual]));
        round_trip_type_signature(OneOf(vec![
            Variadic(vec![Int32, Int64]),
            VariadicEqual,
            Any(3),
        ]));
        round_trip_type_signature(OneOf(vec![
            OneOf(vec![
                Variadic(vec![Int32, DataType::new_list(Int32, true)]),
                VariadicEqual,
            ]),
            VariadicEqual,
            Any(3),
            Uniform(3, vec![DataType::new_list(Int32, true)]),
        ]));
    }

    #[test]
    fn test_round_trip_volatility() {
        round_trip_volatility(Volatility::Immutable);
        round_trip_volatility(Volatility::Stable);
        round_trip_volatility(Volatility::Volatile);
    }

    #[test]
    fn test_round_trip_signature() {
        use DataType::*;
        use Volatility::*;
        round_trip_signature(Signature::any(1, Immutable));
        round_trip_signature(Signature::exact(vec![Int32, Int64], Stable));
        round_trip_signature(Signature::variadic(vec![Int32, Int64], Volatile));
        round_trip_signature(Signature::variadic_equal(Immutable));
    }
}
