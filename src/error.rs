use std::{io, path::Display};
use derive_more::From;
pub type Result<T> = core::result::Result<T, Error>;
// pub type Error = Box<dyn std::error::Error>; // for development


#[derive(Debug, From)]
pub enum Error {
    #[from]
    Custom(String),

    // -- <module, e.g. fs>
    // SomeInvariantLikeCantListFilesInDir
    // do use crate::{Error, Result}; in the module
    // or later we can declare the error in the module and do
    // Fs(crate::fs::Error)

    // -- Externals
    // #[from]
    // Io(std::io::Error), // create a new error type in the module
    #[from]
    Io(std::io::Error),

    // Implement TryFromSliceError
    #[from]
    TryFromSliceError(core::array::TryFromSliceError),
}

impl Error {
    pub fn cutsom(val: Display) -> Self {
        Error::Custom(val.to_string())
    }
}

impl From<&str> for Error {
    fn from(val: &str) -> Self {
        Error::Custom(val.to_string())
    }
}


impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for Error {}
