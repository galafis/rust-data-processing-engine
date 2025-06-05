// Error handling utilities
// Author: Gabriel Demetrios Lafis

use std::error::Error;
use std::fmt;

use crate::data::DataError;
use crate::processing::ProcessingError;
use crate::storage::StorageError;
use crate::api::ApiError;

/// Application error type
#[derive(Debug)]
pub enum AppError {
    Data(DataError),
    Processing(ProcessingError),
    Storage(StorageError),
    Api(ApiError),
    Config(String),
    Other(String),
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AppError::Data(err) => write!(f, "Data error: {}", err),
            AppError::Processing(err) => write!(f, "Processing error: {}", err),
            AppError::Storage(err) => write!(f, "Storage error: {}", err),
            AppError::Api(err) => write!(f, "API error: {}", err),
            AppError::Config(msg) => write!(f, "Configuration error: {}", msg),
            AppError::Other(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl Error for AppError {}

impl From<DataError> for AppError {
    fn from(err: DataError) -> Self {
        AppError::Data(err)
    }
}

impl From<ProcessingError> for AppError {
    fn from(err: ProcessingError) -> Self {
        AppError::Processing(err)
    }
}

impl From<StorageError> for AppError {
    fn from(err: StorageError) -> Self {
        AppError::Storage(err)
    }
}

impl From<ApiError> for AppError {
    fn from(err: ApiError) -> Self {
        AppError::Api(err)
    }
}

impl From<std::io::Error> for AppError {
    fn from(err: std::io::Error) -> Self {
        AppError::Other(err.to_string())
    }
}

/// Result type alias for AppError
pub type AppResult<T> = Result<T, AppError>;

