// API module for exposing functionality via HTTP
// Author: Gabriel Demetrios Lafis

mod server;
mod routes;
mod handlers;
mod models;

pub use server::*;
pub use routes::*;
pub use handlers::*;
pub use models::*;

use std::error::Error;
use std::fmt;

use crate::data::DataError;
use crate::processing::ProcessingError;
use crate::storage::StorageError;

/// Represents an error in the API module
#[derive(Debug)]
pub enum ApiError {
    DataError(DataError),
    ProcessingError(ProcessingError),
    StorageError(StorageError),
    ValidationError(String),
    NotFound(String),
    Unauthorized(String),
    Forbidden(String),
    Conflict(String),
    InternalError(String),
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ApiError::DataError(err) => write!(f, "Data error: {}", err),
            ApiError::ProcessingError(err) => write!(f, "Processing error: {}", err),
            ApiError::StorageError(err) => write!(f, "Storage error: {}", err),
            ApiError::ValidationError(msg) => write!(f, "Validation error: {}", msg),
            ApiError::NotFound(msg) => write!(f, "Not found: {}", msg),
            ApiError::Unauthorized(msg) => write!(f, "Unauthorized: {}", msg),
            ApiError::Forbidden(msg) => write!(f, "Forbidden: {}", msg),
            ApiError::Conflict(msg) => write!(f, "Conflict: {}", msg),
            ApiError::InternalError(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl Error for ApiError {}

impl From<DataError> for ApiError {
    fn from(err: DataError) -> Self {
        ApiError::DataError(err)
    }
}

impl From<ProcessingError> for ApiError {
    fn from(err: ProcessingError) -> Self {
        ApiError::ProcessingError(err)
    }
}

impl From<StorageError> for ApiError {
    fn from(err: StorageError) -> Self {
        ApiError::StorageError(err)
    }
}

