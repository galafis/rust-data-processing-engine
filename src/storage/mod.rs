// Storage module for data persistence
// Author: Gabriel Demetrios Lafis

mod file;
mod memory;
mod cache;

pub use file::*;
pub use memory::*;
pub use cache::*;

use std::error::Error;
use std::fmt;

use crate::data::{DataError, DataSet};

/// Represents a data storage
pub trait DataStorage {
    /// Store a dataset
    fn store(&self, name: &str, data: &DataSet) -> Result<(), StorageError>;
    
    /// Load a dataset
    fn load(&self, name: &str) -> Result<DataSet, StorageError>;
    
    /// Check if a dataset exists
    fn exists(&self, name: &str) -> Result<bool, StorageError>;
    
    /// Delete a dataset
    fn delete(&self, name: &str) -> Result<(), StorageError>;
    
    /// List all datasets
    fn list(&self) -> Result<Vec<String>, StorageError>;
}

/// Represents an error in the storage module
#[derive(Debug)]
pub enum StorageError {
    DataError(DataError),
    IoError(std::io::Error),
    NotFound(String),
    AlreadyExists(String),
    InvalidFormat(String),
    Other(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StorageError::DataError(err) => write!(f, "Data error: {}", err),
            StorageError::IoError(err) => write!(f, "IO error: {}", err),
            StorageError::NotFound(name) => write!(f, "Dataset '{}' not found", name),
            StorageError::AlreadyExists(name) => write!(f, "Dataset '{}' already exists", name),
            StorageError::InvalidFormat(msg) => write!(f, "Invalid format: {}", msg),
            StorageError::Other(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl Error for StorageError {}

impl From<DataError> for StorageError {
    fn from(err: DataError) -> Self {
        StorageError::DataError(err)
    }
}

impl From<std::io::Error> for StorageError {
    fn from(err: std::io::Error) -> Self {
        StorageError::IoError(err)
    }
}

