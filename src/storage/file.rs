// File storage implementation
// Author: Gabriel Demetrios Lafis

use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};

use crate::data::{DataSet, DataSource, DataSink};
use crate::data::csv::{CsvSource, CsvSink};
use crate::data::json::{JsonSource, JsonSink};
use crate::data::parquet::{ParquetSource, ParquetSink, ParquetCompression};
use super::{DataStorage, StorageError};

/// File format for storage
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FileFormat {
    Csv,
    Json,
    Parquet,
}

impl FileFormat {
    /// Get the file extension for this format
    pub fn extension(&self) -> &'static str {
        match self {
            FileFormat::Csv => "csv",
            FileFormat::Json => "json",
            FileFormat::Parquet => "parquet",
        }
    }
    
    /// Parse a file format from a string
    pub fn from_str(s: &str) -> Result<Self, StorageError> {
        match s.to_lowercase().as_str() {
            "csv" => Ok(FileFormat::Csv),
            "json" => Ok(FileFormat::Json),
            "parquet" => Ok(FileFormat::Parquet),
            _ => Err(StorageError::InvalidFormat(
                format!("Unknown file format: {}", s)
            )),
        }
    }
    
    /// Parse a file format from a file extension
    pub fn from_extension(ext: &str) -> Result<Self, StorageError> {
        Self::from_str(ext)
    }
}

/// File storage for datasets
pub struct FileStorage {
    base_dir: PathBuf,
    format: FileFormat,
}

impl FileStorage {
    /// Create a new file storage
    pub fn new<P: AsRef<Path>>(base_dir: P, format: FileFormat) -> Result<Self, StorageError> {
        let base_dir = base_dir.as_ref().to_path_buf();
        
        // Create directory if it doesn't exist
        if !base_dir.exists() {
            fs::create_dir_all(&base_dir)?;
        }
        
        Ok(FileStorage { base_dir, format })
    }
    
    /// Get the path for a dataset
    fn get_path(&self, name: &str) -> PathBuf {
        let mut path = self.base_dir.clone();
        path.push(format!("{}.{}", name, self.format.extension()));
        path
    }
}

impl DataStorage for FileStorage {
    fn store(&self, name: &str, data: &DataSet) -> Result<(), StorageError> {
        let path = self.get_path(name);
        
        match self.format {
            FileFormat::Csv => {
                let sink = CsvSink::new(&path, ',');
                sink.write(data).map_err(StorageError::from)
            },
            FileFormat::Json => {
                let sink = JsonSink::new(&path, true);
                sink.write(data).map_err(StorageError::from)
            },
            FileFormat::Parquet => {
                let sink = ParquetSink::new(&path, ParquetCompression::Snappy);
                sink.write(data).map_err(StorageError::from)
            },
        }
    }
    
    fn load(&self, name: &str) -> Result<DataSet, StorageError> {
        let path = self.get_path(name);
        
        if !path.exists() {
            return Err(StorageError::NotFound(name.to_string()));
        }
        
        match self.format {
            FileFormat::Csv => {
                let source = CsvSource::new(&path, true, ',');
                source.read().map_err(StorageError::from)
            },
            FileFormat::Json => {
                let source = JsonSource::new(&path);
                source.read().map_err(StorageError::from)
            },
            FileFormat::Parquet => {
                let source = ParquetSource::new(&path);
                source.read().map_err(StorageError::from)
            },
        }
    }
    
    fn exists(&self, name: &str) -> Result<bool, StorageError> {
        let path = self.get_path(name);
        Ok(path.exists())
    }
    
    fn delete(&self, name: &str) -> Result<(), StorageError> {
        let path = self.get_path(name);
        
        if !path.exists() {
            return Err(StorageError::NotFound(name.to_string()));
        }
        
        fs::remove_file(path)?;
        Ok(())
    }
    
    fn list(&self) -> Result<Vec<String>, StorageError> {
        let mut datasets = Vec::new();
        let ext = self.format.extension();
        
        for entry in fs::read_dir(&self.base_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_file() {
                if let Some(file_ext) = path.extension() {
                    if file_ext == ext {
                        if let Some(stem) = path.file_stem() {
                            if let Some(name) = stem.to_str() {
                                datasets.push(name.to_string());
                            }
                        }
                    }
                }
            }
        }
        
        Ok(datasets)
    }
}

