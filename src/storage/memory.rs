// Memory storage implementation
// Author: Gabriel Demetrios Lafis

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::data::DataSet;
use super::{DataStorage, StorageError};

/// Memory storage for datasets
pub struct MemoryStorage {
    datasets: Arc<RwLock<HashMap<String, DataSet>>>,
}

impl MemoryStorage {
    /// Create a new memory storage
    pub fn new() -> Self {
        MemoryStorage {
            datasets: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl DataStorage for MemoryStorage {
    fn store(&self, name: &str, data: &DataSet) -> Result<(), StorageError> {
        let mut datasets = self.datasets.write().map_err(|_| {
            StorageError::Other("Failed to acquire write lock".to_string())
        })?;
        
        datasets.insert(name.to_string(), data.clone());
        Ok(())
    }
    
    fn load(&self, name: &str) -> Result<DataSet, StorageError> {
        let datasets = self.datasets.read().map_err(|_| {
            StorageError::Other("Failed to acquire read lock".to_string())
        })?;
        
        datasets.get(name)
            .cloned()
            .ok_or_else(|| StorageError::NotFound(name.to_string()))
    }
    
    fn exists(&self, name: &str) -> Result<bool, StorageError> {
        let datasets = self.datasets.read().map_err(|_| {
            StorageError::Other("Failed to acquire read lock".to_string())
        })?;
        
        Ok(datasets.contains_key(name))
    }
    
    fn delete(&self, name: &str) -> Result<(), StorageError> {
        let mut datasets = self.datasets.write().map_err(|_| {
            StorageError::Other("Failed to acquire write lock".to_string())
        })?;
        
        if datasets.remove(name).is_none() {
            return Err(StorageError::NotFound(name.to_string()));
        }
        
        Ok(())
    }
    
    fn list(&self) -> Result<Vec<String>, StorageError> {
        let datasets = self.datasets.read().map_err(|_| {
            StorageError::Other("Failed to acquire read lock".to_string())
        })?;
        
        Ok(datasets.keys().cloned().collect())
    }
}

