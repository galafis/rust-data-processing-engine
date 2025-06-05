// Cache storage implementation
// Author: Gabriel Demetrios Lafis

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::data::DataSet;
use super::{DataStorage, StorageError};

/// Cache entry with expiration
struct CacheEntry {
    data: DataSet,
    expires_at: Option<Instant>,
}

/// Cache storage for datasets
pub struct CacheStorage {
    backend: Box<dyn DataStorage + Send + Sync>,
    cache: Arc<RwLock<HashMap<String, CacheEntry>>>,
    default_ttl: Option<Duration>,
}

impl CacheStorage {
    /// Create a new cache storage with a backend
    pub fn new<S>(backend: S) -> Self
    where
        S: DataStorage + Send + Sync + 'static,
    {
        CacheStorage {
            backend: Box::new(backend),
            cache: Arc::new(RwLock::new(HashMap::new())),
            default_ttl: None,
        }
    }
    
    /// Set the default time-to-live for cache entries
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = Some(ttl);
        self
    }
    
    /// Clear expired entries from the cache
    pub fn clear_expired(&self) -> Result<(), StorageError> {
        let mut cache = self.cache.write().map_err(|_| {
            StorageError::Other("Failed to acquire write lock".to_string())
        })?;
        
        let now = Instant::now();
        cache.retain(|_, entry| {
            entry.expires_at.map_or(true, |expires| expires > now)
        });
        
        Ok(())
    }
    
    /// Clear all entries from the cache
    pub fn clear_all(&self) -> Result<(), StorageError> {
        let mut cache = self.cache.write().map_err(|_| {
            StorageError::Other("Failed to acquire write lock".to_string())
        })?;
        
        cache.clear();
        Ok(())
    }
}

impl DataStorage for CacheStorage {
    fn store(&self, name: &str, data: &DataSet) -> Result<(), StorageError> {
        // Store in backend
        self.backend.store(name, data)?;
        
        // Update cache
        let mut cache = self.cache.write().map_err(|_| {
            StorageError::Other("Failed to acquire write lock".to_string())
        })?;
        
        let expires_at = self.default_ttl.map(|ttl| Instant::now() + ttl);
        
        cache.insert(name.to_string(), CacheEntry {
            data: data.clone(),
            expires_at,
        });
        
        Ok(())
    }
    
    fn load(&self, name: &str) -> Result<DataSet, StorageError> {
        // Clear expired entries
        self.clear_expired()?;
        
        // Check cache first
        let cache = self.cache.read().map_err(|_| {
            StorageError::Other("Failed to acquire read lock".to_string())
        })?;
        
        if let Some(entry) = cache.get(name) {
            return Ok(entry.data.clone());
        }
        
        // Load from backend and update cache
        let data = self.backend.load(name)?;
        
        drop(cache); // Release read lock before acquiring write lock
        
        let mut cache = self.cache.write().map_err(|_| {
            StorageError::Other("Failed to acquire write lock".to_string())
        })?;
        
        let expires_at = self.default_ttl.map(|ttl| Instant::now() + ttl);
        
        cache.insert(name.to_string(), CacheEntry {
            data: data.clone(),
            expires_at,
        });
        
        Ok(data)
    }
    
    fn exists(&self, name: &str) -> Result<bool, StorageError> {
        // Clear expired entries
        self.clear_expired()?;
        
        // Check cache first
        let cache = self.cache.read().map_err(|_| {
            StorageError::Other("Failed to acquire read lock".to_string())
        })?;
        
        if cache.contains_key(name) {
            return Ok(true);
        }
        
        // Check backend
        self.backend.exists(name)
    }
    
    fn delete(&self, name: &str) -> Result<(), StorageError> {
        // Delete from backend
        self.backend.delete(name)?;
        
        // Remove from cache
        let mut cache = self.cache.write().map_err(|_| {
            StorageError::Other("Failed to acquire write lock".to_string())
        })?;
        
        cache.remove(name);
        
        Ok(())
    }
    
    fn list(&self) -> Result<Vec<String>, StorageError> {
        // Just delegate to backend
        self.backend.list()
    }
}

