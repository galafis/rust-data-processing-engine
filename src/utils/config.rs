// Configuration utilities
// Author: Gabriel Demetrios Lafis

use std::fs::File;
use std::io::Read;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// Application configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub logging: LoggingConfig,
}

/// Server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub workers: Option<usize>,
    pub enable_cors: bool,
}

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub type_: String,
    pub path: Option<String>,
    pub format: Option<String>,
    pub cache_ttl: Option<u64>,
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub file: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            server: ServerConfig {
                host: "127.0.0.1".to_string(),
                port: 8080,
                workers: None,
                enable_cors: false,
            },
            storage: StorageConfig {
                type_: "memory".to_string(),
                path: None,
                format: None,
                cache_ttl: None,
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                file: None,
            },
        }
    }
}

impl Config {
    /// Load configuration from a file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        
        let config = if path.as_ref().extension().map_or(false, |ext| ext == "json") {
            serde_json::from_str(&contents)?
        } else if path.as_ref().extension().map_or(false, |ext| ext == "yaml" || ext == "yml") {
            serde_yaml::from_str(&contents)?
        } else {
            return Err("Unsupported config file format".into());
        };
        
        Ok(config)
    }
    
    /// Get the log level filter
    pub fn log_level_filter(&self) -> log::LevelFilter {
        match self.logging.level.to_lowercase().as_str() {
            "off" => log::LevelFilter::Off,
            "error" => log::LevelFilter::Error,
            "warn" => log::LevelFilter::Warn,
            "info" => log::LevelFilter::Info,
            "debug" => log::LevelFilter::Debug,
            "trace" => log::LevelFilter::Trace,
            _ => log::LevelFilter::Info,
        }
    }
}

