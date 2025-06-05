// Rust Data Processing Engine - Main executable
// Author: Gabriel Demetrios Lafis

use std::path::PathBuf;
use std::sync::Arc;

use clap::{App, Arg, SubCommand};
use log::{info, error};

use rust_data_processing_engine::{
    api::Server,
    storage::{FileStorage, FileFormat, MemoryStorage, CacheStorage},
    utils::{Config, init_logging},
};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Parse command line arguments
    let matches = App::new("Rust Data Processing Engine")
        .version(env!("CARGO_PKG_VERSION"))
        .author("Gabriel Demetrios Lafis")
        .about("A high-performance data processing engine")
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file")
                .takes_value(true),
        )
        .subcommand(
            SubCommand::with_name("server")
                .about("Run the API server")
                .arg(
                    Arg::with_name("host")
                        .short("h")
                        .long("host")
                        .value_name("HOST")
                        .help("Sets the server host")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("port")
                        .short("p")
                        .long("port")
                        .value_name("PORT")
                        .help("Sets the server port")
                        .takes_value(true),
                ),
        )
        .get_matches();
    
    // Load configuration
    let config = if let Some(config_path) = matches.value_of("config") {
        match Config::from_file(config_path) {
            Ok(config) => config,
            Err(err) => {
                eprintln!("Error loading config file: {}", err);
                Config::default()
            }
        }
    } else {
        Config::default()
    };
    
    // Initialize logging
    if let Err(err) = init_logging(config.log_level_filter()) {
        eprintln!("Error initializing logger: {}", err);
    }
    
    // Create storage
    let storage: Arc<dyn rust_data_processing_engine::storage::DataStorage + Send + Sync> = match config.storage.type_.as_str() {
        "file" => {
            let path = config.storage.path.clone().unwrap_or_else(|| "./data".to_string());
            let format = match config.storage.format.as_deref() {
                Some("csv") => FileFormat::Csv,
                Some("json") => FileFormat::Json,
                Some("parquet") => FileFormat::Parquet,
                _ => FileFormat::Csv,
            };
            
            match FileStorage::new(path, format) {
                Ok(storage) => Arc::new(storage),
                Err(err) => {
                    error!("Error creating file storage: {:?}", err);
                    Arc::new(MemoryStorage::new())
                }
            }
        },
        "cache" => {
            let path = config.storage.path.clone().unwrap_or_else(|| "./data".to_string());
            let format = match config.storage.format.as_deref() {
                Some("csv") => FileFormat::Csv,
                Some("json") => FileFormat::Json,
                Some("parquet") => FileFormat::Parquet,
                _ => FileFormat::Csv,
            };
            
            let file_storage = match FileStorage::new(path, format) {
                Ok(storage) => storage,
                Err(err) => {
                    error!("Error creating file storage for cache: {:?}", err);
                    return Ok(());
                }
            };
            
            let mut cache_storage = CacheStorage::new(file_storage);
            
            if let Some(ttl) = config.storage.cache_ttl {
                cache_storage = cache_storage.with_ttl(std::time::Duration::from_secs(ttl));
            }
            
            Arc::new(cache_storage)
        },
        _ => Arc::new(MemoryStorage::new()),
    };
    
    // Handle subcommands
    if let Some(matches) = matches.subcommand_matches("server") {
        // Override config with command line arguments
        let host = matches.value_of("host").unwrap_or(&config.server.host);
        let port = matches
            .value_of("port")
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(config.server.port);
        
        // Create server config
        let server_config = rust_data_processing_engine::api::ServerConfig {
            host: host.to_string(),
            port,
            workers: config.server.workers.unwrap_or_else(num_cpus::get),
            enable_cors: config.server.enable_cors,
        };
        
        // Create and run server
        info!("Starting server at {}:{}", host, port);
        let server = Server::new(storage, server_config);
        server.run().await?;
    } else {
        println!("No subcommand specified. Use --help for usage information.");
    }
    
    Ok(())
}

