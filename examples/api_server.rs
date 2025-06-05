// API server example
// Author: Gabriel Demetrios Lafis

use std::sync::Arc;

use rust_data_processing_engine::{
    api::{Server, ServerConfig},
    storage::MemoryStorage,
    utils::init_logging,
};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize logging
    init_logging(log::LevelFilter::Info).unwrap();
    
    // Create in-memory storage
    let storage = Arc::new(MemoryStorage::new());
    
    // Create server config
    let config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port: 8080,
        workers: num_cpus::get(),
        enable_cors: true,
    };
    
    // Create and run server
    println!("Starting API server at http://{}:{}", config.host, config.port);
    println!("Press Ctrl+C to stop");
    
    let server = Server::new(storage, config);
    server.run().await
}

