// API server implementation
// Author: Gabriel Demetrios Lafis

use std::net::SocketAddr;
use std::sync::Arc;

use actix_web::{web, App, HttpServer};
use actix_cors::Cors;

use crate::storage::DataStorage;
use super::routes;

/// API server configuration
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub workers: usize,
    pub enable_cors: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            host: "127.0.0.1".to_string(),
            port: 8080,
            workers: num_cpus::get(),
            enable_cors: false,
        }
    }
}

/// API server
pub struct Server {
    config: ServerConfig,
    storage: Arc<dyn DataStorage + Send + Sync>,
}

impl Server {
    /// Create a new API server
    pub fn new<S>(storage: S, config: ServerConfig) -> Self
    where
        S: DataStorage + Send + Sync + 'static,
    {
        Server {
            config,
            storage: Arc::new(storage),
        }
    }
    
    /// Run the API server
    pub async fn run(&self) -> std::io::Result<()> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        let addr = addr.parse::<SocketAddr>().unwrap();
        
        let storage = self.storage.clone();
        let enable_cors = self.config.enable_cors;
        
        println!("Starting server at http://{}", addr);
        
        HttpServer::new(move || {
            let mut app = App::new()
                .app_data(web::Data::new(storage.clone()));
            
            if enable_cors {
                app = app.wrap(
                    Cors::default()
                        .allow_any_origin()
                        .allow_any_method()
                        .allow_any_header()
                        .max_age(3600)
                );
            }
            
            app.configure(routes::configure)
        })
        .workers(self.config.workers)
        .bind(addr)?
        .run()
        .await
    }
}

