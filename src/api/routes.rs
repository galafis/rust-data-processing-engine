// API routes configuration
// Author: Gabriel Demetrios Lafis

use actix_web::{web, HttpResponse, Responder};

use super::handlers;

/// Configure API routes
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api/v1")
            // Health check
            .route("/health", web::get().to(health_check))
            
            // Datasets
            .service(
                web::scope("/datasets")
                    .route("", web::get().to(handlers::list_datasets))
                    .route("", web::post().to(handlers::create_dataset))
                    .route("/{name}", web::get().to(handlers::get_dataset))
                    .route("/{name}", web::put().to(handlers::update_dataset))
                    .route("/{name}", web::delete().to(handlers::delete_dataset))
            )
            
            // Processing
            .service(
                web::scope("/process")
                    .route("/transform", web::post().to(handlers::transform_dataset))
                    .route("/filter", web::post().to(handlers::filter_dataset))
                    .route("/aggregate", web::post().to(handlers::aggregate_dataset))
                    .route("/join", web::post().to(handlers::join_datasets))
                    .route("/stats", web::post().to(handlers::compute_stats))
            )
    );
}

/// Health check handler
async fn health_check() -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

