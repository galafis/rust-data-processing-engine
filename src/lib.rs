// Rust Data Processing Engine
// Author: Gabriel Demetrios Lafis

//! # Rust Data Processing Engine
//!
//! A high-performance data processing engine written in Rust.
//!
//! ## Features
//!
//! - Data loading and saving in various formats (CSV, JSON, Parquet)
//! - Data transformation and filtering
//! - Aggregation and grouping
//! - Joining datasets
//! - Statistical analysis
//! - REST API for remote access
//!
//! ## Example
//!
//! ```rust
//! use rust_data_processing_engine::{
//!     data::{DataSet, DataType, Field, Row, Schema, Value},
//!     processing::{FilterProcessor, Pipeline, SelectTransform},
//!     storage::{FileStorage, FileFormat},
//! };
//!
//! // Create a schema
//! let schema = Schema::new(vec![
//!     Field::new("id".to_string(), DataType::Integer, false),
//!     Field::new("name".to_string(), DataType::String, false),
//!     Field::new("age".to_string(), DataType::Integer, true),
//! ]);
//!
//! // Create a dataset
//! let mut dataset = DataSet::new(schema);
//!
//! // Add rows
//! dataset.add_row(Row::new(vec![
//!     Value::Integer(1),
//!     Value::String("Alice".to_string()),
//!     Value::Integer(30),
//! ])).unwrap();
//!
//! dataset.add_row(Row::new(vec![
//!     Value::Integer(2),
//!     Value::String("Bob".to_string()),
//!     Value::Integer(25),
//! ])).unwrap();
//!
//! // Create a pipeline
//! let pipeline = Pipeline::new("example")
//!     .add(FilterProcessor::greater_than("age", Value::Integer(20)))
//!     .add(SelectTransform::new(vec!["name".to_string(), "age".to_string()]));
//!
//! // Process the dataset
//! let result = pipeline.process(&dataset).unwrap();
//!
//! // Save the result
//! let storage = FileStorage::new("./data", FileFormat::Csv).unwrap();
//! storage.store("result", &result).unwrap();
//! ```

pub mod data;
pub mod processing;
pub mod storage;
pub mod api;
pub mod utils;

// Re-export main types
pub use data::{DataSet, DataType, Field, Row, Schema, Value};
pub use processing::Pipeline;
pub use storage::FileStorage;
pub use api::Server;
pub use utils::Config;

