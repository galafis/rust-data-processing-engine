// API request and response models
// Author: Gabriel Demetrios Lafis

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Schema field definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaField {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Request to create a new dataset
#[derive(Debug, Clone, Deserialize)]
pub struct CreateDatasetRequest {
    pub name: String,
    pub schema: Vec<SchemaField>,
    pub data: Vec<Vec<JsonValue>>,
}

/// Request to update an existing dataset
#[derive(Debug, Clone, Deserialize)]
pub struct UpdateDatasetRequest {
    pub data: Option<Vec<Vec<JsonValue>>>,
}

/// Request to transform a dataset
#[derive(Debug, Clone, Deserialize)]
pub struct TransformRequest {
    pub source: String,
    pub target: Option<String>,
    pub transform_type: String,
    pub params: JsonValue,
}

/// Request to filter a dataset
#[derive(Debug, Clone, Deserialize)]
pub struct FilterRequest {
    pub source: String,
    pub target: Option<String>,
    pub filter_type: String,
    pub params: JsonValue,
}

/// Aggregation definition
#[derive(Debug, Clone, Deserialize)]
pub struct Aggregation {
    pub function: String,
    pub input_column: String,
    pub output_name: String,
}

/// Request to aggregate a dataset
#[derive(Debug, Clone, Deserialize)]
pub struct AggregateRequest {
    pub source: String,
    pub target: Option<String>,
    pub group_by: Option<Vec<String>>,
    pub aggregations: Vec<Aggregation>,
}

/// Request to join datasets
#[derive(Debug, Clone, Deserialize)]
pub struct JoinRequest {
    pub left: String,
    pub right: String,
    pub target: Option<String>,
    pub join_type: String,
    pub left_columns: Vec<String>,
    pub right_columns: Vec<String>,
}

/// Request to compute statistics on a dataset
#[derive(Debug, Clone, Deserialize)]
pub struct StatsRequest {
    pub source: String,
    pub stats_type: String,
    pub columns: Vec<String>,
    pub output_name: String,
}

