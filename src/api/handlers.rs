// API request handlers
// Author: Gabriel Demetrios Lafis

use actix_web::{web, HttpResponse, Responder};
use serde_json::json;
use std::sync::Arc;

use crate::data::{DataSet, DataType, Field, Row, Schema, Value};
use crate::processing::{
    FilterProcessor, GroupByProcessor, JoinProcessor, JoinType,
    SelectTransform, AddColumnTransform, CastTransform, StatsProcessor, StatsType,
};
use crate::storage::DataStorage;
use super::{ApiError, models::*};

/// List all datasets
pub async fn list_datasets(
    storage: web::Data<Arc<dyn DataStorage + Send + Sync>>,
) -> Result<impl Responder, ApiError> {
    let datasets = storage.list()?;
    
    Ok(HttpResponse::Ok().json(json!({
        "datasets": datasets,
    })))
}

/// Create a new dataset
pub async fn create_dataset(
    storage: web::Data<Arc<dyn DataStorage + Send + Sync>>,
    payload: web::Json<CreateDatasetRequest>,
) -> Result<impl Responder, ApiError> {
    let req = payload.into_inner();
    
    // Check if dataset already exists
    if storage.exists(&req.name)? {
        return Err(ApiError::Conflict(format!(
            "Dataset '{}' already exists", req.name
        )));
    }
    
    // Create schema
    let fields = req.schema.iter()
        .map(|field| {
            let data_type = match field.data_type.as_str() {
                "boolean" => DataType::Boolean,
                "integer" => DataType::Integer,
                "float" => DataType::Float,
                "string" => DataType::String,
                "binary" => DataType::Binary,
                _ => return Err(ApiError::ValidationError(format!(
                    "Invalid data type: {}", field.data_type
                ))),
            };
            
            Ok(Field::new(field.name.clone(), data_type, field.nullable))
        })
        .collect::<Result<Vec<_>, _>>()?;
    
    let schema = Schema::new(fields);
    let mut dataset = DataSet::new(schema);
    
    // Add rows
    for row_data in &req.data {
        let values = row_data.iter()
            .map(|value| match value {
                serde_json::Value::Null => Value::Null,
                serde_json::Value::Bool(b) => Value::Boolean(*b),
                serde_json::Value::Number(n) => {
                    if n.is_i64() {
                        Value::Integer(n.as_i64().unwrap())
                    } else {
                        Value::Float(n.as_f64().unwrap())
                    }
                },
                serde_json::Value::String(s) => Value::String(s.clone()),
                _ => Value::Null,
            })
            .collect();
        
        let row = Row::new(values);
        dataset.add_row(row).map_err(ApiError::from)?;
    }
    
    // Store dataset
    storage.store(&req.name, &dataset)?;
    
    Ok(HttpResponse::Created().json(json!({
        "name": req.name,
        "rows": dataset.len(),
    })))
}

/// Get a dataset
pub async fn get_dataset(
    storage: web::Data<Arc<dyn DataStorage + Send + Sync>>,
    path: web::Path<String>,
) -> Result<impl Responder, ApiError> {
    let name = path.into_inner();
    
    // Check if dataset exists
    if !storage.exists(&name)? {
        return Err(ApiError::NotFound(format!(
            "Dataset '{}' not found", name
        )));
    }
    
    // Load dataset
    let dataset = storage.load(&name)?;
    
    // Convert to response
    let schema = dataset.schema.fields.iter()
        .map(|field| SchemaField {
            name: field.name.clone(),
            data_type: match field.data_type {
                DataType::Boolean => "boolean".to_string(),
                DataType::Integer => "integer".to_string(),
                DataType::Float => "float".to_string(),
                DataType::String => "string".to_string(),
                DataType::Binary => "binary".to_string(),
                _ => "unknown".to_string(),
            },
            nullable: field.nullable,
        })
        .collect::<Vec<_>>();
    
    let data = dataset.data.iter()
        .map(|row| {
            row.values.iter()
                .map(|value| match value {
                    Value::Null => serde_json::Value::Null,
                    Value::Boolean(b) => serde_json::Value::Bool(*b),
                    Value::Integer(i) => serde_json::Value::Number((*i).into()),
                    Value::Float(f) => {
                        serde_json::Number::from_f64(*f)
                            .map(serde_json::Value::Number)
                            .unwrap_or(serde_json::Value::Null)
                    },
                    Value::String(s) => serde_json::Value::String(s.clone()),
                    Value::Binary(_) => serde_json::Value::String("[binary data]".to_string()),
                    Value::Array(_) => serde_json::Value::String("[array]".to_string()),
                    Value::Map(_) => serde_json::Value::String("[map]".to_string()),
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    
    Ok(HttpResponse::Ok().json(json!({
        "name": name,
        "schema": schema,
        "data": data,
        "rows": dataset.len(),
    })))
}

/// Update a dataset
pub async fn update_dataset(
    storage: web::Data<Arc<dyn DataStorage + Send + Sync>>,
    path: web::Path<String>,
    payload: web::Json<UpdateDatasetRequest>,
) -> Result<impl Responder, ApiError> {
    let name = path.into_inner();
    let req = payload.into_inner();
    
    // Check if dataset exists
    if !storage.exists(&name)? {
        return Err(ApiError::NotFound(format!(
            "Dataset '{}' not found", name
        )));
    }
    
    // Load dataset
    let mut dataset = storage.load(&name)?;
    
    // Update rows if provided
    if let Some(data) = req.data {
        // Clear existing data
        dataset.data.clear();
        
        // Add new rows
        for row_data in data {
            let values = row_data.iter()
                .map(|value| match value {
                    serde_json::Value::Null => Value::Null,
                    serde_json::Value::Bool(b) => Value::Boolean(*b),
                    serde_json::Value::Number(n) => {
                        if n.is_i64() {
                            Value::Integer(n.as_i64().unwrap())
                        } else {
                            Value::Float(n.as_f64().unwrap())
                        }
                    },
                    serde_json::Value::String(s) => Value::String(s.clone()),
                    _ => Value::Null,
                })
                .collect();
            
            let row = Row::new(values);
            dataset.add_row(row).map_err(ApiError::from)?;
        }
    }
    
    // Store updated dataset
    storage.store(&name, &dataset)?;
    
    Ok(HttpResponse::Ok().json(json!({
        "name": name,
        "rows": dataset.len(),
    })))
}

/// Delete a dataset
pub async fn delete_dataset(
    storage: web::Data<Arc<dyn DataStorage + Send + Sync>>,
    path: web::Path<String>,
) -> Result<impl Responder, ApiError> {
    let name = path.into_inner();
    
    // Check if dataset exists
    if !storage.exists(&name)? {
        return Err(ApiError::NotFound(format!(
            "Dataset '{}' not found", name
        )));
    }
    
    // Delete dataset
    storage.delete(&name)?;
    
    Ok(HttpResponse::NoContent().finish())
}

/// Transform a dataset
pub async fn transform_dataset(
    storage: web::Data<Arc<dyn DataStorage + Send + Sync>>,
    payload: web::Json<TransformRequest>,
) -> Result<impl Responder, ApiError> {
    let req = payload.into_inner();
    
    // Check if source dataset exists
    if !storage.exists(&req.source)? {
        return Err(ApiError::NotFound(format!(
            "Source dataset '{}' not found", req.source
        )));
    }
    
    // Load source dataset
    let source = storage.load(&req.source)?;
    
    // Apply transformation
    let result = match req.transform_type.as_str() {
        "select" => {
            let columns = req.params.get("columns")
                .and_then(|v| v.as_array())
                .ok_or_else(|| ApiError::ValidationError(
                    "Missing or invalid 'columns' parameter".to_string()
                ))?
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>();
            
            let transform = SelectTransform::new(columns);
            transform.process(&source)?
        },
        "add_column" => {
            let name = req.params.get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| ApiError::ValidationError(
                    "Missing or invalid 'name' parameter".to_string()
                ))?;
            
            let value = req.params.get("value")
                .ok_or_else(|| ApiError::ValidationError(
                    "Missing 'value' parameter".to_string()
                ))?;
            
            let data_type = req.params.get("data_type")
                .and_then(|v| v.as_str())
                .ok_or_else(|| ApiError::ValidationError(
                    "Missing or invalid 'data_type' parameter".to_string()
                ))?;
            
            let data_type = match data_type {
                "boolean" => DataType::Boolean,
                "integer" => DataType::Integer,
                "float" => DataType::Float,
                "string" => DataType::String,
                _ => return Err(ApiError::ValidationError(format!(
                    "Invalid data type: {}", data_type
                ))),
            };
            
            let value = match value {
                serde_json::Value::Null => Value::Null,
                serde_json::Value::Bool(b) => Value::Boolean(*b),
                serde_json::Value::Number(n) => {
                    if n.is_i64() {
                        Value::Integer(n.as_i64().unwrap())
                    } else {
                        Value::Float(n.as_f64().unwrap())
                    }
                },
                serde_json::Value::String(s) => Value::String(s.clone()),
                _ => Value::Null,
            };
            
            let transform = AddColumnTransform::with_constant(name, data_type, true, value);
            transform.process(&source)?
        },
        "cast" => {
            let column = req.params.get("column")
                .and_then(|v| v.as_str())
                .ok_or_else(|| ApiError::ValidationError(
                    "Missing or invalid 'column' parameter".to_string()
                ))?;
            
            let target_type = req.params.get("target_type")
                .and_then(|v| v.as_str())
                .ok_or_else(|| ApiError::ValidationError(
                    "Missing or invalid 'target_type' parameter".to_string()
                ))?;
            
            let data_type = match target_type {
                "boolean" => DataType::Boolean,
                "integer" => DataType::Integer,
                "float" => DataType::Float,
                "string" => DataType::String,
                _ => return Err(ApiError::ValidationError(format!(
                    "Invalid target type: {}", target_type
                ))),
            };
            
            let transform = CastTransform::new(column, data_type);
            transform.process(&source)?
        },
        _ => return Err(ApiError::ValidationError(format!(
            "Unknown transform type: {}", req.transform_type
        ))),
    };
    
    // Store result dataset if target is specified
    if let Some(target) = req.target {
        storage.store(&target, &result)?;
        
        Ok(HttpResponse::Ok().json(json!({
            "target": target,
            "rows": result.len(),
        })))
    } else {
        // Return result directly
        let data = result.data.iter()
            .map(|row| {
                row.values.iter()
                    .map(|value| match value {
                        Value::Null => serde_json::Value::Null,
                        Value::Boolean(b) => serde_json::Value::Bool(*b),
                        Value::Integer(i) => serde_json::Value::Number((*i).into()),
                        Value::Float(f) => {
                            serde_json::Number::from_f64(*f)
                                .map(serde_json::Value::Number)
                                .unwrap_or(serde_json::Value::Null)
                        },
                        Value::String(s) => serde_json::Value::String(s.clone()),
                        Value::Binary(_) => serde_json::Value::String("[binary data]".to_string()),
                        Value::Array(_) => serde_json::Value::String("[array]".to_string()),
                        Value::Map(_) => serde_json::Value::String("[map]".to_string()),
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        
        Ok(HttpResponse::Ok().json(json!({
            "data": data,
            "rows": result.len(),
        })))
    }
}

/// Filter a dataset
pub async fn filter_dataset(
    storage: web::Data<Arc<dyn DataStorage + Send + Sync>>,
    payload: web::Json<FilterRequest>,
) -> Result<impl Responder, ApiError> {
    let req = payload.into_inner();
    
    // Check if source dataset exists
    if !storage.exists(&req.source)? {
        return Err(ApiError::NotFound(format!(
            "Source dataset '{}' not found", req.source
        )));
    }
    
    // Load source dataset
    let source = storage.load(&req.source)?;
    
    // Apply filter
    let filter = match req.filter_type.as_str() {
        "equals" => {
            let column = req.params.get("column")
                .and_then(|v| v.as_str())
                .ok_or_else(|| ApiError::ValidationError(
                    "Missing or invalid 'column' parameter".to_string()
                ))?;
            
            let value = req.params.get("value")
                .ok_or_else(|| ApiError::ValidationError(
                    "Missing 'value' parameter".to_string()
                ))?;
            
            let value = match value {
                serde_json::Value::Null => Value::Null,
                serde_json::Value::Bool(b) => Value::Boolean(*b),
                serde_json::Value::Number(n) => {
                    if n.is_i64() {
                        Value::Integer(n.as_i64().unwrap())
                    } else {
                        Value::Float(n.as_f64().unwrap())
                    }
                },
                serde_json::Value::String(s) => Value::String(s.clone()),
                _ => Value::Null,
            };
            
            FilterProcessor::equals(column, value)
        },
        "greater_than" => {
            let column = req.params.get("column")
                .and_then(|v| v.as_str())
                .ok_or_else(|| ApiError::ValidationError(
                    "Missing or invalid 'column' parameter".to_string()
                ))?;
            
            let value = req.params.get("value")
                .ok_or_else(|| ApiError::ValidationError(
                    "Missing 'value' parameter".to_string()
                ))?;
            
            let value = match value {
                serde_json::Value::Number(n) => {
                    if n.is_i64() {
                        Value::Integer(n.as_i64().unwrap())
                    } else {
                        Value::Float(n.as_f64().unwrap())
                    }
                },
                serde_json::Value::String(s) => Value::String(s.clone()),
                _ => return Err(ApiError::ValidationError(
                    "Value must be a number or string for greater_than filter".to_string()
                )),
            };
            
            FilterProcessor::greater_than(column, value)
        },
        "less_than" => {
            let column = req.params.get("column")
                .and_then(|v| v.as_str())
                .ok_or_else(|| ApiError::ValidationError(
                    "Missing or invalid 'column' parameter".to_string()
                ))?;
            
            let value = req.params.get("value")
                .ok_or_else(|| ApiError::ValidationError(
                    "Missing 'value' parameter".to_string()
                ))?;
            
            let value = match value {
                serde_json::Value::Number(n) => {
                    if n.is_i64() {
                        Value::Integer(n.as_i64().unwrap())
                    } else {
                        Value::Float(n.as_f64().unwrap())
                    }
                },
                serde_json::Value::String(s) => Value::String(s.clone()),
                _ => return Err(ApiError::ValidationError(
                    "Value must be a number or string for less_than filter".to_string()
                )),
            };
            
            FilterProcessor::less_than(column, value)
        },
        "not_null" => {
            let column = req.params.get("column")
                .and_then(|v| v.as_str())
                .ok_or_else(|| ApiError::ValidationError(
                    "Missing or invalid 'column' parameter".to_string()
                ))?;
            
            FilterProcessor::not_null(column)
        },
        "contains" => {
            let column = req.params.get("column")
                .and_then(|v| v.as_str())
                .ok_or_else(|| ApiError::ValidationError(
                    "Missing or invalid 'column' parameter".to_string()
                ))?;
            
            let substring = req.params.get("substring")
                .and_then(|v| v.as_str())
                .ok_or_else(|| ApiError::ValidationError(
                    "Missing or invalid 'substring' parameter".to_string()
                ))?;
            
            FilterProcessor::contains(column, substring)
        },
        _ => return Err(ApiError::ValidationError(format!(
            "Unknown filter type: {}", req.filter_type
        ))),
    };
    
    let result = filter.process(&source)?;
    
    // Store result dataset if target is specified
    if let Some(target) = req.target {
        storage.store(&target, &result)?;
        
        Ok(HttpResponse::Ok().json(json!({
            "target": target,
            "rows": result.len(),
        })))
    } else {
        // Return result directly
        let data = result.data.iter()
            .map(|row| {
                row.values.iter()
                    .map(|value| match value {
                        Value::Null => serde_json::Value::Null,
                        Value::Boolean(b) => serde_json::Value::Bool(*b),
                        Value::Integer(i) => serde_json::Value::Number((*i).into()),
                        Value::Float(f) => {
                            serde_json::Number::from_f64(*f)
                                .map(serde_json::Value::Number)
                                .unwrap_or(serde_json::Value::Null)
                        },
                        Value::String(s) => serde_json::Value::String(s.clone()),
                        Value::Binary(_) => serde_json::Value::String("[binary data]".to_string()),
                        Value::Array(_) => serde_json::Value::String("[array]".to_string()),
                        Value::Map(_) => serde_json::Value::String("[map]".to_string()),
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        
        Ok(HttpResponse::Ok().json(json!({
            "data": data,
            "rows": result.len(),
        })))
    }
}

/// Aggregate a dataset
pub async fn aggregate_dataset(
    storage: web::Data<Arc<dyn DataStorage + Send + Sync>>,
    payload: web::Json<AggregateRequest>,
) -> Result<impl Responder, ApiError> {
    let req = payload.into_inner();
    
    // Check if source dataset exists
    if !storage.exists(&req.source)? {
        return Err(ApiError::NotFound(format!(
            "Source dataset '{}' not found", req.source
        )));
    }
    
    // Load source dataset
    let source = storage.load(&req.source)?;
    
    // Create group by processor
    let mut group_by = GroupByProcessor::new();
    
    // Add group by columns
    if let Some(columns) = req.group_by {
        for column in columns {
            group_by = group_by.group_by(&column);
        }
    }
    
    // Add aggregations
    for agg in req.aggregations {
        match agg.function.as_str() {
            "count" => {
                group_by = group_by.count(&agg.output_name, &agg.input_column);
            },
            "sum" => {
                group_by = group_by.sum(&agg.output_name, &agg.input_column);
            },
            "avg" => {
                group_by = group_by.avg(&agg.output_name, &agg.input_column);
            },
            "min" => {
                group_by = group_by.min(&agg.output_name, &agg.input_column);
            },
            "max" => {
                group_by = group_by.max(&agg.output_name, &agg.input_column);
            },
            _ => return Err(ApiError::ValidationError(format!(
                "Unknown aggregation function: {}", agg.function
            ))),
        }
    }
    
    // Apply aggregation
    let result = group_by.process(&source)?;
    
    // Store result dataset if target is specified
    if let Some(target) = req.target {
        storage.store(&target, &result)?;
        
        Ok(HttpResponse::Ok().json(json!({
            "target": target,
            "rows": result.len(),
        })))
    } else {
        // Return result directly
        let data = result.data.iter()
            .map(|row| {
                row.values.iter()
                    .map(|value| match value {
                        Value::Null => serde_json::Value::Null,
                        Value::Boolean(b) => serde_json::Value::Bool(*b),
                        Value::Integer(i) => serde_json::Value::Number((*i).into()),
                        Value::Float(f) => {
                            serde_json::Number::from_f64(*f)
                                .map(serde_json::Value::Number)
                                .unwrap_or(serde_json::Value::Null)
                        },
                        Value::String(s) => serde_json::Value::String(s.clone()),
                        Value::Binary(_) => serde_json::Value::String("[binary data]".to_string()),
                        Value::Array(_) => serde_json::Value::String("[array]".to_string()),
                        Value::Map(_) => serde_json::Value::String("[map]".to_string()),
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        
        Ok(HttpResponse::Ok().json(json!({
            "data": data,
            "rows": result.len(),
        })))
    }
}

/// Join datasets
pub async fn join_datasets(
    storage: web::Data<Arc<dyn DataStorage + Send + Sync>>,
    payload: web::Json<JoinRequest>,
) -> Result<impl Responder, ApiError> {
    let req = payload.into_inner();
    
    // Check if left dataset exists
    if !storage.exists(&req.left)? {
        return Err(ApiError::NotFound(format!(
            "Left dataset '{}' not found", req.left
        )));
    }
    
    // Check if right dataset exists
    if !storage.exists(&req.right)? {
        return Err(ApiError::NotFound(format!(
            "Right dataset '{}' not found", req.right
        )));
    }
    
    // Load datasets
    let left = storage.load(&req.left)?;
    let right = storage.load(&req.right)?;
    
    // Create join processor
    let join_type = match req.join_type.as_str() {
        "inner" => JoinType::Inner,
        "left" => JoinType::Left,
        "right" => JoinType::Right,
        "full" => JoinType::Full,
        "cross" => JoinType::Cross,
        _ => return Err(ApiError::ValidationError(format!(
            "Unknown join type: {}", req.join_type
        ))),
    };
    
    let join = if join_type == JoinType::Cross {
        JoinProcessor::cross()
    } else {
        JoinProcessor::new(join_type, req.left_columns, req.right_columns)
    };
    
    // Apply join
    let result = join.process_join(&left, &right)?;
    
    // Store result dataset if target is specified
    if let Some(target) = req.target {
        storage.store(&target, &result)?;
        
        Ok(HttpResponse::Ok().json(json!({
            "target": target,
            "rows": result.len(),
        })))
    } else {
        // Return result directly
        let data = result.data.iter()
            .map(|row| {
                row.values.iter()
                    .map(|value| match value {
                        Value::Null => serde_json::Value::Null,
                        Value::Boolean(b) => serde_json::Value::Bool(*b),
                        Value::Integer(i) => serde_json::Value::Number((*i).into()),
                        Value::Float(f) => {
                            serde_json::Number::from_f64(*f)
                                .map(serde_json::Value::Number)
                                .unwrap_or(serde_json::Value::Null)
                        },
                        Value::String(s) => serde_json::Value::String(s.clone()),
                        Value::Binary(_) => serde_json::Value::String("[binary data]".to_string()),
                        Value::Array(_) => serde_json::Value::String("[array]".to_string()),
                        Value::Map(_) => serde_json::Value::String("[map]".to_string()),
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        
        Ok(HttpResponse::Ok().json(json!({
            "data": data,
            "rows": result.len(),
        })))
    }
}

/// Compute statistics on a dataset
pub async fn compute_stats(
    storage: web::Data<Arc<dyn DataStorage + Send + Sync>>,
    payload: web::Json<StatsRequest>,
) -> Result<impl Responder, ApiError> {
    let req = payload.into_inner();
    
    // Check if source dataset exists
    if !storage.exists(&req.source)? {
        return Err(ApiError::NotFound(format!(
            "Source dataset '{}' not found", req.source
        )));
    }
    
    // Load source dataset
    let source = storage.load(&req.source)?;
    
    // Create stats processor
    let stats_type = match req.stats_type.as_str() {
        "mean" => StatsType::Mean,
        "median" => StatsType::Median,
        "mode" => StatsType::Mode,
        "std_dev" => StatsType::StdDev,
        "variance" => StatsType::Variance,
        "min" => StatsType::Min,
        "max" => StatsType::Max,
        "range" => StatsType::Range,
        "sum" => StatsType::Sum,
        "count" => StatsType::Count,
        "correlation" => StatsType::Correlation,
        "covariance" => StatsType::Covariance,
        _ => return Err(ApiError::ValidationError(format!(
            "Unknown stats type: {}", req.stats_type
        ))),
    };
    
    let stats = StatsProcessor::new(&req.output_name, req.columns, stats_type);
    
    // Apply stats
    let result = stats.process(&source)?;
    
    // Get the result value
    let value = if !result.data.is_empty() && !result.data[0].values.is_empty() {
        match &result.data[0].values[0] {
            Value::Null => serde_json::Value::Null,
            Value::Boolean(b) => serde_json::Value::Bool(*b),
            Value::Integer(i) => serde_json::Value::Number((*i).into()),
            Value::Float(f) => {
                serde_json::Number::from_f64(*f)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null)
            },
            Value::String(s) => serde_json::Value::String(s.clone()),
            _ => serde_json::Value::Null,
        }
    } else {
        serde_json::Value::Null
    };
    
    Ok(HttpResponse::Ok().json(json!({
        "name": req.output_name,
        "value": value,
    })))
}

