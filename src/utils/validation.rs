// Validation utilities
// Author: Gabriel Demetrios Lafis

use crate::data::{DataSet, DataType, Value};

/// Validate that a dataset has the expected schema
pub fn validate_schema(dataset: &DataSet, expected_columns: &[(&str, DataType)]) -> Result<(), String> {
    for (name, data_type) in expected_columns {
        let mut found = false;
        
        for (i, field) in dataset.schema.fields.iter().enumerate() {
            if &field.name == name {
                if &field.data_type != data_type {
                    return Err(format!(
                        "Column '{}' has type {:?}, expected {:?}",
                        name, field.data_type, data_type
                    ));
                }
                
                found = true;
                break;
            }
        }
        
        if !found {
            return Err(format!("Column '{}' not found", name));
        }
    }
    
    Ok(())
}

/// Validate that a value is not null
pub fn validate_not_null(value: &Value, name: &str) -> Result<(), String> {
    if matches!(value, Value::Null) {
        Err(format!("'{}' cannot be null", name))
    } else {
        Ok(())
    }
}

/// Validate that a string value is not empty
pub fn validate_not_empty(value: &Value, name: &str) -> Result<(), String> {
    match value {
        Value::String(s) if s.is_empty() => {
            Err(format!("'{}' cannot be empty", name))
        },
        Value::String(_) => Ok(()),
        _ => Err(format!("'{}' must be a string", name)),
    }
}

/// Validate that a numeric value is positive
pub fn validate_positive(value: &Value, name: &str) -> Result<(), String> {
    match value {
        Value::Integer(i) if *i <= 0 => {
            Err(format!("'{}' must be positive", name))
        },
        Value::Float(f) if *f <= 0.0 => {
            Err(format!("'{}' must be positive", name))
        },
        Value::Integer(_) | Value::Float(_) => Ok(()),
        _ => Err(format!("'{}' must be a number", name)),
    }
}

/// Validate that a numeric value is in range
pub fn validate_range<T: PartialOrd + std::fmt::Display>(
    value: T,
    min: T,
    max: T,
    name: &str,
) -> Result<(), String> {
    if value < min || value > max {
        Err(format!(
            "'{}' must be between {} and {}",
            name, min, max
        ))
    } else {
        Ok(())
    }
}

