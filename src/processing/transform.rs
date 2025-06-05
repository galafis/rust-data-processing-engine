// Transform operations for data processing
// Author: Gabriel Demetrios Lafis

use std::collections::HashSet;

use crate::data::{DataSet, DataType, Field, Row, Schema, Value};
use super::{DataProcessor, ProcessingError, ProcessorType};

/// Select specific columns from a dataset
pub struct SelectTransform {
    columns: Vec<String>,
}

impl SelectTransform {
    /// Create a new select transform with the given column names
    pub fn new(columns: Vec<String>) -> Self {
        SelectTransform { columns }
    }
}

impl DataProcessor for SelectTransform {
    fn process(&self, input: &DataSet) -> Result<DataSet, ProcessingError> {
        // Find indices of selected columns
        let mut indices = Vec::new();
        let mut selected_fields = Vec::new();
        
        for col in &self.columns {
            let mut found = false;
            
            for (i, field) in input.schema.fields.iter().enumerate() {
                if &field.name == col {
                    indices.push(i);
                    selected_fields.push(field.clone());
                    found = true;
                    break;
                }
            }
            
            if !found {
                return Err(ProcessingError::InvalidArgument(
                    format!("Column '{}' not found", col)
                ));
            }
        }
        
        // Create new schema with selected columns
        let schema = Schema::new(selected_fields);
        let mut result = DataSet::new(schema);
        
        // Copy selected columns to new dataset
        for row in &input.data {
            let values: Vec<Value> = indices.iter()
                .map(|&i| row.values[i].clone())
                .collect();
            
            let new_row = Row::new(values);
            result.add_row(new_row)?;
        }
        
        // Copy metadata
        for (key, value) in &input.metadata.properties {
            result.metadata.add(key.clone(), value.clone());
        }
        
        Ok(result)
    }
    
    fn name(&self) -> &str {
        "select"
    }
    
    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Transform
    }
}

/// Rename columns in a dataset
pub struct RenameTransform {
    renames: Vec<(String, String)>, // (old_name, new_name)
}

impl RenameTransform {
    /// Create a new rename transform with the given column renames
    pub fn new(renames: Vec<(String, String)>) -> Self {
        RenameTransform { renames }
    }
}

impl DataProcessor for RenameTransform {
    fn process(&self, input: &DataSet) -> Result<DataSet, ProcessingError> {
        // Create new schema with renamed columns
        let mut fields = input.schema.fields.clone();
        
        for (old_name, new_name) in &self.renames {
            let mut found = false;
            
            for field in &mut fields {
                if &field.name == old_name {
                    field.name = new_name.clone();
                    found = true;
                    break;
                }
            }
            
            if !found {
                return Err(ProcessingError::InvalidArgument(
                    format!("Column '{}' not found", old_name)
                ));
            }
        }
        
        // Check for duplicate column names
        let mut names = HashSet::new();
        for field in &fields {
            if !names.insert(&field.name) {
                return Err(ProcessingError::InvalidArgument(
                    format!("Duplicate column name '{}' after rename", field.name)
                ));
            }
        }
        
        // Create new dataset with renamed schema
        let schema = Schema::new(fields);
        let mut result = DataSet::new(schema);
        
        // Copy data
        for row in &input.data {
            result.add_row(row.clone())?;
        }
        
        // Copy metadata
        for (key, value) in &input.metadata.properties {
            result.metadata.add(key.clone(), value.clone());
        }
        
        Ok(result)
    }
    
    fn name(&self) -> &str {
        "rename"
    }
    
    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Transform
    }
}

/// Add a new column to a dataset
pub struct AddColumnTransform {
    name: String,
    data_type: DataType,
    nullable: bool,
    generator: Box<dyn Fn(&Row, &DataSet) -> Value>,
}

impl AddColumnTransform {
    /// Create a new add column transform with a generator function
    pub fn new<F>(name: &str, data_type: DataType, nullable: bool, generator: F) -> Self
    where
        F: Fn(&Row, &DataSet) -> Value + 'static,
    {
        AddColumnTransform {
            name: name.to_string(),
            data_type,
            nullable,
            generator: Box::new(generator),
        }
    }
    
    /// Create a new add column transform with a constant value
    pub fn with_constant(name: &str, data_type: DataType, nullable: bool, value: Value) -> Self {
        Self::new(name, data_type, nullable, move |_, _| value.clone())
    }
}

impl DataProcessor for AddColumnTransform {
    fn process(&self, input: &DataSet) -> Result<DataSet, ProcessingError> {
        // Check if column name already exists
        for field in &input.schema.fields {
            if field.name == self.name {
                return Err(ProcessingError::InvalidArgument(
                    format!("Column '{}' already exists", self.name)
                ));
            }
        }
        
        // Create new schema with added column
        let mut fields = input.schema.fields.clone();
        fields.push(Field::new(self.name.clone(), self.data_type.clone(), self.nullable));
        
        let schema = Schema::new(fields);
        let mut result = DataSet::new(schema);
        
        // Copy data and add new column
        for row in &input.data {
            let mut values = row.values.clone();
            values.push((self.generator)(row, input));
            
            let new_row = Row::new(values);
            result.add_row(new_row)?;
        }
        
        // Copy metadata
        for (key, value) in &input.metadata.properties {
            result.metadata.add(key.clone(), value.clone());
        }
        
        Ok(result)
    }
    
    fn name(&self) -> &str {
        "add_column"
    }
    
    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Transform
    }
}

/// Cast a column to a different data type
pub struct CastTransform {
    column: String,
    target_type: DataType,
}

impl CastTransform {
    /// Create a new cast transform
    pub fn new(column: &str, target_type: DataType) -> Self {
        CastTransform {
            column: column.to_string(),
            target_type,
        }
    }
    
    /// Cast a value to the target type
    fn cast_value(&self, value: &Value) -> Result<Value, ProcessingError> {
        match (value, &self.target_type) {
            // Null remains null for any type
            (Value::Null, _) => Ok(Value::Null),
            
            // Boolean casts
            (Value::Boolean(b), DataType::Boolean) => Ok(Value::Boolean(*b)),
            (Value::Boolean(b), DataType::Integer) => Ok(Value::Integer(if *b { 1 } else { 0 })),
            (Value::Boolean(b), DataType::Float) => Ok(Value::Float(if *b { 1.0 } else { 0.0 })),
            (Value::Boolean(b), DataType::String) => Ok(Value::String(b.to_string())),
            
            // Integer casts
            (Value::Integer(i), DataType::Boolean) => Ok(Value::Boolean(*i != 0)),
            (Value::Integer(i), DataType::Integer) => Ok(Value::Integer(*i)),
            (Value::Integer(i), DataType::Float) => Ok(Value::Float(*i as f64)),
            (Value::Integer(i), DataType::String) => Ok(Value::String(i.to_string())),
            
            // Float casts
            (Value::Float(f), DataType::Boolean) => Ok(Value::Boolean(*f != 0.0)),
            (Value::Float(f), DataType::Integer) => Ok(Value::Integer(*f as i64)),
            (Value::Float(f), DataType::Float) => Ok(Value::Float(*f)),
            (Value::Float(f), DataType::String) => Ok(Value::String(f.to_string())),
            
            // String casts
            (Value::String(s), DataType::Boolean) => {
                let lower = s.to_lowercase();
                if lower == "true" || lower == "yes" || lower == "1" {
                    Ok(Value::Boolean(true))
                } else if lower == "false" || lower == "no" || lower == "0" {
                    Ok(Value::Boolean(false))
                } else {
                    Err(ProcessingError::InvalidOperation(
                        format!("Cannot cast '{}' to boolean", s)
                    ))
                }
            },
            (Value::String(s), DataType::Integer) => {
                s.parse::<i64>()
                    .map(Value::Integer)
                    .map_err(|_| ProcessingError::InvalidOperation(
                        format!("Cannot cast '{}' to integer", s)
                    ))
            },
            (Value::String(s), DataType::Float) => {
                s.parse::<f64>()
                    .map(Value::Float)
                    .map_err(|_| ProcessingError::InvalidOperation(
                        format!("Cannot cast '{}' to float", s)
                    ))
            },
            (Value::String(s), DataType::String) => Ok(Value::String(s.clone())),
            
            // Other casts not supported
            _ => Err(ProcessingError::NotSupported(
                format!("Cast from {:?} to {:?} not supported", value, self.target_type)
            )),
        }
    }
}

impl DataProcessor for CastTransform {
    fn process(&self, input: &DataSet) -> Result<DataSet, ProcessingError> {
        // Find column index
        let mut col_idx = None;
        
        for (i, field) in input.schema.fields.iter().enumerate() {
            if field.name == self.column {
                col_idx = Some(i);
                break;
            }
        }
        
        let col_idx = col_idx.ok_or_else(|| ProcessingError::InvalidArgument(
            format!("Column '{}' not found", self.column)
        ))?;
        
        // Create new schema with updated data type
        let mut fields = input.schema.fields.clone();
        fields[col_idx].data_type = self.target_type.clone();
        
        let schema = Schema::new(fields);
        let mut result = DataSet::new(schema);
        
        // Copy data and cast the specified column
        for row in &input.data {
            let mut values = row.values.clone();
            values[col_idx] = self.cast_value(&values[col_idx])?;
            
            let new_row = Row::new(values);
            result.add_row(new_row)?;
        }
        
        // Copy metadata
        for (key, value) in &input.metadata.properties {
            result.metadata.add(key.clone(), value.clone());
        }
        
        Ok(result)
    }
    
    fn name(&self) -> &str {
        "cast"
    }
    
    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Transform
    }
}

/// Drop columns from a dataset
pub struct DropColumnsTransform {
    columns: Vec<String>,
}

impl DropColumnsTransform {
    /// Create a new drop columns transform
    pub fn new(columns: Vec<String>) -> Self {
        DropColumnsTransform { columns }
    }
}

impl DataProcessor for DropColumnsTransform {
    fn process(&self, input: &DataSet) -> Result<DataSet, ProcessingError> {
        // Find indices of columns to keep
        let mut keep_indices = Vec::new();
        let mut keep_fields = Vec::new();
        
        for (i, field) in input.schema.fields.iter().enumerate() {
            if !self.columns.contains(&field.name) {
                keep_indices.push(i);
                keep_fields.push(field.clone());
            }
        }
        
        // Create new schema with kept columns
        let schema = Schema::new(keep_fields);
        let mut result = DataSet::new(schema);
        
        // Copy kept columns to new dataset
        for row in &input.data {
            let values: Vec<Value> = keep_indices.iter()
                .map(|&i| row.values[i].clone())
                .collect();
            
            let new_row = Row::new(values);
            result.add_row(new_row)?;
        }
        
        // Copy metadata
        for (key, value) in &input.metadata.properties {
            result.metadata.add(key.clone(), value.clone());
        }
        
        Ok(result)
    }
    
    fn name(&self) -> &str {
        "drop_columns"
    }
    
    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Transform
    }
}

