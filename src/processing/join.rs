// Join operations for data processing
// Author: Gabriel Demetrios Lafis

use std::collections::HashMap;

use crate::data::{DataSet, Field, Row, Schema, Value};
use super::{DataProcessor, ProcessingError, ProcessorType};

/// Join type for joining datasets
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// Join processor for joining datasets
pub struct JoinProcessor {
    join_type: JoinType,
    left_columns: Vec<String>,
    right_columns: Vec<String>,
}

impl JoinProcessor {
    /// Create a new join processor
    pub fn new(join_type: JoinType, left_columns: Vec<String>, right_columns: Vec<String>) -> Self {
        JoinProcessor {
            join_type,
            left_columns,
            right_columns,
        }
    }
    
    /// Create a new inner join processor
    pub fn inner(left_columns: Vec<String>, right_columns: Vec<String>) -> Self {
        Self::new(JoinType::Inner, left_columns, right_columns)
    }
    
    /// Create a new left join processor
    pub fn left(left_columns: Vec<String>, right_columns: Vec<String>) -> Self {
        Self::new(JoinType::Left, left_columns, right_columns)
    }
    
    /// Create a new right join processor
    pub fn right(left_columns: Vec<String>, right_columns: Vec<String>) -> Self {
        Self::new(JoinType::Right, left_columns, right_columns)
    }
    
    /// Create a new full join processor
    pub fn full(left_columns: Vec<String>, right_columns: Vec<String>) -> Self {
        Self::new(JoinType::Full, left_columns, right_columns)
    }
    
    /// Create a new cross join processor
    pub fn cross() -> Self {
        Self::new(JoinType::Cross, Vec::new(), Vec::new())
    }
    
    /// Process a join between two datasets
    fn process_join(&self, left: &DataSet, right: &DataSet) -> Result<DataSet, ProcessingError> {
        // For cross join, we don't need join columns
        if self.join_type == JoinType::Cross {
            return self.process_cross_join(left, right);
        }
        
        // Check that join columns are valid
        if self.left_columns.len() != self.right_columns.len() {
            return Err(ProcessingError::InvalidArgument(
                format!(
                    "Number of left join columns ({}) must match number of right join columns ({})",
                    self.left_columns.len(),
                    self.right_columns.len()
                )
            ));
        }
        
        // Find column indices for join columns
        let mut left_indices = Vec::new();
        for col in &self.left_columns {
            let mut found = false;
            
            for (i, field) in left.schema.fields.iter().enumerate() {
                if &field.name == col {
                    left_indices.push(i);
                    found = true;
                    break;
                }
            }
            
            if !found {
                return Err(ProcessingError::InvalidArgument(
                    format!("Left join column '{}' not found", col)
                ));
            }
        }
        
        let mut right_indices = Vec::new();
        for col in &self.right_columns {
            let mut found = false;
            
            for (i, field) in right.schema.fields.iter().enumerate() {
                if &field.name == col {
                    right_indices.push(i);
                    found = true;
                    break;
                }
            }
            
            if !found {
                return Err(ProcessingError::InvalidArgument(
                    format!("Right join column '{}' not found", col)
                ));
            }
        }
        
        // Create output schema
        let mut output_fields = Vec::new();
        
        // Add all left fields
        for field in &left.schema.fields {
            output_fields.push(field.clone());
        }
        
        // Add right fields except join columns
        for (i, field) in right.schema.fields.iter().enumerate() {
            if !right_indices.contains(&i) {
                // Rename if there's a name conflict
                let mut name = field.name.clone();
                let mut counter = 1;
                
                while output_fields.iter().any(|f| f.name == name) {
                    name = format!("{}_{}", field.name, counter);
                    counter += 1;
                }
                
                output_fields.push(Field::new(name, field.data_type.clone(), field.nullable));
            }
        }
        
        let output_schema = Schema::new(output_fields);
        let mut result = DataSet::new(output_schema);
        
        // Build hash map for right dataset
        let mut right_map: HashMap<Vec<Value>, Vec<&Row>> = HashMap::new();
        
        for row in &right.data {
            let key: Vec<Value> = right_indices.iter()
                .map(|&i| row.values[i].clone())
                .collect();
            
            right_map.entry(key).or_default().push(row);
        }
        
        // Process left rows
        let mut left_matched = vec![false; left.data.len()];
        
        for (left_idx, left_row) in left.data.iter().enumerate() {
            let key: Vec<Value> = left_indices.iter()
                .map(|&i| left_row.values[i].clone())
                .collect();
            
            if let Some(right_rows) = right_map.get(&key) {
                // Match found
                left_matched[left_idx] = true;
                
                for right_row in right_rows {
                    // Create output row
                    let mut output_values = left_row.values.clone();
                    
                    // Add right values except join columns
                    for (i, value) in right_row.values.iter().enumerate() {
                        if !right_indices.contains(&i) {
                            output_values.push(value.clone());
                        }
                    }
                    
                    let output_row = Row::new(output_values);
                    result.add_row(output_row)?;
                }
            } else if self.join_type == JoinType::Left || self.join_type == JoinType::Full {
                // No match, but include left row for left and full joins
                let mut output_values = left_row.values.clone();
                
                // Add nulls for right values
                let right_non_join_count = right.schema.fields.len() - right_indices.len();
                for _ in 0..right_non_join_count {
                    output_values.push(Value::Null);
                }
                
                let output_row = Row::new(output_values);
                result.add_row(output_row)?;
            }
        }
        
        // Process unmatched right rows for right and full joins
        if self.join_type == JoinType::Right || self.join_type == JoinType::Full {
            for (key, right_rows) in &right_map {
                // Check if this key was matched
                let mut matched = false;
                
                for left_idx in 0..left.data.len() {
                    let left_key: Vec<Value> = left_indices.iter()
                        .map(|&i| left.data[left_idx].values[i].clone())
                        .collect();
                    
                    if &left_key == key {
                        matched = true;
                        break;
                    }
                }
                
                if !matched {
                    for right_row in right_rows {
                        // Create output row with nulls for left values
                        let mut output_values = vec![Value::Null; left.schema.fields.len()];
                        
                        // Add right values except join columns
                        for (i, value) in right_row.values.iter().enumerate() {
                            if !right_indices.contains(&i) {
                                output_values.push(value.clone());
                            }
                        }
                        
                        let output_row = Row::new(output_values);
                        result.add_row(output_row)?;
                    }
                }
            }
        }
        
        // Copy metadata
        for (key, value) in &left.metadata.properties {
            result.metadata.add(key.clone(), value.clone());
        }
        
        for (key, value) in &right.metadata.properties {
            let mut new_key = key.clone();
            let mut counter = 1;
            
            while result.metadata.properties.contains_key(&new_key) {
                new_key = format!("{}_{}", key, counter);
                counter += 1;
            }
            
            result.metadata.add(new_key, value.clone());
        }
        
        Ok(result)
    }
    
    /// Process a cross join between two datasets
    fn process_cross_join(&self, left: &DataSet, right: &DataSet) -> Result<DataSet, ProcessingError> {
        // Create output schema
        let mut output_fields = Vec::new();
        
        // Add all left fields
        for field in &left.schema.fields {
            output_fields.push(field.clone());
        }
        
        // Add all right fields
        for field in &right.schema.fields {
            // Rename if there's a name conflict
            let mut name = field.name.clone();
            let mut counter = 1;
            
            while output_fields.iter().any(|f| f.name == name) {
                name = format!("{}_{}", field.name, counter);
                counter += 1;
            }
            
            output_fields.push(Field::new(name, field.data_type.clone(), field.nullable));
        }
        
        let output_schema = Schema::new(output_fields);
        let mut result = DataSet::new(output_schema);
        
        // Perform cross join
        for left_row in &left.data {
            for right_row in &right.data {
                // Create output row
                let mut output_values = left_row.values.clone();
                output_values.extend(right_row.values.clone());
                
                let output_row = Row::new(output_values);
                result.add_row(output_row)?;
            }
        }
        
        // Copy metadata
        for (key, value) in &left.metadata.properties {
            result.metadata.add(key.clone(), value.clone());
        }
        
        for (key, value) in &right.metadata.properties {
            let mut new_key = key.clone();
            let mut counter = 1;
            
            while result.metadata.properties.contains_key(&new_key) {
                new_key = format!("{}_{}", key, counter);
                counter += 1;
            }
            
            result.metadata.add(new_key, value.clone());
        }
        
        Ok(result)
    }
}

impl DataProcessor for JoinProcessor {
    fn process(&self, input: &DataSet) -> Result<DataSet, ProcessingError> {
        // This processor requires a second dataset, which should be provided via a context
        Err(ProcessingError::InvalidOperation(
            "JoinProcessor requires a second dataset. Use process_join method directly.".to_string()
        ))
    }
    
    fn name(&self) -> &str {
        match self.join_type {
            JoinType::Inner => "inner_join",
            JoinType::Left => "left_join",
            JoinType::Right => "right_join",
            JoinType::Full => "full_join",
            JoinType::Cross => "cross_join",
        }
    }
    
    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Join
    }
}

