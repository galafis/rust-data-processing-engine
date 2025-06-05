// Filter operations for data processing
// Author: Gabriel Demetrios Lafis

use crate::data::{DataSet, Row, Value};
use super::{DataProcessor, ProcessingError, ProcessorType};

/// Filter rows based on a predicate
pub struct FilterProcessor {
    name: String,
    predicate: Box<dyn Fn(&Row, &DataSet) -> bool>,
}

impl FilterProcessor {
    /// Create a new filter processor with a predicate function
    pub fn new<F>(name: &str, predicate: F) -> Self
    where
        F: Fn(&Row, &DataSet) -> bool + 'static,
    {
        FilterProcessor {
            name: name.to_string(),
            predicate: Box::new(predicate),
        }
    }
    
    /// Create a filter that keeps rows where a column equals a value
    pub fn equals(column: &str, value: Value) -> Self {
        let column = column.to_string();
        Self::new(
            &format!("equals_{}", column),
            move |row, dataset| {
                // Find column index
                let mut col_idx = None;
                for (i, field) in dataset.schema.fields.iter().enumerate() {
                    if field.name == column {
                        col_idx = Some(i);
                        break;
                    }
                }
                
                if let Some(i) = col_idx {
                    match (&row.values[i], &value) {
                        (Value::Null, Value::Null) => true,
                        (Value::Boolean(a), Value::Boolean(b)) => a == b,
                        (Value::Integer(a), Value::Integer(b)) => a == b,
                        (Value::Float(a), Value::Float(b)) => (a - b).abs() < f64::EPSILON,
                        (Value::String(a), Value::String(b)) => a == b,
                        _ => false,
                    }
                } else {
                    false
                }
            },
        )
    }
    
    /// Create a filter that keeps rows where a column is greater than a value
    pub fn greater_than(column: &str, value: Value) -> Self {
        let column = column.to_string();
        Self::new(
            &format!("greater_than_{}", column),
            move |row, dataset| {
                // Find column index
                let mut col_idx = None;
                for (i, field) in dataset.schema.fields.iter().enumerate() {
                    if field.name == column {
                        col_idx = Some(i);
                        break;
                    }
                }
                
                if let Some(i) = col_idx {
                    match (&row.values[i], &value) {
                        (Value::Integer(a), Value::Integer(b)) => a > b,
                        (Value::Float(a), Value::Float(b)) => a > b,
                        (Value::String(a), Value::String(b)) => a > b,
                        _ => false,
                    }
                } else {
                    false
                }
            },
        )
    }
    
    /// Create a filter that keeps rows where a column is less than a value
    pub fn less_than(column: &str, value: Value) -> Self {
        let column = column.to_string();
        Self::new(
            &format!("less_than_{}", column),
            move |row, dataset| {
                // Find column index
                let mut col_idx = None;
                for (i, field) in dataset.schema.fields.iter().enumerate() {
                    if field.name == column {
                        col_idx = Some(i);
                        break;
                    }
                }
                
                if let Some(i) = col_idx {
                    match (&row.values[i], &value) {
                        (Value::Integer(a), Value::Integer(b)) => a < b,
                        (Value::Float(a), Value::Float(b)) => a < b,
                        (Value::String(a), Value::String(b)) => a < b,
                        _ => false,
                    }
                } else {
                    false
                }
            },
        )
    }
    
    /// Create a filter that keeps rows where a column is not null
    pub fn not_null(column: &str) -> Self {
        let column = column.to_string();
        Self::new(
            &format!("not_null_{}", column),
            move |row, dataset| {
                // Find column index
                let mut col_idx = None;
                for (i, field) in dataset.schema.fields.iter().enumerate() {
                    if field.name == column {
                        col_idx = Some(i);
                        break;
                    }
                }
                
                if let Some(i) = col_idx {
                    !matches!(row.values[i], Value::Null)
                } else {
                    false
                }
            },
        )
    }
    
    /// Create a filter that keeps rows where a column contains a substring
    pub fn contains(column: &str, substring: &str) -> Self {
        let column = column.to_string();
        let substring = substring.to_string();
        Self::new(
            &format!("contains_{}", column),
            move |row, dataset| {
                // Find column index
                let mut col_idx = None;
                for (i, field) in dataset.schema.fields.iter().enumerate() {
                    if field.name == column {
                        col_idx = Some(i);
                        break;
                    }
                }
                
                if let Some(i) = col_idx {
                    match &row.values[i] {
                        Value::String(s) => s.contains(&substring),
                        _ => false,
                    }
                } else {
                    false
                }
            },
        )
    }
}

impl DataProcessor for FilterProcessor {
    fn process(&self, input: &DataSet) -> Result<DataSet, ProcessingError> {
        // Create new dataset with same schema
        let mut result = DataSet::new(input.schema.clone());
        
        // Filter rows
        for row in &input.data {
            if (self.predicate)(row, input) {
                result.add_row(row.clone())?;
            }
        }
        
        // Copy metadata
        for (key, value) in &input.metadata.properties {
            result.metadata.add(key.clone(), value.clone());
        }
        
        Ok(result)
    }
    
    fn name(&self) -> &str {
        &self.name
    }
    
    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Filter
    }
}

/// Limit the number of rows in a dataset
pub struct LimitProcessor {
    limit: usize,
}

impl LimitProcessor {
    /// Create a new limit processor
    pub fn new(limit: usize) -> Self {
        LimitProcessor { limit }
    }
}

impl DataProcessor for LimitProcessor {
    fn process(&self, input: &DataSet) -> Result<DataSet, ProcessingError> {
        // Create new dataset with same schema
        let mut result = DataSet::new(input.schema.clone());
        
        // Copy limited number of rows
        for (i, row) in input.data.iter().enumerate() {
            if i >= self.limit {
                break;
            }
            
            result.add_row(row.clone())?;
        }
        
        // Copy metadata
        for (key, value) in &input.metadata.properties {
            result.metadata.add(key.clone(), value.clone());
        }
        
        Ok(result)
    }
    
    fn name(&self) -> &str {
        "limit"
    }
    
    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Filter
    }
}

/// Skip a number of rows in a dataset
pub struct SkipProcessor {
    skip: usize,
}

impl SkipProcessor {
    /// Create a new skip processor
    pub fn new(skip: usize) -> Self {
        SkipProcessor { skip }
    }
}

impl DataProcessor for SkipProcessor {
    fn process(&self, input: &DataSet) -> Result<DataSet, ProcessingError> {
        // Create new dataset with same schema
        let mut result = DataSet::new(input.schema.clone());
        
        // Skip rows and copy the rest
        for (i, row) in input.data.iter().enumerate() {
            if i < self.skip {
                continue;
            }
            
            result.add_row(row.clone())?;
        }
        
        // Copy metadata
        for (key, value) in &input.metadata.properties {
            result.metadata.add(key.clone(), value.clone());
        }
        
        Ok(result)
    }
    
    fn name(&self) -> &str {
        "skip"
    }
    
    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Filter
    }
}

/// Sample rows from a dataset
pub struct SampleProcessor {
    fraction: f64,
    seed: Option<u64>,
}

impl SampleProcessor {
    /// Create a new sample processor
    pub fn new(fraction: f64, seed: Option<u64>) -> Self {
        SampleProcessor { fraction, seed }
    }
}

impl DataProcessor for SampleProcessor {
    fn process(&self, input: &DataSet) -> Result<DataSet, ProcessingError> {
        if self.fraction < 0.0 || self.fraction > 1.0 {
            return Err(ProcessingError::InvalidArgument(
                format!("Sample fraction must be between 0.0 and 1.0, got {}", self.fraction)
            ));
        }
        
        // Create new dataset with same schema
        let mut result = DataSet::new(input.schema.clone());
        
        // Set up random number generator
        let mut rng = match self.seed {
            Some(seed) => rand::rngs::StdRng::seed_from_u64(seed),
            None => rand::rngs::StdRng::from_entropy(),
        };
        
        use rand::Rng;
        
        // Sample rows
        for row in &input.data {
            if rng.gen::<f64>() < self.fraction {
                result.add_row(row.clone())?;
            }
        }
        
        // Copy metadata
        for (key, value) in &input.metadata.properties {
            result.metadata.add(key.clone(), value.clone());
        }
        
        Ok(result)
    }
    
    fn name(&self) -> &str {
        "sample"
    }
    
    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Filter
    }
}

