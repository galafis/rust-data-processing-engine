// Aggregate operations for data processing
// Author: Gabriel Demetrios Lafis

use std::collections::HashMap;

use crate::data::{DataSet, DataType, Field, Row, Schema, Value};
use super::{DataProcessor, ProcessingError, ProcessorType};

/// Represents an aggregation function
pub trait AggregateFunction: Send + Sync {
    /// Get the name of the aggregation function
    fn name(&self) -> &str;
    
    /// Get the output data type of the aggregation function
    fn output_type(&self, input_type: &DataType) -> DataType;
    
    /// Initialize the aggregation state
    fn init(&self) -> Box<dyn std::any::Any + Send>;
    
    /// Update the aggregation state with a new value
    fn update(&self, state: &mut Box<dyn std::any::Any + Send>, value: &Value);
    
    /// Finalize the aggregation and return the result
    fn finalize(&self, state: Box<dyn std::any::Any + Send>) -> Value;
}

/// Count aggregation function
pub struct CountFunction;

impl AggregateFunction for CountFunction {
    fn name(&self) -> &str {
        "count"
    }
    
    fn output_type(&self, _input_type: &DataType) -> DataType {
        DataType::Integer
    }
    
    fn init(&self) -> Box<dyn std::any::Any + Send> {
        Box::new(0i64)
    }
    
    fn update(&self, state: &mut Box<dyn std::any::Any + Send>, value: &Value) {
        if !matches!(value, Value::Null) {
            let count = state.downcast_mut::<i64>().unwrap();
            *count += 1;
        }
    }
    
    fn finalize(&self, state: Box<dyn std::any::Any + Send>) -> Value {
        let count = *state.downcast::<i64>().unwrap();
        Value::Integer(count)
    }
}

/// Sum aggregation function
pub struct SumFunction;

impl AggregateFunction for SumFunction {
    fn name(&self) -> &str {
        "sum"
    }
    
    fn output_type(&self, input_type: &DataType) -> DataType {
        match input_type {
            DataType::Integer => DataType::Integer,
            DataType::Float => DataType::Float,
            _ => DataType::Float, // Default to float for other types
        }
    }
    
    fn init(&self) -> Box<dyn std::any::Any + Send> {
        Box::new((0i64, 0.0f64, false)) // (int_sum, float_sum, is_float)
    }
    
    fn update(&self, state: &mut Box<dyn std::any::Any + Send>, value: &Value) {
        let (int_sum, float_sum, is_float) = state.downcast_mut::<(i64, f64, bool)>().unwrap();
        
        match value {
            Value::Integer(i) => {
                if *is_float {
                    *float_sum += *i as f64;
                } else {
                    *int_sum += *i;
                }
            },
            Value::Float(f) => {
                if !*is_float {
                    *float_sum = *int_sum as f64;
                    *is_float = true;
                }
                *float_sum += *f;
            },
            _ => {}, // Ignore other types
        }
    }
    
    fn finalize(&self, state: Box<dyn std::any::Any + Send>) -> Value {
        let (int_sum, float_sum, is_float) = *state.downcast::<(i64, f64, bool)>().unwrap();
        
        if is_float {
            Value::Float(float_sum)
        } else {
            Value::Integer(int_sum)
        }
    }
}

/// Average aggregation function
pub struct AvgFunction;

impl AggregateFunction for AvgFunction {
    fn name(&self) -> &str {
        "avg"
    }
    
    fn output_type(&self, _input_type: &DataType) -> DataType {
        DataType::Float
    }
    
    fn init(&self) -> Box<dyn std::any::Any + Send> {
        Box::new((0.0f64, 0i64)) // (sum, count)
    }
    
    fn update(&self, state: &mut Box<dyn std::any::Any + Send>, value: &Value) {
        let (sum, count) = state.downcast_mut::<(f64, i64)>().unwrap();
        
        match value {
            Value::Integer(i) => {
                *sum += *i as f64;
                *count += 1;
            },
            Value::Float(f) => {
                *sum += *f;
                *count += 1;
            },
            _ => {}, // Ignore other types
        }
    }
    
    fn finalize(&self, state: Box<dyn std::any::Any + Send>) -> Value {
        let (sum, count) = *state.downcast::<(f64, i64)>().unwrap();
        
        if count > 0 {
            Value::Float(sum / count as f64)
        } else {
            Value::Null
        }
    }
}

/// Min aggregation function
pub struct MinFunction;

impl AggregateFunction for MinFunction {
    fn name(&self) -> &str {
        "min"
    }
    
    fn output_type(&self, input_type: &DataType) -> DataType {
        input_type.clone()
    }
    
    fn init(&self) -> Box<dyn std::any::Any + Send> {
        Box::new((i64::MAX, f64::MAX, String::new(), false, false, false)) // (int_min, float_min, string_min, has_int, has_float, has_string)
    }
    
    fn update(&self, state: &mut Box<dyn std::any::Any + Send>, value: &Value) {
        let (int_min, float_min, string_min, has_int, has_float, has_string) = 
            state.downcast_mut::<(i64, f64, String, bool, bool, bool)>().unwrap();
        
        match value {
            Value::Integer(i) => {
                if !*has_int || *i < *int_min {
                    *int_min = *i;
                    *has_int = true;
                }
            },
            Value::Float(f) => {
                if !*has_float || *f < *float_min {
                    *float_min = *f;
                    *has_float = true;
                }
            },
            Value::String(s) => {
                if !*has_string || s < string_min {
                    *string_min = s.clone();
                    *has_string = true;
                }
            },
            _ => {}, // Ignore other types
        }
    }
    
    fn finalize(&self, state: Box<dyn std::any::Any + Send>) -> Value {
        let (int_min, float_min, string_min, has_int, has_float, has_string) = 
            *state.downcast::<(i64, f64, String, bool, bool, bool)>().unwrap();
        
        if has_int {
            Value::Integer(int_min)
        } else if has_float {
            Value::Float(float_min)
        } else if has_string {
            Value::String(string_min)
        } else {
            Value::Null
        }
    }
}

/// Max aggregation function
pub struct MaxFunction;

impl AggregateFunction for MaxFunction {
    fn name(&self) -> &str {
        "max"
    }
    
    fn output_type(&self, input_type: &DataType) -> DataType {
        input_type.clone()
    }
    
    fn init(&self) -> Box<dyn std::any::Any + Send> {
        Box::new((i64::MIN, f64::MIN, String::new(), false, false, false)) // (int_max, float_max, string_max, has_int, has_float, has_string)
    }
    
    fn update(&self, state: &mut Box<dyn std::any::Any + Send>, value: &Value) {
        let (int_max, float_max, string_max, has_int, has_float, has_string) = 
            state.downcast_mut::<(i64, f64, String, bool, bool, bool)>().unwrap();
        
        match value {
            Value::Integer(i) => {
                if !*has_int || *i > *int_max {
                    *int_max = *i;
                    *has_int = true;
                }
            },
            Value::Float(f) => {
                if !*has_float || *f > *float_max {
                    *float_max = *f;
                    *has_float = true;
                }
            },
            Value::String(s) => {
                if !*has_string || s > string_max {
                    *string_max = s.clone();
                    *has_string = true;
                }
            },
            _ => {}, // Ignore other types
        }
    }
    
    fn finalize(&self, state: Box<dyn std::any::Any + Send>) -> Value {
        let (int_max, float_max, string_max, has_int, has_float, has_string) = 
            *state.downcast::<(i64, f64, String, bool, bool, bool)>().unwrap();
        
        if has_int {
            Value::Integer(int_max)
        } else if has_float {
            Value::Float(float_max)
        } else if has_string {
            Value::String(string_max)
        } else {
            Value::Null
        }
    }
}

/// Group by processor for aggregating data
pub struct GroupByProcessor {
    group_by_columns: Vec<String>,
    aggregations: Vec<(String, String, Box<dyn AggregateFunction>)>, // (output_name, input_column, function)
}

impl GroupByProcessor {
    /// Create a new group by processor
    pub fn new() -> Self {
        GroupByProcessor {
            group_by_columns: Vec::new(),
            aggregations: Vec::new(),
        }
    }
    
    /// Add a column to group by
    pub fn group_by(mut self, column: &str) -> Self {
        self.group_by_columns.push(column.to_string());
        self
    }
    
    /// Add an aggregation
    pub fn aggregate<F: AggregateFunction + 'static>(
        mut self,
        output_name: &str,
        input_column: &str,
        function: F,
    ) -> Self {
        self.aggregations.push((
            output_name.to_string(),
            input_column.to_string(),
            Box::new(function),
        ));
        self
    }
    
    /// Add a count aggregation
    pub fn count(self, output_name: &str, input_column: &str) -> Self {
        self.aggregate(output_name, input_column, CountFunction)
    }
    
    /// Add a sum aggregation
    pub fn sum(self, output_name: &str, input_column: &str) -> Self {
        self.aggregate(output_name, input_column, SumFunction)
    }
    
    /// Add an average aggregation
    pub fn avg(self, output_name: &str, input_column: &str) -> Self {
        self.aggregate(output_name, input_column, AvgFunction)
    }
    
    /// Add a min aggregation
    pub fn min(self, output_name: &str, input_column: &str) -> Self {
        self.aggregate(output_name, input_column, MinFunction)
    }
    
    /// Add a max aggregation
    pub fn max(self, output_name: &str, input_column: &str) -> Self {
        self.aggregate(output_name, input_column, MaxFunction)
    }
}

impl DataProcessor for GroupByProcessor {
    fn process(&self, input: &DataSet) -> Result<DataSet, ProcessingError> {
        if self.group_by_columns.is_empty() && self.aggregations.is_empty() {
            return Err(ProcessingError::InvalidArgument(
                "Group by processor requires at least one group by column or aggregation".to_string()
            ));
        }
        
        // Find column indices for group by columns
        let mut group_by_indices = Vec::new();
        let mut group_by_fields = Vec::new();
        
        for col in &self.group_by_columns {
            let mut found = false;
            
            for (i, field) in input.schema.fields.iter().enumerate() {
                if &field.name == col {
                    group_by_indices.push(i);
                    group_by_fields.push(field.clone());
                    found = true;
                    break;
                }
            }
            
            if !found {
                return Err(ProcessingError::InvalidArgument(
                    format!("Group by column '{}' not found", col)
                ));
            }
        }
        
        // Find column indices and types for aggregation columns
        let mut agg_indices = Vec::new();
        let mut agg_output_fields = Vec::new();
        
        for (output_name, input_column, function) in &self.aggregations {
            let mut found = false;
            
            for (i, field) in input.schema.fields.iter().enumerate() {
                if &field.name == input_column {
                    agg_indices.push(i);
                    
                    // Create output field with the function's output type
                    let output_type = function.output_type(&field.data_type);
                    agg_output_fields.push(Field::new(
                        output_name.clone(),
                        output_type,
                        true,
                    ));
                    
                    found = true;
                    break;
                }
            }
            
            if !found {
                return Err(ProcessingError::InvalidArgument(
                    format!("Aggregation column '{}' not found", input_column)
                ));
            }
        }
        
        // Create output schema
        let mut output_fields = group_by_fields;
        output_fields.extend(agg_output_fields);
        let output_schema = Schema::new(output_fields);
        
        // Group rows by the group by columns
        let mut groups: HashMap<Vec<Value>, Vec<&Row>> = HashMap::new();
        
        for row in &input.data {
            let key: Vec<Value> = group_by_indices.iter()
                .map(|&i| row.values[i].clone())
                .collect();
            
            groups.entry(key).or_default().push(row);
        }
        
        // Initialize result dataset
        let mut result = DataSet::new(output_schema);
        
        // Process each group
        for (key, rows) in groups {
            // Initialize aggregation states
            let mut agg_states: Vec<Box<dyn std::any::Any + Send>> = self.aggregations.iter()
                .map(|(_, _, function)| function.init())
                .collect();
            
            // Update aggregation states with each row
            for row in rows {
                for (i, (_, _, function)) in self.aggregations.iter().enumerate() {
                    let col_idx = agg_indices[i];
                    function.update(&mut agg_states[i], &row.values[col_idx]);
                }
            }
            
            // Finalize aggregations
            let agg_results: Vec<Value> = self.aggregations.iter().enumerate()
                .map(|(i, (_, _, function))| function.finalize(std::mem::replace(&mut agg_states[i], function.init())))
                .collect();
            
            // Create output row
            let mut output_values = key;
            output_values.extend(agg_results);
            
            let output_row = Row::new(output_values);
            result.add_row(output_row)?;
        }
        
        // Copy metadata
        for (key, value) in &input.metadata.properties {
            result.metadata.add(key.clone(), value.clone());
        }
        
        Ok(result)
    }
    
    fn name(&self) -> &str {
        "group_by"
    }
    
    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Aggregate
    }
}

