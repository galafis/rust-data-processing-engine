// Window operations for data processing
// Author: Gabriel Demetrios Lafis

use crate::data::{DataSet, DataType, Field, Row, Schema, Value};
use super::{DataProcessor, ProcessingError, ProcessorType};

/// Window function type
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WindowFunctionType {
    RowNumber,
    Rank,
    DenseRank,
    Lead,
    Lag,
    FirstValue,
    LastValue,
    NthValue,
    Custom(fn(&[&Row], usize) -> Value),
}

/// Window processor for window functions
pub struct WindowProcessor {
    output_column: String,
    function_type: WindowFunctionType,
    partition_by: Vec<String>,
    order_by: Vec<(String, bool)>, // (column, ascending)
    function_args: Vec<Value>,
}

impl WindowProcessor {
    /// Create a new window processor
    pub fn new(
        output_column: &str,
        function_type: WindowFunctionType,
        partition_by: Vec<String>,
        order_by: Vec<(String, bool)>,
        function_args: Vec<Value>,
    ) -> Self {
        WindowProcessor {
            output_column: output_column.to_string(),
            function_type,
            partition_by,
            order_by,
            function_args,
        }
    }
    
    /// Create a row number window function
    pub fn row_number(output_column: &str) -> Self {
        Self::new(
            output_column,
            WindowFunctionType::RowNumber,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
    }
    
    /// Create a rank window function
    pub fn rank(output_column: &str) -> Self {
        Self::new(
            output_column,
            WindowFunctionType::Rank,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
    }
    
    /// Create a dense rank window function
    pub fn dense_rank(output_column: &str) -> Self {
        Self::new(
            output_column,
            WindowFunctionType::DenseRank,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
    }
    
    /// Create a lead window function
    pub fn lead(output_column: &str, offset: i64) -> Self {
        Self::new(
            output_column,
            WindowFunctionType::Lead,
            Vec::new(),
            Vec::new(),
            vec![Value::Integer(offset)],
        )
    }
    
    /// Create a lag window function
    pub fn lag(output_column: &str, offset: i64) -> Self {
        Self::new(
            output_column,
            WindowFunctionType::Lag,
            Vec::new(),
            Vec::new(),
            vec![Value::Integer(offset)],
        )
    }
    
    /// Create a first value window function
    pub fn first_value(output_column: &str) -> Self {
        Self::new(
            output_column,
            WindowFunctionType::FirstValue,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
    }
    
    /// Create a last value window function
    pub fn last_value(output_column: &str) -> Self {
        Self::new(
            output_column,
            WindowFunctionType::LastValue,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
    }
    
    /// Create a nth value window function
    pub fn nth_value(output_column: &str, n: i64) -> Self {
        Self::new(
            output_column,
            WindowFunctionType::NthValue,
            Vec::new(),
            Vec::new(),
            vec![Value::Integer(n)],
        )
    }
    
    /// Add partition by columns
    pub fn partition_by(mut self, columns: Vec<String>) -> Self {
        self.partition_by = columns;
        self
    }
    
    /// Add order by columns
    pub fn order_by(mut self, columns: Vec<(String, bool)>) -> Self {
        self.order_by = columns;
        self
    }
    
    /// Apply a window function to a partition
    fn apply_window_function(&self, partition: &[&Row], row_idx: usize) -> Result<Value, ProcessingError> {
        match self.function_type {
            WindowFunctionType::RowNumber => {
                Ok(Value::Integer((row_idx + 1) as i64))
            },
            WindowFunctionType::Rank => {
                if self.order_by.is_empty() {
                    return Err(ProcessingError::InvalidArgument(
                        "Rank function requires order by columns".to_string()
                    ));
                }
                
                let mut rank = 1;
                let current_row = partition[row_idx];
                
                for (i, row) in partition.iter().enumerate() {
                    if i >= row_idx {
                        break;
                    }
                    
                    let mut equal = true;
                    
                    for (col, ascending) in &self.order_by {
                        let col_idx = self.find_column_index(col)?;
                        
                        match self.compare_values(&row.values[col_idx], &current_row.values[col_idx]) {
                            std::cmp::Ordering::Equal => {},
                            _ => {
                                equal = false;
                                break;
                            }
                        }
                    }
                    
                    if !equal {
                        rank += 1;
                    }
                }
                
                Ok(Value::Integer(rank))
            },
            WindowFunctionType::DenseRank => {
                if self.order_by.is_empty() {
                    return Err(ProcessingError::InvalidArgument(
                        "Dense rank function requires order by columns".to_string()
                    ));
                }
                
                let mut rank = 1;
                let mut prev_row = None;
                let current_row = partition[row_idx];
                
                for (i, row) in partition.iter().enumerate() {
                    if i >= row_idx {
                        break;
                    }
                    
                    if let Some(prev) = prev_row {
                        let mut equal = true;
                        
                        for (col, ascending) in &self.order_by {
                            let col_idx = self.find_column_index(col)?;
                            
                            match self.compare_values(&prev.values[col_idx], &row.values[col_idx]) {
                                std::cmp::Ordering::Equal => {},
                                _ => {
                                    equal = false;
                                    break;
                                }
                            }
                        }
                        
                        if !equal {
                            rank += 1;
                        }
                    }
                    
                    prev_row = Some(row);
                }
                
                Ok(Value::Integer(rank))
            },
            WindowFunctionType::Lead => {
                if self.function_args.is_empty() {
                    return Err(ProcessingError::InvalidArgument(
                        "Lead function requires offset argument".to_string()
                    ));
                }
                
                let offset = match &self.function_args[0] {
                    Value::Integer(i) => *i as usize,
                    _ => return Err(ProcessingError::InvalidArgument(
                        "Lead function offset must be an integer".to_string()
                    )),
                };
                
                let lead_idx = row_idx + offset;
                
                if lead_idx < partition.len() {
                    if self.order_by.is_empty() {
                        Ok(Value::Integer((lead_idx + 1) as i64))
                    } else {
                        let col = &self.order_by[0].0;
                        let col_idx = self.find_column_index(col)?;
                        Ok(partition[lead_idx].values[col_idx].clone())
                    }
                } else {
                    Ok(Value::Null)
                }
            },
            WindowFunctionType::Lag => {
                if self.function_args.is_empty() {
                    return Err(ProcessingError::InvalidArgument(
                        "Lag function requires offset argument".to_string()
                    ));
                }
                
                let offset = match &self.function_args[0] {
                    Value::Integer(i) => *i as usize,
                    _ => return Err(ProcessingError::InvalidArgument(
                        "Lag function offset must be an integer".to_string()
                    )),
                };
                
                if offset <= row_idx {
                    let lag_idx = row_idx - offset;
                    
                    if self.order_by.is_empty() {
                        Ok(Value::Integer((lag_idx + 1) as i64))
                    } else {
                        let col = &self.order_by[0].0;
                        let col_idx = self.find_column_index(col)?;
                        Ok(partition[lag_idx].values[col_idx].clone())
                    }
                } else {
                    Ok(Value::Null)
                }
            },
            WindowFunctionType::FirstValue => {
                if partition.is_empty() {
                    Ok(Value::Null)
                } else if self.order_by.is_empty() {
                    Ok(Value::Integer(1))
                } else {
                    let col = &self.order_by[0].0;
                    let col_idx = self.find_column_index(col)?;
                    Ok(partition[0].values[col_idx].clone())
                }
            },
            WindowFunctionType::LastValue => {
                if partition.is_empty() {
                    Ok(Value::Null)
                } else if self.order_by.is_empty() {
                    Ok(Value::Integer(partition.len() as i64))
                } else {
                    let col = &self.order_by[0].0;
                    let col_idx = self.find_column_index(col)?;
                    Ok(partition[partition.len() - 1].values[col_idx].clone())
                }
            },
            WindowFunctionType::NthValue => {
                if self.function_args.is_empty() {
                    return Err(ProcessingError::InvalidArgument(
                        "Nth value function requires n argument".to_string()
                    ));
                }
                
                let n = match &self.function_args[0] {
                    Value::Integer(i) => *i as usize,
                    _ => return Err(ProcessingError::InvalidArgument(
                        "Nth value function n must be an integer".to_string()
                    )),
                };
                
                if n == 0 || n > partition.len() {
                    Ok(Value::Null)
                } else if self.order_by.is_empty() {
                    Ok(Value::Integer(n as i64))
                } else {
                    let col = &self.order_by[0].0;
                    let col_idx = self.find_column_index(col)?;
                    Ok(partition[n - 1].values[col_idx].clone())
                }
            },
            WindowFunctionType::Custom(f) => {
                Ok(f(partition, row_idx))
            },
        }
    }
    
    /// Find the index of a column
    fn find_column_index(&self, column: &str) -> Result<usize, ProcessingError> {
        for (i, field) in self.schema.fields.iter().enumerate() {
            if field.name == column {
                return Ok(i);
            }
        }
        
        Err(ProcessingError::InvalidArgument(
            format!("Column '{}' not found", column)
        ))
    }
    
    /// Compare two values
    fn compare_values(&self, a: &Value, b: &Value) -> std::cmp::Ordering {
        match (a, b) {
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Null, _) => std::cmp::Ordering::Less,
            (_, Value::Null) => std::cmp::Ordering::Greater,
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            _ => std::cmp::Ordering::Equal,
        }
    }
}

impl DataProcessor for WindowProcessor {
    fn process(&self, input: &DataSet) -> Result<DataSet, ProcessingError> {
        // Check if output column already exists
        for field in &input.schema.fields {
            if field.name == self.output_column {
                return Err(ProcessingError::InvalidArgument(
                    format!("Output column '{}' already exists", self.output_column)
                ));
            }
        }
        
        // Create output schema
        let mut output_fields = input.schema.fields.clone();
        output_fields.push(Field::new(
            self.output_column.clone(),
            DataType::Integer, // Most window functions return integers
            true,
        ));
        
        let output_schema = Schema::new(output_fields);
        let mut result = DataSet::new(output_schema);
        
        // Store schema for later use
        self.schema = input.schema.clone();
        
        // Find partition by column indices
        let mut partition_indices = Vec::new();
        for col in &self.partition_by {
            let mut found = false;
            
            for (i, field) in input.schema.fields.iter().enumerate() {
                if &field.name == col {
                    partition_indices.push(i);
                    found = true;
                    break;
                }
            }
            
            if !found {
                return Err(ProcessingError::InvalidArgument(
                    format!("Partition by column '{}' not found", col)
                ));
            }
        }
        
        // Find order by column indices
        let mut order_indices = Vec::new();
        for (col, _) in &self.order_by {
            let mut found = false;
            
            for (i, field) in input.schema.fields.iter().enumerate() {
                if &field.name == col {
                    order_indices.push(i);
                    found = true;
                    break;
                }
            }
            
            if !found {
                return Err(ProcessingError::InvalidArgument(
                    format!("Order by column '{}' not found", col)
                ));
            }
        }
        
        // Group rows by partition
        let mut partitions = Vec::new();
        
        if partition_indices.is_empty() {
            // Single partition with all rows
            partitions.push(input.data.iter().collect::<Vec<_>>());
        } else {
            // Group rows by partition key
            let mut partition_map = std::collections::HashMap::new();
            
            for row in &input.data {
                let key: Vec<Value> = partition_indices.iter()
                    .map(|&i| row.values[i].clone())
                    .collect();
                
                partition_map.entry(key).or_insert_with(Vec::new).push(row);
            }
            
            partitions.extend(partition_map.values().cloned());
        }
        
        // Sort partitions if order by is specified
        if !order_indices.is_empty() {
            for partition in &mut partitions {
                partition.sort_by(|a, b| {
                    for (i, (_, ascending)) in order_indices.iter().zip(self.order_by.iter()) {
                        let cmp = self.compare_values(&a.values[*i], &b.values[*i]);
                        
                        if cmp != std::cmp::Ordering::Equal {
                            return if *ascending { cmp } else { cmp.reverse() };
                        }
                    }
                    
                    std::cmp::Ordering::Equal
                });
            }
        }
        
        // Apply window function to each row
        let mut window_values = Vec::new();
        
        for partition in &partitions {
            for (i, _) in partition.iter().enumerate() {
                let value = self.apply_window_function(partition, i)?;
                window_values.push(value);
            }
        }
        
        // Create output rows
        let mut value_idx = 0;
        for row in &input.data {
            let mut values = row.values.clone();
            values.push(window_values[value_idx].clone());
            value_idx += 1;
            
            let output_row = Row::new(values);
            result.add_row(output_row)?;
        }
        
        // Copy metadata
        for (key, value) in &input.metadata.properties {
            result.metadata.add(key.clone(), value.clone());
        }
        
        Ok(result)
    }
    
    fn name(&self) -> &str {
        "window"
    }
    
    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Window
    }
}

