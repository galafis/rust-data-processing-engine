// Statistical operations for data processing
// Author: Gabriel Demetrios Lafis

use crate::data::{DataSet, DataType, Field, Row, Schema, Value};
use super::{DataProcessor, ProcessingError, ProcessorType};

/// Statistical processor for computing statistics on datasets
pub struct StatsProcessor {
    name: String,
    columns: Vec<String>,
    stats_type: StatsType,
}

/// Type of statistical operation
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StatsType {
    Mean,
    Median,
    Mode,
    StdDev,
    Variance,
    Min,
    Max,
    Range,
    Sum,
    Count,
    Quantile,
    Correlation,
    Covariance,
}

impl StatsProcessor {
    /// Create a new stats processor
    pub fn new(name: &str, columns: Vec<String>, stats_type: StatsType) -> Self {
        StatsProcessor {
            name: name.to_string(),
            columns,
            stats_type,
        }
    }
    
    /// Create a mean processor
    pub fn mean(column: &str) -> Self {
        Self::new("mean", vec![column.to_string()], StatsType::Mean)
    }
    
    /// Create a median processor
    pub fn median(column: &str) -> Self {
        Self::new("median", vec![column.to_string()], StatsType::Median)
    }
    
    /// Create a mode processor
    pub fn mode(column: &str) -> Self {
        Self::new("mode", vec![column.to_string()], StatsType::Mode)
    }
    
    /// Create a standard deviation processor
    pub fn std_dev(column: &str) -> Self {
        Self::new("std_dev", vec![column.to_string()], StatsType::StdDev)
    }
    
    /// Create a variance processor
    pub fn variance(column: &str) -> Self {
        Self::new("variance", vec![column.to_string()], StatsType::Variance)
    }
    
    /// Create a min processor
    pub fn min(column: &str) -> Self {
        Self::new("min", vec![column.to_string()], StatsType::Min)
    }
    
    /// Create a max processor
    pub fn max(column: &str) -> Self {
        Self::new("max", vec![column.to_string()], StatsType::Max)
    }
    
    /// Create a range processor
    pub fn range(column: &str) -> Self {
        Self::new("range", vec![column.to_string()], StatsType::Range)
    }
    
    /// Create a sum processor
    pub fn sum(column: &str) -> Self {
        Self::new("sum", vec![column.to_string()], StatsType::Sum)
    }
    
    /// Create a count processor
    pub fn count(column: &str) -> Self {
        Self::new("count", vec![column.to_string()], StatsType::Count)
    }
    
    /// Create a quantile processor
    pub fn quantile(column: &str, quantile: f64) -> Self {
        let mut processor = Self::new("quantile", vec![column.to_string()], StatsType::Quantile);
        processor.quantile = quantile;
        processor
    }
    
    /// Create a correlation processor
    pub fn correlation(column1: &str, column2: &str) -> Self {
        Self::new(
            "correlation",
            vec![column1.to_string(), column2.to_string()],
            StatsType::Correlation,
        )
    }
    
    /// Create a covariance processor
    pub fn covariance(column1: &str, column2: &str) -> Self {
        Self::new(
            "covariance",
            vec![column1.to_string(), column2.to_string()],
            StatsType::Covariance,
        )
    }
    
    /// Get numeric values from a column
    fn get_numeric_values(&self, input: &DataSet, column: &str) -> Result<Vec<f64>, ProcessingError> {
        let mut values = Vec::new();
        
        // Find column index
        let mut col_idx = None;
        for (i, field) in input.schema.fields.iter().enumerate() {
            if field.name == column {
                col_idx = Some(i);
                break;
            }
        }
        
        let col_idx = col_idx.ok_or_else(|| ProcessingError::InvalidArgument(
            format!("Column '{}' not found", column)
        ))?;
        
        // Extract numeric values
        for row in &input.data {
            match &row.values[col_idx] {
                Value::Integer(i) => values.push(*i as f64),
                Value::Float(f) => values.push(*f),
                _ => {}, // Ignore non-numeric values
            }
        }
        
        Ok(values)
    }
    
    /// Compute mean of values
    fn compute_mean(&self, values: &[f64]) -> f64 {
        if values.is_empty() {
            return 0.0;
        }
        
        values.iter().sum::<f64>() / values.len() as f64
    }
    
    /// Compute median of values
    fn compute_median(&self, values: &[f64]) -> f64 {
        if values.is_empty() {
            return 0.0;
        }
        
        let mut sorted = values.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        let mid = sorted.len() / 2;
        if sorted.len() % 2 == 0 {
            (sorted[mid - 1] + sorted[mid]) / 2.0
        } else {
            sorted[mid]
        }
    }
    
    /// Compute mode of values
    fn compute_mode(&self, values: &[f64]) -> f64 {
        if values.is_empty() {
            return 0.0;
        }
        
        let mut counts = std::collections::HashMap::new();
        for &value in values {
            *counts.entry(value).or_insert(0) += 1;
        }
        
        counts.into_iter()
            .max_by_key(|&(_, count)| count)
            .map(|(value, _)| value)
            .unwrap_or(0.0)
    }
    
    /// Compute standard deviation of values
    fn compute_std_dev(&self, values: &[f64]) -> f64 {
        if values.is_empty() {
            return 0.0;
        }
        
        let mean = self.compute_mean(values);
        let variance = values.iter()
            .map(|&x| (x - mean).powi(2))
            .sum::<f64>() / values.len() as f64;
        
        variance.sqrt()
    }
    
    /// Compute variance of values
    fn compute_variance(&self, values: &[f64]) -> f64 {
        if values.is_empty() {
            return 0.0;
        }
        
        let mean = self.compute_mean(values);
        values.iter()
            .map(|&x| (x - mean).powi(2))
            .sum::<f64>() / values.len() as f64
    }
    
    /// Compute minimum of values
    fn compute_min(&self, values: &[f64]) -> f64 {
        values.iter()
            .fold(f64::INFINITY, |a, &b| a.min(b))
    }
    
    /// Compute maximum of values
    fn compute_max(&self, values: &[f64]) -> f64 {
        values.iter()
            .fold(f64::NEG_INFINITY, |a, &b| a.max(b))
    }
    
    /// Compute range of values
    fn compute_range(&self, values: &[f64]) -> f64 {
        if values.is_empty() {
            return 0.0;
        }
        
        let min = self.compute_min(values);
        let max = self.compute_max(values);
        max - min
    }
    
    /// Compute sum of values
    fn compute_sum(&self, values: &[f64]) -> f64 {
        values.iter().sum()
    }
    
    /// Compute count of values
    fn compute_count(&self, values: &[f64]) -> f64 {
        values.len() as f64
    }
    
    /// Compute quantile of values
    fn compute_quantile(&self, values: &[f64], q: f64) -> f64 {
        if values.is_empty() {
            return 0.0;
        }
        
        let mut sorted = values.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        let pos = q * (sorted.len() - 1) as f64;
        let idx = pos.floor() as usize;
        let frac = pos - idx as f64;
        
        if idx + 1 < sorted.len() {
            sorted[idx] + frac * (sorted[idx + 1] - sorted[idx])
        } else {
            sorted[idx]
        }
    }
    
    /// Compute correlation between two sets of values
    fn compute_correlation(&self, values1: &[f64], values2: &[f64]) -> f64 {
        if values1.is_empty() || values2.is_empty() || values1.len() != values2.len() {
            return 0.0;
        }
        
        let mean1 = self.compute_mean(values1);
        let mean2 = self.compute_mean(values2);
        
        let mut numerator = 0.0;
        let mut denom1 = 0.0;
        let mut denom2 = 0.0;
        
        for i in 0..values1.len() {
            let diff1 = values1[i] - mean1;
            let diff2 = values2[i] - mean2;
            
            numerator += diff1 * diff2;
            denom1 += diff1 * diff1;
            denom2 += diff2 * diff2;
        }
        
        if denom1 == 0.0 || denom2 == 0.0 {
            0.0
        } else {
            numerator / (denom1.sqrt() * denom2.sqrt())
        }
    }
    
    /// Compute covariance between two sets of values
    fn compute_covariance(&self, values1: &[f64], values2: &[f64]) -> f64 {
        if values1.is_empty() || values2.is_empty() || values1.len() != values2.len() {
            return 0.0;
        }
        
        let mean1 = self.compute_mean(values1);
        let mean2 = self.compute_mean(values2);
        
        let mut sum = 0.0;
        for i in 0..values1.len() {
            sum += (values1[i] - mean1) * (values2[i] - mean2);
        }
        
        sum / values1.len() as f64
    }
}

impl DataProcessor for StatsProcessor {
    fn process(&self, input: &DataSet) -> Result<DataSet, ProcessingError> {
        // Create output schema with a single row and column
        let output_fields = vec![
            Field::new(self.name.clone(), DataType::Float, false),
        ];
        
        let output_schema = Schema::new(output_fields);
        let mut result = DataSet::new(output_schema);
        
        // Compute statistic
        let stat_value = match self.stats_type {
            StatsType::Mean => {
                let values = self.get_numeric_values(input, &self.columns[0])?;
                Value::Float(self.compute_mean(&values))
            },
            StatsType::Median => {
                let values = self.get_numeric_values(input, &self.columns[0])?;
                Value::Float(self.compute_median(&values))
            },
            StatsType::Mode => {
                let values = self.get_numeric_values(input, &self.columns[0])?;
                Value::Float(self.compute_mode(&values))
            },
            StatsType::StdDev => {
                let values = self.get_numeric_values(input, &self.columns[0])?;
                Value::Float(self.compute_std_dev(&values))
            },
            StatsType::Variance => {
                let values = self.get_numeric_values(input, &self.columns[0])?;
                Value::Float(self.compute_variance(&values))
            },
            StatsType::Min => {
                let values = self.get_numeric_values(input, &self.columns[0])?;
                Value::Float(self.compute_min(&values))
            },
            StatsType::Max => {
                let values = self.get_numeric_values(input, &self.columns[0])?;
                Value::Float(self.compute_max(&values))
            },
            StatsType::Range => {
                let values = self.get_numeric_values(input, &self.columns[0])?;
                Value::Float(self.compute_range(&values))
            },
            StatsType::Sum => {
                let values = self.get_numeric_values(input, &self.columns[0])?;
                Value::Float(self.compute_sum(&values))
            },
            StatsType::Count => {
                let values = self.get_numeric_values(input, &self.columns[0])?;
                Value::Float(self.compute_count(&values))
            },
            StatsType::Quantile => {
                let values = self.get_numeric_values(input, &self.columns[0])?;
                Value::Float(self.compute_quantile(&values, self.quantile))
            },
            StatsType::Correlation => {
                if self.columns.len() < 2 {
                    return Err(ProcessingError::InvalidArgument(
                        "Correlation requires two columns".to_string()
                    ));
                }
                
                let values1 = self.get_numeric_values(input, &self.columns[0])?;
                let values2 = self.get_numeric_values(input, &self.columns[1])?;
                Value::Float(self.compute_correlation(&values1, &values2))
            },
            StatsType::Covariance => {
                if self.columns.len() < 2 {
                    return Err(ProcessingError::InvalidArgument(
                        "Covariance requires two columns".to_string()
                    ));
                }
                
                let values1 = self.get_numeric_values(input, &self.columns[0])?;
                let values2 = self.get_numeric_values(input, &self.columns[1])?;
                Value::Float(self.compute_covariance(&values1, &values2))
            },
        };
        
        // Create output row
        let output_row = Row::new(vec![stat_value]);
        result.add_row(output_row)?;
        
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
        ProcessorType::Stats
    }
}

