// Processing module for data transformation and analysis
// Author: Gabriel Demetrios Lafis

mod transform;
mod filter;
mod aggregate;
mod join;
mod window;
mod stats;

pub use transform::*;
pub use filter::*;
pub use aggregate::*;
pub use join::*;
pub use window::*;
pub use stats::*;

use std::error::Error;
use std::fmt;

use crate::data::{DataError, DataSet, Row, Schema, Value};

/// Represents a data processor that transforms data
pub trait DataProcessor {
    /// Process a dataset and return a new dataset
    fn process(&self, input: &DataSet) -> Result<DataSet, ProcessingError>;
    
    /// Get the processor name
    fn name(&self) -> &str;
    
    /// Get the processor type
    fn processor_type(&self) -> ProcessorType;
}

/// Represents a data processor that transforms data in place
pub trait InPlaceDataProcessor {
    /// Process a dataset in place
    fn process_in_place(&self, input: &mut DataSet) -> Result<(), ProcessingError>;
    
    /// Get the processor name
    fn name(&self) -> &str;
    
    /// Get the processor type
    fn processor_type(&self) -> ProcessorType;
}

/// Represents a processor type
#[derive(Debug, Clone, PartialEq)]
pub enum ProcessorType {
    Transform,
    Filter,
    Aggregate,
    Join,
    Window,
    Stats,
    Custom(String),
}

/// Represents an error in the processing module
#[derive(Debug)]
pub enum ProcessingError {
    DataError(DataError),
    InvalidOperation(String),
    InvalidArgument(String),
    NotSupported(String),
    Other(String),
}

impl fmt::Display for ProcessingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ProcessingError::DataError(err) => write!(f, "Data error: {}", err),
            ProcessingError::InvalidOperation(msg) => write!(f, "Invalid operation: {}", msg),
            ProcessingError::InvalidArgument(msg) => write!(f, "Invalid argument: {}", msg),
            ProcessingError::NotSupported(msg) => write!(f, "Not supported: {}", msg),
            ProcessingError::Other(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl Error for ProcessingError {}

impl From<DataError> for ProcessingError {
    fn from(err: DataError) -> Self {
        ProcessingError::DataError(err)
    }
}

/// Pipeline for chaining multiple processors
pub struct Pipeline {
    name: String,
    processors: Vec<Box<dyn DataProcessor>>,
}

impl Pipeline {
    /// Create a new pipeline with the given name
    pub fn new(name: &str) -> Self {
        Pipeline {
            name: name.to_string(),
            processors: Vec::new(),
        }
    }
    
    /// Add a processor to the pipeline
    pub fn add<P: DataProcessor + 'static>(mut self, processor: P) -> Self {
        self.processors.push(Box::new(processor));
        self
    }
    
    /// Execute the pipeline on a dataset
    pub fn execute(&self, input: &DataSet) -> Result<DataSet, ProcessingError> {
        let mut current = input.clone();
        
        for processor in &self.processors {
            current = processor.process(&current)?;
        }
        
        Ok(current)
    }
}

impl DataProcessor for Pipeline {
    fn process(&self, input: &DataSet) -> Result<DataSet, ProcessingError> {
        self.execute(input)
    }
    
    fn name(&self) -> &str {
        &self.name
    }
    
    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Custom("Pipeline".to_string())
    }
}

