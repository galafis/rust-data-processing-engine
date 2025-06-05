// Data module for handling data structures and formats
// Author: Gabriel Demetrios Lafis

mod csv;
mod json;
mod parquet;
mod schema;

pub use csv::*;
pub use json::*;
pub use parquet::*;
pub use schema::*;

use std::error::Error;
use std::fmt;

/// Represents a generic data source
pub trait DataSource {
    /// Read data from the source
    fn read(&self) -> Result<DataSet, DataError>;
    
    /// Get the source name
    fn name(&self) -> &str;
    
    /// Get the source type
    fn source_type(&self) -> SourceType;
}

/// Represents a generic data sink
pub trait DataSink {
    /// Write data to the sink
    fn write(&self, data: &DataSet) -> Result<(), DataError>;
    
    /// Get the sink name
    fn name(&self) -> &str;
    
    /// Get the sink type
    fn sink_type(&self) -> SinkType;
}

/// Represents a dataset with schema and data
#[derive(Debug, Clone)]
pub struct DataSet {
    pub schema: Schema,
    pub data: Vec<Row>,
    pub metadata: Metadata,
}

impl DataSet {
    /// Create a new empty dataset
    pub fn new(schema: Schema) -> Self {
        DataSet {
            schema,
            data: Vec::new(),
            metadata: Metadata::new(),
        }
    }
    
    /// Add a row to the dataset
    pub fn add_row(&mut self, row: Row) -> Result<(), DataError> {
        if row.values.len() != self.schema.fields.len() {
            return Err(DataError::SchemaMismatch);
        }
        
        self.data.push(row);
        Ok(())
    }
    
    /// Get the number of rows in the dataset
    pub fn len(&self) -> usize {
        self.data.len()
    }
    
    /// Check if the dataset is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
    
    /// Get a reference to a row by index
    pub fn get_row(&self, index: usize) -> Option<&Row> {
        self.data.get(index)
    }
    
    /// Get a mutable reference to a row by index
    pub fn get_row_mut(&mut self, index: usize) -> Option<&mut Row> {
        self.data.get_mut(index)
    }
}

/// Represents a row in a dataset
#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    /// Create a new row with the given values
    pub fn new(values: Vec<Value>) -> Self {
        Row { values }
    }
    
    /// Get a reference to a value by index
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }
    
    /// Get a mutable reference to a value by index
    pub fn get_mut(&mut self, index: usize) -> Option<&mut Value> {
        self.values.get_mut(index)
    }
}

/// Represents a value in a row
#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Binary(Vec<u8>),
    Array(Vec<Value>),
    Map(std::collections::HashMap<String, Value>),
}

/// Represents a schema for a dataset
#[derive(Debug, Clone)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    /// Create a new schema with the given fields
    pub fn new(fields: Vec<Field>) -> Self {
        Schema { fields }
    }
    
    /// Get a reference to a field by name
    pub fn get_field_by_name(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.name == name)
    }
    
    /// Get a reference to a field by index
    pub fn get_field(&self, index: usize) -> Option<&Field> {
        self.fields.get(index)
    }
}

/// Represents a field in a schema
#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl Field {
    /// Create a new field
    pub fn new(name: String, data_type: DataType, nullable: bool) -> Self {
        Field {
            name,
            data_type,
            nullable,
        }
    }
}

/// Represents a data type for a field
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
    Binary,
    Array(Box<DataType>),
    Map(Box<DataType>),
}

/// Represents metadata for a dataset
#[derive(Debug, Clone)]
pub struct Metadata {
    pub properties: std::collections::HashMap<String, String>,
}

impl Metadata {
    /// Create new empty metadata
    pub fn new() -> Self {
        Metadata {
            properties: std::collections::HashMap::new(),
        }
    }
    
    /// Add a property to the metadata
    pub fn add(&mut self, key: String, value: String) {
        self.properties.insert(key, value);
    }
    
    /// Get a property from the metadata
    pub fn get(&self, key: &str) -> Option<&String> {
        self.properties.get(key)
    }
}

/// Represents a source type
#[derive(Debug, Clone, PartialEq)]
pub enum SourceType {
    File,
    Database,
    Stream,
    API,
    Custom(String),
}

/// Represents a sink type
#[derive(Debug, Clone, PartialEq)]
pub enum SinkType {
    File,
    Database,
    Stream,
    API,
    Custom(String),
}

/// Represents an error in the data module
#[derive(Debug)]
pub enum DataError {
    IoError(std::io::Error),
    ParseError(String),
    SchemaMismatch,
    ValidationError(String),
    NotSupported(String),
    Other(String),
}

impl fmt::Display for DataError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DataError::IoError(err) => write!(f, "IO error: {}", err),
            DataError::ParseError(msg) => write!(f, "Parse error: {}", msg),
            DataError::SchemaMismatch => write!(f, "Schema mismatch"),
            DataError::ValidationError(msg) => write!(f, "Validation error: {}", msg),
            DataError::NotSupported(msg) => write!(f, "Not supported: {}", msg),
            DataError::Other(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl Error for DataError {}

impl From<std::io::Error> for DataError {
    fn from(err: std::io::Error) -> Self {
        DataError::IoError(err)
    }
}

