// CSV data source and sink implementation
// Author: Gabriel Demetrios Lafis

use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

use super::{DataError, DataSet, DataSink, DataSource, Field, Row, Schema, SinkType, SourceType, Value};

/// CSV data source
pub struct CsvSource {
    path: String,
    has_header: bool,
    delimiter: char,
}

impl CsvSource {
    /// Create a new CSV data source
    pub fn new<P: AsRef<Path>>(path: P, has_header: bool, delimiter: char) -> Self {
        CsvSource {
            path: path.as_ref().to_string_lossy().to_string(),
            has_header,
            delimiter,
        }
    }
}

impl DataSource for CsvSource {
    fn read(&self) -> Result<DataSet, DataError> {
        let file = File::open(&self.path).map_err(DataError::IoError)?;
        let reader = BufReader::new(file);
        
        let mut csv_reader = csv::ReaderBuilder::new()
            .delimiter(self.delimiter as u8)
            .has_headers(self.has_header)
            .from_reader(reader);
        
        // Read headers to create schema
        let headers: Vec<String> = if self.has_header {
            csv_reader.headers()
                .map_err(|e| DataError::ParseError(e.to_string()))?
                .iter()
                .map(|s| s.to_string())
                .collect()
        } else {
            // Generate column names if no header
            let record = csv_reader.records().next()
                .ok_or_else(|| DataError::ParseError("Empty CSV file".to_string()))?
                .map_err(|e| DataError::ParseError(e.to_string()))?;
            
            (0..record.len())
                .map(|i| format!("column_{}", i))
                .collect()
        };
        
        // Create schema with string fields
        let fields: Vec<Field> = headers.iter()
            .map(|name| Field::new(name.clone(), super::DataType::String, true))
            .collect();
        
        let schema = Schema::new(fields);
        let mut dataset = DataSet::new(schema);
        
        // Reset reader if we've already read a record
        if !self.has_header {
            let file = File::open(&self.path).map_err(DataError::IoError)?;
            let reader = BufReader::new(file);
            csv_reader = csv::ReaderBuilder::new()
                .delimiter(self.delimiter as u8)
                .has_headers(self.has_header)
                .from_reader(reader);
        }
        
        // Read data
        for result in csv_reader.records() {
            let record = result.map_err(|e| DataError::ParseError(e.to_string()))?;
            
            let values: Vec<Value> = record.iter()
                .map(|field| {
                    if field.is_empty() {
                        Value::Null
                    } else {
                        Value::String(field.to_string())
                    }
                })
                .collect();
            
            let row = Row::new(values);
            dataset.add_row(row)?;
        }
        
        // Add metadata
        dataset.metadata.add("source".to_string(), "csv".to_string());
        dataset.metadata.add("path".to_string(), self.path.clone());
        
        Ok(dataset)
    }
    
    fn name(&self) -> &str {
        &self.path
    }
    
    fn source_type(&self) -> SourceType {
        SourceType::File
    }
}

/// CSV data sink
pub struct CsvSink {
    path: String,
    delimiter: char,
}

impl CsvSink {
    /// Create a new CSV data sink
    pub fn new<P: AsRef<Path>>(path: P, delimiter: char) -> Self {
        CsvSink {
            path: path.as_ref().to_string_lossy().to_string(),
            delimiter,
        }
    }
}

impl DataSink for CsvSink {
    fn write(&self, data: &DataSet) -> Result<(), DataError> {
        let file = File::create(&self.path).map_err(DataError::IoError)?;
        let writer = BufWriter::new(file);
        
        let mut csv_writer = csv::WriterBuilder::new()
            .delimiter(self.delimiter as u8)
            .from_writer(writer);
        
        // Write headers
        let headers: Vec<&str> = data.schema.fields.iter()
            .map(|field| field.name.as_str())
            .collect();
        
        csv_writer.write_record(&headers)
            .map_err(|e| DataError::IoError(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
        
        // Write data
        for row in &data.data {
            let record: Vec<String> = row.values.iter()
                .map(|value| match value {
                    Value::Null => "".to_string(),
                    Value::Boolean(b) => b.to_string(),
                    Value::Integer(i) => i.to_string(),
                    Value::Float(f) => f.to_string(),
                    Value::String(s) => s.clone(),
                    Value::Binary(_) => "[binary data]".to_string(),
                    Value::Array(_) => "[array]".to_string(),
                    Value::Map(_) => "[map]".to_string(),
                })
                .collect();
            
            csv_writer.write_record(&record)
                .map_err(|e| DataError::IoError(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
        }
        
        csv_writer.flush()
            .map_err(DataError::IoError)?;
        
        Ok(())
    }
    
    fn name(&self) -> &str {
        &self.path
    }
    
    fn sink_type(&self) -> SinkType {
        SinkType::File
    }
}

