// JSON data source and sink implementation
// Author: Gabriel Demetrios Lafis

use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::collections::HashMap;

use serde_json::{Value as JsonValue, Map};

use super::{DataError, DataSet, DataSink, DataSource, Field, Row, Schema, SinkType, SourceType, Value, DataType};

/// JSON data source
pub struct JsonSource {
    path: String,
    array_path: Option<String>,
}

impl JsonSource {
    /// Create a new JSON data source
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        JsonSource {
            path: path.as_ref().to_string_lossy().to_string(),
            array_path: None,
        }
    }
    
    /// Create a new JSON data source with a path to the array
    pub fn with_array_path<P: AsRef<Path>, S: Into<String>>(path: P, array_path: S) -> Self {
        JsonSource {
            path: path.as_ref().to_string_lossy().to_string(),
            array_path: Some(array_path.into()),
        }
    }
    
    /// Convert a JSON value to a data value
    fn json_to_value(json: &JsonValue) -> Value {
        match json {
            JsonValue::Null => Value::Null,
            JsonValue::Bool(b) => Value::Boolean(*b),
            JsonValue::Number(n) => {
                if n.is_i64() {
                    Value::Integer(n.as_i64().unwrap())
                } else {
                    Value::Float(n.as_f64().unwrap())
                }
            },
            JsonValue::String(s) => Value::String(s.clone()),
            JsonValue::Array(arr) => {
                let values: Vec<Value> = arr.iter()
                    .map(|v| Self::json_to_value(v))
                    .collect();
                Value::Array(values)
            },
            JsonValue::Object(obj) => {
                let mut map = HashMap::new();
                for (k, v) in obj {
                    map.insert(k.clone(), Self::json_to_value(v));
                }
                Value::Map(map)
            },
        }
    }
    
    /// Infer schema from a JSON object
    fn infer_schema(obj: &Map<String, JsonValue>) -> Schema {
        let fields: Vec<Field> = obj.iter()
            .map(|(key, value)| {
                let data_type = match value {
                    JsonValue::Null => DataType::String, // Default to string for null values
                    JsonValue::Bool(_) => DataType::Boolean,
                    JsonValue::Number(n) => {
                        if n.is_i64() {
                            DataType::Integer
                        } else {
                            DataType::Float
                        }
                    },
                    JsonValue::String(_) => DataType::String,
                    JsonValue::Array(_) => DataType::Array(Box::new(DataType::String)), // Simplified
                    JsonValue::Object(_) => DataType::Map(Box::new(DataType::String)), // Simplified
                };
                
                Field::new(key.clone(), data_type, true)
            })
            .collect();
        
        Schema::new(fields)
    }
}

impl DataSource for JsonSource {
    fn read(&self) -> Result<DataSet, DataError> {
        let file = File::open(&self.path).map_err(DataError::IoError)?;
        let reader = BufReader::new(file);
        
        let json: JsonValue = serde_json::from_reader(reader)
            .map_err(|e| DataError::ParseError(e.to_string()))?;
        
        // Get the array of objects
        let array = if let Some(ref array_path) = self.array_path {
            let parts: Vec<&str> = array_path.split('.').collect();
            let mut current = &json;
            
            for part in parts {
                current = current.get(part)
                    .ok_or_else(|| DataError::ParseError(format!("Path '{}' not found in JSON", array_path)))?;
            }
            
            current.as_array()
                .ok_or_else(|| DataError::ParseError(format!("Path '{}' is not an array", array_path)))?
        } else if json.is_array() {
            json.as_array().unwrap()
        } else {
            return Err(DataError::ParseError("JSON root is not an array and no array path provided".to_string()));
        };
        
        if array.is_empty() {
            return Err(DataError::ParseError("Empty JSON array".to_string()));
        }
        
        // Infer schema from the first object
        let first_obj = array[0].as_object()
            .ok_or_else(|| DataError::ParseError("Array element is not an object".to_string()))?;
        
        let schema = Self::infer_schema(first_obj);
        let mut dataset = DataSet::new(schema);
        
        // Process all objects
        for item in array {
            let obj = item.as_object()
                .ok_or_else(|| DataError::ParseError("Array element is not an object".to_string()))?;
            
            let mut values = Vec::new();
            
            for field in &dataset.schema.fields {
                let value = obj.get(&field.name)
                    .map_or(Value::Null, |v| Self::json_to_value(v));
                values.push(value);
            }
            
            let row = Row::new(values);
            dataset.add_row(row)?;
        }
        
        // Add metadata
        dataset.metadata.add("source".to_string(), "json".to_string());
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

/// JSON data sink
pub struct JsonSink {
    path: String,
    pretty: bool,
}

impl JsonSink {
    /// Create a new JSON data sink
    pub fn new<P: AsRef<Path>>(path: P, pretty: bool) -> Self {
        JsonSink {
            path: path.as_ref().to_string_lossy().to_string(),
            pretty,
        }
    }
    
    /// Convert a data value to a JSON value
    fn value_to_json(value: &Value) -> JsonValue {
        match value {
            Value::Null => JsonValue::Null,
            Value::Boolean(b) => JsonValue::Bool(*b),
            Value::Integer(i) => JsonValue::Number((*i).into()),
            Value::Float(f) => {
                let n = serde_json::Number::from_f64(*f);
                match n {
                    Some(num) => JsonValue::Number(num),
                    None => JsonValue::Null,
                }
            },
            Value::String(s) => JsonValue::String(s.clone()),
            Value::Binary(b) => {
                // Convert binary to base64 string
                let base64 = base64::encode(b);
                JsonValue::String(base64)
            },
            Value::Array(arr) => {
                let values: Vec<JsonValue> = arr.iter()
                    .map(|v| Self::value_to_json(v))
                    .collect();
                JsonValue::Array(values)
            },
            Value::Map(map) => {
                let mut obj = Map::new();
                for (k, v) in map {
                    obj.insert(k.clone(), Self::value_to_json(v));
                }
                JsonValue::Object(obj)
            },
        }
    }
}

impl DataSink for JsonSink {
    fn write(&self, data: &DataSet) -> Result<(), DataError> {
        let file = File::create(&self.path).map_err(DataError::IoError)?;
        let writer = BufWriter::new(file);
        
        let mut array = Vec::new();
        
        for row in &data.data {
            let mut obj = Map::new();
            
            for (i, field) in data.schema.fields.iter().enumerate() {
                let value = row.values.get(i).unwrap_or(&Value::Null);
                obj.insert(field.name.clone(), Self::value_to_json(value));
            }
            
            array.push(JsonValue::Object(obj));
        }
        
        let json = JsonValue::Array(array);
        
        if self.pretty {
            serde_json::to_writer_pretty(writer, &json)
                .map_err(|e| DataError::IoError(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
        } else {
            serde_json::to_writer(writer, &json)
                .map_err(|e| DataError::IoError(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
        }
        
        Ok(())
    }
    
    fn name(&self) -> &str {
        &self.path
    }
    
    fn sink_type(&self) -> SinkType {
        SinkType::File
    }
}

