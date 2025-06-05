// Schema definition and validation
// Author: Gabriel Demetrios Lafis

use super::{DataError, DataType, Field, Schema, Value};

/// Schema validator for ensuring data conforms to a schema
pub struct SchemaValidator;

impl SchemaValidator {
    /// Validate a value against a data type
    pub fn validate_value(value: &Value, data_type: &DataType) -> Result<(), DataError> {
        match (value, data_type) {
            (Value::Null, _) => Ok(()), // Null is valid for any type
            (Value::Boolean(_), DataType::Boolean) => Ok(()),
            (Value::Integer(_), DataType::Integer) => Ok(()),
            (Value::Float(_), DataType::Float) => Ok(()),
            (Value::String(_), DataType::String) => Ok(()),
            (Value::Binary(_), DataType::Binary) => Ok(()),
            (Value::Array(arr), DataType::Array(elem_type)) => {
                // Validate each element in the array
                for elem in arr {
                    Self::validate_value(elem, elem_type)?;
                }
                Ok(())
            },
            (Value::Map(map), DataType::Map(val_type)) => {
                // Validate each value in the map
                for (_, val) in map {
                    Self::validate_value(val, val_type)?;
                }
                Ok(())
            },
            _ => Err(DataError::ValidationError(format!(
                "Value type mismatch: expected {:?}", data_type
            ))),
        }
    }
    
    /// Validate a row against a schema
    pub fn validate_row(row: &super::Row, schema: &Schema) -> Result<(), DataError> {
        // Check if row has the correct number of fields
        if row.values.len() != schema.fields.len() {
            return Err(DataError::ValidationError(format!(
                "Row has {} fields, schema has {} fields",
                row.values.len(),
                schema.fields.len()
            )));
        }
        
        // Validate each field
        for (i, field) in schema.fields.iter().enumerate() {
            let value = &row.values[i];
            
            // Check if null is allowed
            if !field.nullable && matches!(value, Value::Null) {
                return Err(DataError::ValidationError(format!(
                    "Field '{}' cannot be null", field.name
                )));
            }
            
            // Validate type
            Self::validate_value(value, &field.data_type)?;
        }
        
        Ok(())
    }
}

/// Schema builder for creating schemas
pub struct SchemaBuilder {
    fields: Vec<Field>,
}

impl SchemaBuilder {
    /// Create a new schema builder
    pub fn new() -> Self {
        SchemaBuilder {
            fields: Vec::new(),
        }
    }
    
    /// Add a field to the schema
    pub fn add_field(mut self, name: &str, data_type: DataType, nullable: bool) -> Self {
        self.fields.push(Field::new(name.to_string(), data_type, nullable));
        self
    }
    
    /// Add a boolean field
    pub fn add_boolean(self, name: &str, nullable: bool) -> Self {
        self.add_field(name, DataType::Boolean, nullable)
    }
    
    /// Add an integer field
    pub fn add_integer(self, name: &str, nullable: bool) -> Self {
        self.add_field(name, DataType::Integer, nullable)
    }
    
    /// Add a float field
    pub fn add_float(self, name: &str, nullable: bool) -> Self {
        self.add_field(name, DataType::Float, nullable)
    }
    
    /// Add a string field
    pub fn add_string(self, name: &str, nullable: bool) -> Self {
        self.add_field(name, DataType::String, nullable)
    }
    
    /// Add a binary field
    pub fn add_binary(self, name: &str, nullable: bool) -> Self {
        self.add_field(name, DataType::Binary, nullable)
    }
    
    /// Add an array field
    pub fn add_array(self, name: &str, element_type: DataType, nullable: bool) -> Self {
        self.add_field(name, DataType::Array(Box::new(element_type)), nullable)
    }
    
    /// Add a map field
    pub fn add_map(self, name: &str, value_type: DataType, nullable: bool) -> Self {
        self.add_field(name, DataType::Map(Box::new(value_type)), nullable)
    }
    
    /// Build the schema
    pub fn build(self) -> Schema {
        Schema::new(self.fields)
    }
}

impl Default for SchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

