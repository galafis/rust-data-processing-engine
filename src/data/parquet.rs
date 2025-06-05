// Parquet data source and sink implementation
// Author: Gabriel Demetrios Lafis

use std::path::Path;
use std::sync::Arc;

use super::{DataError, DataSet, DataSink, DataSource, Field, Row, Schema, SinkType, SourceType, Value, DataType};

/// Parquet data source
pub struct ParquetSource {
    path: String,
}

impl ParquetSource {
    /// Create a new Parquet data source
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        ParquetSource {
            path: path.as_ref().to_string_lossy().to_string(),
        }
    }
    
    /// Convert Arrow data type to our data type
    #[cfg(feature = "parquet")]
    fn convert_arrow_type(arrow_type: &arrow::datatypes::DataType) -> DataType {
        use arrow::datatypes::DataType as ArrowType;
        
        match arrow_type {
            ArrowType::Boolean => DataType::Boolean,
            ArrowType::Int8 | ArrowType::Int16 | ArrowType::Int32 | ArrowType::Int64 |
            ArrowType::UInt8 | ArrowType::UInt16 | ArrowType::UInt32 | ArrowType::UInt64 => DataType::Integer,
            ArrowType::Float16 | ArrowType::Float32 | ArrowType::Float64 => DataType::Float,
            ArrowType::Utf8 | ArrowType::LargeUtf8 => DataType::String,
            ArrowType::Binary | ArrowType::LargeBinary => DataType::Binary,
            ArrowType::List(_) | ArrowType::LargeList(_) | ArrowType::FixedSizeList(_, _) => {
                DataType::Array(Box::new(DataType::String)) // Simplified
            },
            ArrowType::Struct(_) | ArrowType::Map(_, _) => {
                DataType::Map(Box::new(DataType::String)) // Simplified
            },
            _ => DataType::String, // Default for other types
        }
    }
}

impl DataSource for ParquetSource {
    fn read(&self) -> Result<DataSet, DataError> {
        #[cfg(feature = "parquet")]
        {
            use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
            use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
            use parquet::file::reader::SerializedFileReader;
            use std::fs::File;
            
            let file = File::open(&self.path).map_err(DataError::IoError)?;
            let file_reader = SerializedFileReader::new(file)
                .map_err(|e| DataError::ParseError(e.to_string()))?;
            
            let mut arrow_reader = ParquetRecordBatchReader::try_new(Arc::new(file_reader), 1024)
                .map_err(|e| DataError::ParseError(e.to_string()))?;
            
            // Get schema from the first batch
            let first_batch = arrow_reader.next()
                .ok_or_else(|| DataError::ParseError("Empty Parquet file".to_string()))?
                .map_err(|e| DataError::ParseError(e.to_string()))?;
            
            let arrow_schema = first_batch.schema();
            
            // Convert Arrow schema to our schema
            let fields: Vec<Field> = arrow_schema.fields().iter()
                .map(|field| {
                    Field::new(
                        field.name().clone(),
                        Self::convert_arrow_type(field.data_type()),
                        field.is_nullable(),
                    )
                })
                .collect();
            
            let schema = Schema::new(fields);
            let mut dataset = DataSet::new(schema);
            
            // Process the first batch
            let mut process_batch = |batch: &arrow::record_batch::RecordBatch| -> Result<(), DataError> {
                let num_rows = batch.num_rows();
                
                for row_idx in 0..num_rows {
                    let mut values = Vec::new();
                    
                    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                        let array = batch.column(col_idx);
                        
                        let value = match field.data_type() {
                            arrow::datatypes::DataType::Boolean => {
                                let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                                if array.is_null(row_idx) {
                                    Value::Null
                                } else {
                                    Value::Boolean(array.value(row_idx))
                                }
                            },
                            arrow::datatypes::DataType::Int8 | arrow::datatypes::DataType::Int16 |
                            arrow::datatypes::DataType::Int32 | arrow::datatypes::DataType::Int64 |
                            arrow::datatypes::DataType::UInt8 | arrow::datatypes::DataType::UInt16 |
                            arrow::datatypes::DataType::UInt32 | arrow::datatypes::DataType::UInt64 => {
                                let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                                if array.is_null(row_idx) {
                                    Value::Null
                                } else {
                                    Value::Integer(array.value(row_idx))
                                }
                            },
                            arrow::datatypes::DataType::Float32 | arrow::datatypes::DataType::Float64 => {
                                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                                if array.is_null(row_idx) {
                                    Value::Null
                                } else {
                                    Value::Float(array.value(row_idx))
                                }
                            },
                            arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8 => {
                                let array = array.as_any().downcast_ref::<StringArray>().unwrap();
                                if array.is_null(row_idx) {
                                    Value::Null
                                } else {
                                    Value::String(array.value(row_idx).to_string())
                                }
                            },
                            _ => Value::Null, // Simplified for other types
                        };
                        
                        values.push(value);
                    }
                    
                    let row = Row::new(values);
                    dataset.add_row(row)?;
                }
                
                Ok(())
            };
            
            process_batch(&first_batch)?;
            
            // Process remaining batches
            while let Some(batch_result) = arrow_reader.next() {
                let batch = batch_result.map_err(|e| DataError::ParseError(e.to_string()))?;
                process_batch(&batch)?;
            }
            
            // Add metadata
            dataset.metadata.add("source".to_string(), "parquet".to_string());
            dataset.metadata.add("path".to_string(), self.path.clone());
            
            Ok(dataset)
        }
        
        #[cfg(not(feature = "parquet"))]
        {
            Err(DataError::NotSupported("Parquet support not enabled".to_string()))
        }
    }
    
    fn name(&self) -> &str {
        &self.path
    }
    
    fn source_type(&self) -> SourceType {
        SourceType::File
    }
}

/// Parquet data sink
pub struct ParquetSink {
    path: String,
    compression: ParquetCompression,
}

/// Parquet compression options
#[derive(Debug, Clone, Copy)]
pub enum ParquetCompression {
    Uncompressed,
    Snappy,
    Gzip,
    Lzo,
    Brotli,
    Zstd,
}

impl ParquetSink {
    /// Create a new Parquet data sink
    pub fn new<P: AsRef<Path>>(path: P, compression: ParquetCompression) -> Self {
        ParquetSink {
            path: path.as_ref().to_string_lossy().to_string(),
            compression,
        }
    }
    
    /// Convert our data type to Arrow data type
    #[cfg(feature = "parquet")]
    fn convert_to_arrow_type(data_type: &DataType) -> arrow::datatypes::DataType {
        use arrow::datatypes::DataType as ArrowType;
        
        match data_type {
            DataType::Boolean => ArrowType::Boolean,
            DataType::Integer => ArrowType::Int64,
            DataType::Float => ArrowType::Float64,
            DataType::String => ArrowType::Utf8,
            DataType::Binary => ArrowType::Binary,
            DataType::Array(_) => {
                ArrowType::List(Arc::new(arrow::datatypes::Field::new(
                    "item",
                    ArrowType::Utf8,
                    true,
                )))
            },
            DataType::Map(_) => {
                ArrowType::Struct(vec![
                    arrow::datatypes::Field::new("key", ArrowType::Utf8, false),
                    arrow::datatypes::Field::new("value", ArrowType::Utf8, true),
                ])
            },
        }
    }
    
    /// Convert compression enum to parquet compression
    #[cfg(feature = "parquet")]
    fn get_compression(&self) -> parquet::basic::Compression {
        use parquet::basic::Compression;
        
        match self.compression {
            ParquetCompression::Uncompressed => Compression::UNCOMPRESSED,
            ParquetCompression::Snappy => Compression::SNAPPY,
            ParquetCompression::Gzip => Compression::GZIP,
            ParquetCompression::Lzo => Compression::LZO,
            ParquetCompression::Brotli => Compression::BROTLI,
            ParquetCompression::Zstd => Compression::ZSTD,
        }
    }
}

impl DataSink for ParquetSink {
    fn write(&self, data: &DataSet) -> Result<(), DataError> {
        #[cfg(feature = "parquet")]
        {
            use arrow::array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
            use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
            use arrow::record_batch::RecordBatch;
            use parquet::arrow::ArrowWriter;
            use std::fs::File;
            use std::sync::Arc;
            
            // Convert our schema to Arrow schema
            let arrow_fields: Vec<ArrowField> = data.schema.fields.iter()
                .map(|field| {
                    ArrowField::new(
                        &field.name,
                        Self::convert_to_arrow_type(&field.data_type),
                        field.nullable,
                    )
                })
                .collect();
            
            let arrow_schema = Arc::new(ArrowSchema::new(arrow_fields));
            
            // Create array builders for each column
            let mut builders: Vec<Box<dyn arrow::array::ArrayBuilder>> = data.schema.fields.iter()
                .map(|field| {
                    match field.data_type {
                        DataType::Boolean => Box::new(BooleanBuilder::new()) as Box<dyn arrow::array::ArrayBuilder>,
                        DataType::Integer => Box::new(Int64Builder::new()) as Box<dyn arrow::array::ArrayBuilder>,
                        DataType::Float => Box::new(Float64Builder::new()) as Box<dyn arrow::array::ArrayBuilder>,
                        DataType::String | DataType::Binary | DataType::Array(_) | DataType::Map(_) => {
                            Box::new(StringBuilder::new()) as Box<dyn arrow::array::ArrayBuilder>
                        },
                    }
                })
                .collect();
            
            // Fill builders with data
            for row in &data.data {
                for (i, value) in row.values.iter().enumerate() {
                    match (value, &data.schema.fields[i].data_type) {
                        (Value::Null, _) => {
                            match &data.schema.fields[i].data_type {
                                DataType::Boolean => {
                                    let builder = builders[i].as_any_mut().downcast_mut::<BooleanBuilder>().unwrap();
                                    builder.append_null();
                                },
                                DataType::Integer => {
                                    let builder = builders[i].as_any_mut().downcast_mut::<Int64Builder>().unwrap();
                                    builder.append_null();
                                },
                                DataType::Float => {
                                    let builder = builders[i].as_any_mut().downcast_mut::<Float64Builder>().unwrap();
                                    builder.append_null();
                                },
                                _ => {
                                    let builder = builders[i].as_any_mut().downcast_mut::<StringBuilder>().unwrap();
                                    builder.append_null();
                                },
                            }
                        },
                        (Value::Boolean(b), DataType::Boolean) => {
                            let builder = builders[i].as_any_mut().downcast_mut::<BooleanBuilder>().unwrap();
                            builder.append_value(*b);
                        },
                        (Value::Integer(n), DataType::Integer) => {
                            let builder = builders[i].as_any_mut().downcast_mut::<Int64Builder>().unwrap();
                            builder.append_value(*n);
                        },
                        (Value::Float(f), DataType::Float) => {
                            let builder = builders[i].as_any_mut().downcast_mut::<Float64Builder>().unwrap();
                            builder.append_value(*f);
                        },
                        (Value::String(s), DataType::String) => {
                            let builder = builders[i].as_any_mut().downcast_mut::<StringBuilder>().unwrap();
                            builder.append_value(s);
                        },
                        // Convert other types to string
                        (value, _) => {
                            let builder = builders[i].as_any_mut().downcast_mut::<StringBuilder>().unwrap();
                            let s = match value {
                                Value::Boolean(b) => b.to_string(),
                                Value::Integer(n) => n.to_string(),
                                Value::Float(f) => f.to_string(),
                                Value::String(s) => s.clone(),
                                Value::Binary(_) => "[binary data]".to_string(),
                                Value::Array(_) => "[array]".to_string(),
                                Value::Map(_) => "[map]".to_string(),
                                Value::Null => unreachable!(),
                            };
                            builder.append_value(&s);
                        },
                    }
                }
            }
            
            // Finish arrays
            let arrays: Vec<ArrayRef> = builders.iter_mut()
                .map(|builder| builder.finish())
                .collect();
            
            // Create record batch
            let batch = RecordBatch::try_new(arrow_schema.clone(), arrays)
                .map_err(|e| DataError::Other(e.to_string()))?;
            
            // Write to Parquet file
            let file = File::create(&self.path).map_err(DataError::IoError)?;
            
            let mut writer = ArrowWriter::try_new(
                file,
                arrow_schema,
                Some(parquet::file::properties::WriterProperties::builder()
                    .set_compression(self.get_compression())
                    .build()),
            ).map_err(|e| DataError::Other(e.to_string()))?;
            
            writer.write(&batch).map_err(|e| DataError::Other(e.to_string()))?;
            writer.close().map_err(|e| DataError::Other(e.to_string()))?;
            
            Ok(())
        }
        
        #[cfg(not(feature = "parquet"))]
        {
            Err(DataError::NotSupported("Parquet support not enabled".to_string()))
        }
    }
    
    fn name(&self) -> &str {
        &self.path
    }
    
    fn sink_type(&self) -> SinkType {
        SinkType::File
    }
}

