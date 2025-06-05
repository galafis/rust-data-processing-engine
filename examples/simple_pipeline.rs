// Simple pipeline example
// Author: Gabriel Demetrios Lafis

use rust_data_processing_engine::{
    data::{DataSet, DataType, Field, Row, Schema, Value},
    processing::{FilterProcessor, Pipeline, SelectTransform, AddColumnTransform},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a schema
    let schema = Schema::new(vec![
        Field::new("id".to_string(), DataType::Integer, false),
        Field::new("name".to_string(), DataType::String, false),
        Field::new("age".to_string(), DataType::Integer, true),
        Field::new("salary".to_string(), DataType::Float, true),
    ]);
    
    // Create a dataset
    let mut dataset = DataSet::new(schema);
    
    // Add rows
    dataset.add_row(Row::new(vec![
        Value::Integer(1),
        Value::String("Alice".to_string()),
        Value::Integer(30),
        Value::Float(75000.0),
    ]))?;
    
    dataset.add_row(Row::new(vec![
        Value::Integer(2),
        Value::String("Bob".to_string()),
        Value::Integer(25),
        Value::Float(65000.0),
    ]))?;
    
    dataset.add_row(Row::new(vec![
        Value::Integer(3),
        Value::String("Charlie".to_string()),
        Value::Integer(35),
        Value::Float(85000.0),
    ]))?;
    
    dataset.add_row(Row::new(vec![
        Value::Integer(4),
        Value::String("Diana".to_string()),
        Value::Integer(28),
        Value::Float(70000.0),
    ]))?;
    
    // Print original dataset
    println!("Original dataset:");
    print_dataset(&dataset);
    
    // Create a pipeline
    let pipeline = Pipeline::new("example")
        // Filter for age > 25
        .add(FilterProcessor::greater_than("age", Value::Integer(25)))
        // Add a bonus column
        .add(AddColumnTransform::with_constant(
            "bonus",
            DataType::Float,
            true,
            Value::Float(5000.0),
        ))
        // Select only certain columns
        .add(SelectTransform::new(vec![
            "name".to_string(),
            "age".to_string(),
            "salary".to_string(),
            "bonus".to_string(),
        ]));
    
    // Process the dataset
    let result = pipeline.process(&dataset)?;
    
    // Print result
    println!("\nProcessed dataset:");
    print_dataset(&result);
    
    Ok(())
}

// Helper function to print a dataset
fn print_dataset(dataset: &DataSet) {
    // Print header
    for (i, field) in dataset.schema.fields.iter().enumerate() {
        if i > 0 {
            print!(" | ");
        }
        print!("{}", field.name);
    }
    println!();
    
    // Print separator
    for i in 0..dataset.schema.fields.len() {
        if i > 0 {
            print!("-+-");
        }
        print!("----");
    }
    println!();
    
    // Print rows
    for row in &dataset.data {
        for (i, value) in row.values.iter().enumerate() {
            if i > 0 {
                print!(" | ");
            }
            match value {
                Value::Null => print!("NULL"),
                Value::Boolean(b) => print!("{}", b),
                Value::Integer(n) => print!("{}", n),
                Value::Float(f) => print!("{:.1}", f),
                Value::String(s) => print!("{}", s),
                Value::Binary(_) => print!("[binary]"),
                Value::Array(_) => print!("[array]"),
                Value::Map(_) => print!("[map]"),
            }
        }
        println!();
    }
}

