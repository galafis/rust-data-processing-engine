// Pipeline tests
// Author: Gabriel Demetrios Lafis

use rust_data_processing_engine::{
    data::{DataSet, DataType, Field, Row, Schema, Value},
    processing::{
        FilterProcessor, Pipeline, SelectTransform, AddColumnTransform,
        GroupByProcessor, JoinProcessor, JoinType,
    },
};

#[test]
fn test_filter_pipeline() {
    // Create a schema
    let schema = Schema::new(vec![
        Field::new("id".to_string(), DataType::Integer, false),
        Field::new("name".to_string(), DataType::String, false),
        Field::new("age".to_string(), DataType::Integer, true),
    ]);
    
    // Create a dataset
    let mut dataset = DataSet::new(schema);
    
    // Add rows
    dataset.add_row(Row::new(vec![
        Value::Integer(1),
        Value::String("Alice".to_string()),
        Value::Integer(30),
    ])).unwrap();
    
    dataset.add_row(Row::new(vec![
        Value::Integer(2),
        Value::String("Bob".to_string()),
        Value::Integer(25),
    ])).unwrap();
    
    dataset.add_row(Row::new(vec![
        Value::Integer(3),
        Value::String("Charlie".to_string()),
        Value::Integer(35),
    ])).unwrap();
    
    // Create a pipeline
    let pipeline = Pipeline::new("test")
        .add(FilterProcessor::greater_than("age", Value::Integer(28)));
    
    // Process the dataset
    let result = pipeline.process(&dataset).unwrap();
    
    // Check result
    assert_eq!(result.len(), 2);
    assert_eq!(result.data[0].values[1], Value::String("Alice".to_string()));
    assert_eq!(result.data[1].values[1], Value::String("Charlie".to_string()));
}

#[test]
fn test_transform_pipeline() {
    // Create a schema
    let schema = Schema::new(vec![
        Field::new("id".to_string(), DataType::Integer, false),
        Field::new("name".to_string(), DataType::String, false),
        Field::new("age".to_string(), DataType::Integer, true),
    ]);
    
    // Create a dataset
    let mut dataset = DataSet::new(schema);
    
    // Add rows
    dataset.add_row(Row::new(vec![
        Value::Integer(1),
        Value::String("Alice".to_string()),
        Value::Integer(30),
    ])).unwrap();
    
    dataset.add_row(Row::new(vec![
        Value::Integer(2),
        Value::String("Bob".to_string()),
        Value::Integer(25),
    ])).unwrap();
    
    // Create a pipeline
    let pipeline = Pipeline::new("test")
        .add(SelectTransform::new(vec!["name".to_string(), "age".to_string()]))
        .add(AddColumnTransform::with_constant(
            "adult",
            DataType::Boolean,
            false,
            Value::Boolean(true),
        ));
    
    // Process the dataset
    let result = pipeline.process(&dataset).unwrap();
    
    // Check result
    assert_eq!(result.len(), 2);
    assert_eq!(result.schema.fields.len(), 3);
    assert_eq!(result.schema.fields[0].name, "name");
    assert_eq!(result.schema.fields[1].name, "age");
    assert_eq!(result.schema.fields[2].name, "adult");
    assert_eq!(result.data[0].values[2], Value::Boolean(true));
    assert_eq!(result.data[1].values[2], Value::Boolean(true));
}

#[test]
fn test_aggregate_pipeline() {
    // Create a schema
    let schema = Schema::new(vec![
        Field::new("category".to_string(), DataType::String, false),
        Field::new("amount".to_string(), DataType::Float, false),
    ]);
    
    // Create a dataset
    let mut dataset = DataSet::new(schema);
    
    // Add rows
    dataset.add_row(Row::new(vec![
        Value::String("A".to_string()),
        Value::Float(100.0),
    ])).unwrap();
    
    dataset.add_row(Row::new(vec![
        Value::String("B".to_string()),
        Value::Float(200.0),
    ])).unwrap();
    
    dataset.add_row(Row::new(vec![
        Value::String("A".to_string()),
        Value::Float(150.0),
    ])).unwrap();
    
    dataset.add_row(Row::new(vec![
        Value::String("B".to_string()),
        Value::Float(250.0),
    ])).unwrap();
    
    // Create a group by processor
    let group_by = GroupByProcessor::new()
        .group_by("category")
        .sum("total", "amount");
    
    // Process the dataset
    let result = group_by.process(&dataset).unwrap();
    
    // Check result
    assert_eq!(result.len(), 2);
    
    // Find category A
    let mut found_a = false;
    let mut found_b = false;
    
    for row in &result.data {
        if row.values[0] == Value::String("A".to_string()) {
            found_a = true;
            assert_eq!(row.values[1], Value::Float(250.0));
        } else if row.values[0] == Value::String("B".to_string()) {
            found_b = true;
            assert_eq!(row.values[1], Value::Float(450.0));
        }
    }
    
    assert!(found_a);
    assert!(found_b);
}

#[test]
fn test_join_pipeline() {
    // Create schemas
    let schema1 = Schema::new(vec![
        Field::new("id".to_string(), DataType::Integer, false),
        Field::new("name".to_string(), DataType::String, false),
    ]);
    
    let schema2 = Schema::new(vec![
        Field::new("id".to_string(), DataType::Integer, false),
        Field::new("age".to_string(), DataType::Integer, true),
    ]);
    
    // Create datasets
    let mut dataset1 = DataSet::new(schema1);
    let mut dataset2 = DataSet::new(schema2);
    
    // Add rows to dataset1
    dataset1.add_row(Row::new(vec![
        Value::Integer(1),
        Value::String("Alice".to_string()),
    ])).unwrap();
    
    dataset1.add_row(Row::new(vec![
        Value::Integer(2),
        Value::String("Bob".to_string()),
    ])).unwrap();
    
    // Add rows to dataset2
    dataset2.add_row(Row::new(vec![
        Value::Integer(1),
        Value::Integer(30),
    ])).unwrap();
    
    dataset2.add_row(Row::new(vec![
        Value::Integer(3),
        Value::Integer(25),
    ])).unwrap();
    
    // Create join processor
    let join = JoinProcessor::new(
        JoinType::Left,
        vec!["id".to_string()],
        vec!["id".to_string()],
    );
    
    // Process the datasets
    let result = join.process_join(&dataset1, &dataset2).unwrap();
    
    // Check result
    assert_eq!(result.len(), 2);
    assert_eq!(result.schema.fields.len(), 3);
    
    // First row should have a match
    assert_eq!(result.data[0].values[0], Value::Integer(1));
    assert_eq!(result.data[0].values[1], Value::String("Alice".to_string()));
    assert_eq!(result.data[0].values[2], Value::Integer(30));
    
    // Second row should have no match (left join)
    assert_eq!(result.data[1].values[0], Value::Integer(2));
    assert_eq!(result.data[1].values[1], Value::String("Bob".to_string()));
    assert_eq!(result.data[1].values[2], Value::Null);
}

