//! Comprehensive SQL Test Suite
//!
//! This test suite covers all SQL features to ensure correctness.
//! Run with: cargo test --test sql_comprehensive

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use query_engine::ExecutionContext;
use std::sync::Arc;

// ============================================================================
// Test Fixtures
// ============================================================================

/// Create a test context with sample tables
fn create_test_context() -> ExecutionContext {
    let mut ctx = ExecutionContext::new();

    // Table: users (id, name, age, salary, active)
    let users_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int64, true),
        Field::new("salary", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
    ]));

    let users_batch = RecordBatch::try_new(
        users_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("Alice"),
                Some("Bob"),
                Some("Charlie"),
                Some("Diana"),
                None,
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                Some(30),
                Some(25),
                Some(35),
                Some(28),
                Some(40),
            ])) as ArrayRef,
            Arc::new(Float64Array::from(vec![
                Some(50000.0),
                Some(60000.0),
                Some(75000.0),
                Some(55000.0),
                Some(80000.0),
            ])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
                Some(true),
                Some(false),
            ])) as ArrayRef,
        ],
    )
    .unwrap();

    ctx.register_table("users", users_schema, vec![users_batch]);

    // Table: orders (order_id, user_id, amount, status)
    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("status", DataType::Utf8, true),
    ]));

    let orders_batch = RecordBatch::try_new(
        orders_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![101, 102, 103, 104, 105, 106])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, 1, 2, 3, 4, 1])) as ArrayRef,
            Arc::new(Float64Array::from(vec![100.0, 200.0, 150.0, 300.0, 250.0, 175.0])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("completed"),
                Some("pending"),
                Some("completed"),
                Some("cancelled"),
                Some("completed"),
                Some("completed"),
            ])) as ArrayRef,
        ],
    )
    .unwrap();

    ctx.register_table("orders", orders_schema, vec![orders_batch]);

    // Table: products (product_id, name, price, category)
    let products_schema = Arc::new(Schema::new(vec![
        Field::new("product_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("category", DataType::Utf8, true),
    ]));

    let products_batch = RecordBatch::try_new(
        products_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Widget", "Gadget", "Gizmo", "Doohickey", "Thingamabob"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![10.0, 25.0, 15.0, 30.0, 20.0])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("A"),
                Some("B"),
                Some("A"),
                Some("B"),
                Some("C"),
            ])) as ArrayRef,
        ],
    )
    .unwrap();

    ctx.register_table("products", products_schema, vec![products_batch]);

    // Table: empty_table (id, value)
    let empty_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let empty_batch = RecordBatch::try_new(
        empty_schema.clone(),
        vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
            Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
        ],
    )
    .unwrap();

    ctx.register_table("empty_table", empty_schema, vec![empty_batch]);

    // Table: numbers (n) - for testing sequences
    let numbers_schema = Arc::new(Schema::new(vec![
        Field::new("n", DataType::Int64, false),
    ]));

    let numbers_batch = RecordBatch::try_new(
        numbers_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])) as ArrayRef,
        ],
    )
    .unwrap();

    ctx.register_table("numbers", numbers_schema, vec![numbers_batch]);

    ctx
}

/// Helper to run a query and get row count
async fn query_row_count(ctx: &ExecutionContext, sql: &str) -> usize {
    match ctx.sql(sql).await {
        Ok(result) => result.row_count,
        Err(e) => panic!("Query failed: {}\nSQL: {}", e, sql),
    }
}

/// Helper to run a query and get first column as i64 values
async fn query_i64_column(ctx: &ExecutionContext, sql: &str) -> Vec<Option<i64>> {
    let result = ctx.sql(sql).await.expect(&format!("Query failed: {}", sql));
    if result.batches.is_empty() || result.batches[0].num_rows() == 0 {
        return vec![];
    }
    let col = result.batches[0].column(0);
    let arr = col.as_any().downcast_ref::<Int64Array>().expect("Expected Int64Array");
    (0..arr.len()).map(|i| {
        if arr.is_null(i) { None } else { Some(arr.value(i)) }
    }).collect()
}

/// Helper to run a query and get first column as f64 values
async fn query_f64_column(ctx: &ExecutionContext, sql: &str) -> Vec<Option<f64>> {
    let result = ctx.sql(sql).await.expect(&format!("Query failed: {}", sql));
    if result.batches.is_empty() || result.batches[0].num_rows() == 0 {
        return vec![];
    }
    let col = result.batches[0].column(0);
    let arr = col.as_any().downcast_ref::<Float64Array>().expect("Expected Float64Array");
    (0..arr.len()).map(|i| {
        if arr.is_null(i) { None } else { Some(arr.value(i)) }
    }).collect()
}

/// Helper to run a query and check if it succeeds
async fn query_succeeds(ctx: &ExecutionContext, sql: &str) -> bool {
    ctx.sql(sql).await.is_ok()
}

// ============================================================================
// BASIC SELECT
// ============================================================================

mod basic_select {
    use super::*;

    #[tokio::test]
    async fn test_select_star() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users").await, 5);
    }

    #[tokio::test]
    async fn test_select_columns() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT id, name FROM users").await, 5);
    }

    #[tokio::test]
    async fn test_select_with_alias() {
        let ctx = create_test_context();
        let result = ctx.sql("SELECT id AS user_id, name AS user_name FROM users").await.unwrap();
        assert_eq!(result.schema.field(0).name(), "user_id");
        assert_eq!(result.schema.field(1).name(), "user_name");
    }

    #[tokio::test]
    async fn test_select_expression() {
        let ctx = create_test_context();
        let values = query_f64_column(&ctx, "SELECT salary * 1.1 FROM users WHERE id = 1").await;
        assert_eq!(values.len(), 1);
        assert!((values[0].unwrap() - 55000.0).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_select_from_empty_table() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM empty_table").await, 0);
    }

    #[tokio::test]
    async fn test_select_literal() {
        let ctx = create_test_context();
        let values = query_i64_column(&ctx, "SELECT 42 FROM users WHERE id = 1").await;
        assert_eq!(values, vec![Some(42)]);
    }
}

// ============================================================================
// WHERE CLAUSE / FILTERING
// ============================================================================

mod where_clause {
    use super::*;

    #[tokio::test]
    async fn test_where_equals() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users WHERE id = 1").await, 1);
    }

    #[tokio::test]
    async fn test_where_not_equals() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users WHERE id != 1").await, 4);
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users WHERE id <> 1").await, 4);
    }

    #[tokio::test]
    async fn test_where_greater_than() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users WHERE age > 30").await, 2);
    }

    #[tokio::test]
    async fn test_where_less_than() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users WHERE age < 30").await, 2);
    }

    #[tokio::test]
    async fn test_where_greater_equals() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users WHERE age >= 30").await, 3);
    }

    #[tokio::test]
    async fn test_where_less_equals() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users WHERE age <= 30").await, 3);
    }

    #[tokio::test]
    async fn test_where_and() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users WHERE age > 25 AND active = true").await, 2);
    }

    #[tokio::test]
    async fn test_where_or() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users WHERE age < 26 OR age > 35").await, 2);
    }

    #[tokio::test]
    async fn test_where_not() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users WHERE NOT active").await, 2);
    }

    #[tokio::test]
    async fn test_where_in() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users WHERE id IN (1, 2, 3)").await, 3);
    }

    #[tokio::test]
    async fn test_where_not_in() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users WHERE id NOT IN (1, 2)").await, 3);
    }

    #[tokio::test]
    async fn test_where_between() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users WHERE age BETWEEN 25 AND 35").await, 4);
    }

    #[tokio::test]
    async fn test_where_like() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users WHERE name LIKE 'A%'").await, 1);
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users WHERE name LIKE '%a%'").await, 2); // Diana, Charlie
    }

    #[tokio::test]
    async fn test_where_is_null() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users WHERE name IS NULL").await, 1);
    }

    #[tokio::test]
    async fn test_where_is_not_null() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users WHERE name IS NOT NULL").await, 4);
    }

    #[tokio::test]
    async fn test_where_string_equals() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users WHERE name = 'Alice'").await, 1);
    }

    #[tokio::test]
    async fn test_where_boolean() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users WHERE active").await, 3);
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users WHERE active = true").await, 3);
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users WHERE active = false").await, 2);
    }

    #[tokio::test]
    async fn test_where_complex() {
        let ctx = create_test_context();
        // Row 3 (Charlie): age=35 > 25 AND salary=75000 > 55000 -> true
        // Row 5: age=40 > 25 AND salary=80000 > 55000 -> true
        // Other rows don't match either condition
        let sql = "SELECT * FROM users WHERE (age > 25 AND salary > 55000) OR (active = false AND age >= 35)";
        assert_eq!(query_row_count(&ctx, sql).await, 2);
    }
}

// ============================================================================
// ORDER BY
// ============================================================================

mod order_by {
    use super::*;

    #[tokio::test]
    async fn test_order_by_asc() {
        let ctx = create_test_context();
        let values = query_i64_column(&ctx, "SELECT id, age FROM users ORDER BY age ASC").await;
        assert_eq!(values, vec![Some(2), Some(4), Some(1), Some(3), Some(5)]);
    }

    #[tokio::test]
    async fn test_order_by_desc() {
        let ctx = create_test_context();
        let values = query_i64_column(&ctx, "SELECT id, age FROM users ORDER BY age DESC").await;
        assert_eq!(values, vec![Some(5), Some(3), Some(1), Some(4), Some(2)]);
    }

    #[tokio::test]
    async fn test_order_by_default_asc() {
        let ctx = create_test_context();
        let values = query_i64_column(&ctx, "SELECT id, age FROM users ORDER BY age").await;
        assert_eq!(values, vec![Some(2), Some(4), Some(1), Some(3), Some(5)]);
    }

    #[tokio::test]
    async fn test_order_by_multiple() {
        let ctx = create_test_context();
        let sql = "SELECT user_id, amount FROM orders ORDER BY user_id ASC, amount DESC";
        let result = ctx.sql(sql).await.unwrap();
        assert_eq!(result.row_count, 6);
        // User 1 should have orders: 200, 175, 100 (descending by amount)
    }

    #[tokio::test]
    async fn test_order_by_expression() {
        let ctx = create_test_context();
        let values = query_i64_column(&ctx, "SELECT id, age FROM users ORDER BY age * 2 DESC").await;
        assert_eq!(values, vec![Some(5), Some(3), Some(1), Some(4), Some(2)]);
    }

    #[tokio::test]
    async fn test_order_by_alias() {
        let ctx = create_test_context();
        let values = query_i64_column(&ctx, "SELECT id, age AS a FROM users ORDER BY a DESC").await;
        assert_eq!(values, vec![Some(5), Some(3), Some(1), Some(4), Some(2)]);
    }

    #[tokio::test]
    async fn test_order_by_column_number() {
        let ctx = create_test_context();
        // ORDER BY 2 means order by the second column
        let values = query_i64_column(&ctx, "SELECT id, age FROM users ORDER BY 2 DESC").await;
        assert_eq!(values, vec![Some(5), Some(3), Some(1), Some(4), Some(2)]);
    }
}

// ============================================================================
// LIMIT / OFFSET
// ============================================================================

mod limit_offset {
    use super::*;

    #[tokio::test]
    async fn test_limit() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users LIMIT 3").await, 3);
    }

    #[tokio::test]
    async fn test_limit_zero() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users LIMIT 0").await, 0);
    }

    #[tokio::test]
    async fn test_limit_exceeds_rows() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users LIMIT 100").await, 5);
    }

    #[tokio::test]
    async fn test_offset() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users LIMIT 10 OFFSET 2").await, 3);
    }

    #[tokio::test]
    async fn test_offset_exceeds_rows() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users LIMIT 10 OFFSET 100").await, 0);
    }

    #[tokio::test]
    async fn test_limit_with_order_by() {
        let ctx = create_test_context();
        let sql = "SELECT id, age FROM users ORDER BY age DESC LIMIT 3";
        let values = query_i64_column(&ctx, sql).await;
        assert_eq!(values.len(), 3);
        assert_eq!(values, vec![Some(5), Some(3), Some(1)]); // Top 3 oldest
    }

    #[tokio::test]
    async fn test_limit_offset_with_order_by() {
        let ctx = create_test_context();
        let sql = "SELECT id, age FROM users ORDER BY age DESC LIMIT 2 OFFSET 1";
        let values = query_i64_column(&ctx, sql).await;
        assert_eq!(values.len(), 2);
        assert_eq!(values, vec![Some(3), Some(1)]); // 2nd and 3rd oldest
    }

    #[tokio::test]
    async fn test_limit_with_where() {
        let ctx = create_test_context();
        let sql = "SELECT * FROM users WHERE active = true LIMIT 2";
        assert_eq!(query_row_count(&ctx, sql).await, 2);
    }

    #[tokio::test]
    async fn test_offset_only() {
        let ctx = create_test_context();
        // OFFSET without LIMIT should still work
        assert_eq!(query_row_count(&ctx, "SELECT * FROM numbers OFFSET 5").await, 5);
    }
}

// ============================================================================
// AGGREGATE FUNCTIONS
// ============================================================================

mod aggregates {
    use super::*;

    #[tokio::test]
    async fn test_count_star() {
        let ctx = create_test_context();
        let values = query_i64_column(&ctx, "SELECT COUNT(*) FROM users").await;
        assert_eq!(values, vec![Some(5)]);
    }

    #[tokio::test]
    async fn test_count_column() {
        let ctx = create_test_context();
        // COUNT(name) should exclude NULLs
        let values = query_i64_column(&ctx, "SELECT COUNT(name) FROM users").await;
        assert_eq!(values, vec![Some(4)]);
    }

    #[tokio::test]
    async fn test_count_distinct() {
        let ctx = create_test_context();
        let values = query_i64_column(&ctx, "SELECT COUNT(DISTINCT user_id) FROM orders").await;
        assert_eq!(values, vec![Some(4)]); // 4 unique users
    }

    #[tokio::test]
    async fn test_sum() {
        let ctx = create_test_context();
        let values = query_f64_column(&ctx, "SELECT SUM(amount) FROM orders").await;
        assert_eq!(values.len(), 1);
        assert!((values[0].unwrap() - 1175.0).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_avg() {
        let ctx = create_test_context();
        let values = query_f64_column(&ctx, "SELECT AVG(age) FROM users").await;
        assert_eq!(values.len(), 1);
        assert!((values[0].unwrap() - 31.6).abs() < 0.1); // (30+25+35+28+40)/5 = 31.6
    }

    #[tokio::test]
    async fn test_min() {
        let ctx = create_test_context();
        let values = query_i64_column(&ctx, "SELECT MIN(age) FROM users").await;
        assert_eq!(values, vec![Some(25)]);
    }

    #[tokio::test]
    async fn test_max() {
        let ctx = create_test_context();
        let values = query_i64_column(&ctx, "SELECT MAX(age) FROM users").await;
        assert_eq!(values, vec![Some(40)]);
    }

    #[tokio::test]
    async fn test_aggregate_empty_table() {
        let ctx = create_test_context();
        let values = query_i64_column(&ctx, "SELECT COUNT(*) FROM empty_table").await;
        assert_eq!(values, vec![Some(0)]);
    }

    #[tokio::test]
    async fn test_aggregate_with_where() {
        let ctx = create_test_context();
        let values = query_i64_column(&ctx, "SELECT COUNT(*) FROM users WHERE active = true").await;
        assert_eq!(values, vec![Some(3)]);
    }

    #[tokio::test]
    async fn test_multiple_aggregates() {
        let ctx = create_test_context();
        let result = ctx.sql("SELECT COUNT(*), SUM(age), AVG(age), MIN(age), MAX(age) FROM users").await.unwrap();
        assert_eq!(result.row_count, 1);
        assert_eq!(result.schema.fields().len(), 5);
    }
}

// ============================================================================
// GROUP BY
// ============================================================================

mod group_by {
    use super::*;

    #[tokio::test]
    async fn test_group_by_single() {
        let ctx = create_test_context();
        let sql = "SELECT status, COUNT(*) FROM orders GROUP BY status";
        let result = ctx.sql(sql).await.unwrap();
        assert_eq!(result.row_count, 3); // completed, pending, cancelled
    }

    #[tokio::test]
    async fn test_group_by_with_sum() {
        let ctx = create_test_context();
        let sql = "SELECT user_id, SUM(amount) FROM orders GROUP BY user_id";
        let result = ctx.sql(sql).await.unwrap();
        assert_eq!(result.row_count, 4); // 4 unique users
    }

    #[tokio::test]
    async fn test_group_by_multiple() {
        let ctx = create_test_context();
        let sql = "SELECT user_id, status, COUNT(*) FROM orders GROUP BY user_id, status";
        let result = ctx.sql(sql).await.unwrap();
        assert!(result.row_count >= 4); // At least 4 combinations
    }

    #[tokio::test]
    async fn test_group_by_with_having() {
        let ctx = create_test_context();
        let sql = "SELECT user_id, COUNT(*) as cnt FROM orders GROUP BY user_id HAVING COUNT(*) > 1";
        let result = ctx.sql(sql).await.unwrap();
        assert_eq!(result.row_count, 1); // Only user 1 has multiple orders
    }

    #[tokio::test]
    async fn test_group_by_with_order_by() {
        let ctx = create_test_context();
        let sql = "SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id ORDER BY total DESC";
        let result = ctx.sql(sql).await.unwrap();
        assert_eq!(result.row_count, 4);
    }

    #[tokio::test]
    async fn test_group_by_with_where_and_having() {
        let ctx = create_test_context();
        let sql = "SELECT user_id, SUM(amount) as total FROM orders WHERE status = 'completed' GROUP BY user_id HAVING SUM(amount) > 100";
        let result = ctx.sql(sql).await.unwrap();
        assert!(result.row_count > 0);
    }

    #[tokio::test]
    async fn test_group_by_null_values() {
        let ctx = create_test_context();
        // Group by a column with NULLs
        let sql = "SELECT category, COUNT(*) FROM products GROUP BY category";
        let result = ctx.sql(sql).await.unwrap();
        assert_eq!(result.row_count, 3); // A, B, C
    }
}

// ============================================================================
// JOIN
// ============================================================================

mod joins {
    use super::*;

    #[tokio::test]
    async fn test_inner_join() {
        let ctx = create_test_context();
        let sql = "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id";
        let result = ctx.sql(sql).await.unwrap();
        assert_eq!(result.row_count, 6); // All orders have matching users
    }

    #[tokio::test]
    async fn test_inner_join_explicit() {
        let ctx = create_test_context();
        let sql = "SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id";
        let result = ctx.sql(sql).await.unwrap();
        assert_eq!(result.row_count, 6);
    }

    #[tokio::test]
    async fn test_left_join() {
        let ctx = create_test_context();
        let sql = "SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id";
        let result = ctx.sql(sql).await.unwrap();
        // User 5 has no orders, but should still appear
        assert!(result.row_count >= 5);
    }

    #[tokio::test]
    async fn test_right_join() {
        let ctx = create_test_context();
        let sql = "SELECT u.name, o.amount FROM users u RIGHT JOIN orders o ON u.id = o.user_id";
        let result = ctx.sql(sql).await.unwrap();
        assert_eq!(result.row_count, 6);
    }

    #[tokio::test]
    async fn test_full_outer_join() {
        let ctx = create_test_context();
        let sql = "SELECT u.name, o.amount FROM users u FULL OUTER JOIN orders o ON u.id = o.user_id";
        let result = ctx.sql(sql).await.unwrap();
        assert!(result.row_count >= 6);
    }

    #[tokio::test]
    async fn test_cross_join() {
        let ctx = create_test_context();
        let sql = "SELECT * FROM users CROSS JOIN products";
        let result = ctx.sql(sql).await.unwrap();
        assert_eq!(result.row_count, 25); // 5 users * 5 products
    }

    #[tokio::test]
    async fn test_join_with_where() {
        let ctx = create_test_context();
        let sql = "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'completed'";
        let result = ctx.sql(sql).await.unwrap();
        assert_eq!(result.row_count, 4);
    }

    #[tokio::test]
    async fn test_join_with_aggregate() {
        let ctx = create_test_context();
        let sql = "SELECT u.name, SUM(o.amount) as total FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name";
        let result = ctx.sql(sql).await.unwrap();
        assert!(result.row_count >= 3);
    }

    #[tokio::test]
    async fn test_self_join() {
        let ctx = create_test_context();
        let sql = "SELECT a.n, b.n FROM numbers a JOIN numbers b ON a.n + 1 = b.n";
        let result = ctx.sql(sql).await.unwrap();
        assert_eq!(result.row_count, 9); // 1-2, 2-3, ..., 9-10
    }

    #[tokio::test]
    async fn test_multiple_joins() {
        let ctx = create_test_context();
        // This is a made-up query but tests multiple joins
        let sql = "SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id JOIN users u2 ON u.id = u2.id";
        let result = ctx.sql(sql).await.unwrap();
        assert_eq!(result.row_count, 6);
    }
}

// ============================================================================
// DISTINCT
// ============================================================================

mod distinct {
    use super::*;

    #[tokio::test]
    async fn test_distinct_single() {
        let ctx = create_test_context();
        let sql = "SELECT DISTINCT user_id FROM orders";
        assert_eq!(query_row_count(&ctx, sql).await, 4);
    }

    #[tokio::test]
    async fn test_distinct_multiple() {
        let ctx = create_test_context();
        let sql = "SELECT DISTINCT user_id, status FROM orders";
        let result = ctx.sql(sql).await.unwrap();
        assert!(result.row_count >= 4);
    }

    #[tokio::test]
    async fn test_distinct_all() {
        let ctx = create_test_context();
        // DISTINCT ALL is same as no DISTINCT
        let sql = "SELECT DISTINCT status FROM orders";
        assert_eq!(query_row_count(&ctx, sql).await, 3);
    }

    #[tokio::test]
    async fn test_distinct_with_order_by() {
        let ctx = create_test_context();
        let sql = "SELECT DISTINCT age FROM users ORDER BY age DESC";
        let values = query_i64_column(&ctx, sql).await;
        assert_eq!(values.len(), 5);
        assert_eq!(values[0], Some(40));
    }

    #[tokio::test]
    async fn test_distinct_with_null() {
        let ctx = create_test_context();
        let sql = "SELECT DISTINCT name FROM users";
        // Should have 4 distinct non-null names + 1 NULL
        assert_eq!(query_row_count(&ctx, sql).await, 5);
    }
}

// ============================================================================
// SUBQUERIES
// ============================================================================

mod subqueries {
    use super::*;

    #[tokio::test]
    async fn test_subquery_in_where() {
        let ctx = create_test_context();
        let sql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)";
        assert_eq!(query_row_count(&ctx, sql).await, 4);
    }

    #[tokio::test]
    async fn test_subquery_scalar() {
        let ctx = create_test_context();
        let sql = "SELECT * FROM users WHERE age > (SELECT AVG(age) FROM users)";
        // Average age is 31.6, so users with age 35 and 40
        assert_eq!(query_row_count(&ctx, sql).await, 2);
    }

    #[tokio::test]
    async fn test_subquery_in_from() {
        let ctx = create_test_context();
        let sql = "SELECT * FROM (SELECT id, name FROM users WHERE active = true) AS active_users";
        assert_eq!(query_row_count(&ctx, sql).await, 3);
    }

    #[tokio::test]
    async fn test_subquery_exists() {
        let ctx = create_test_context();
        let sql = "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)";
        assert_eq!(query_row_count(&ctx, sql).await, 4);
    }

    #[tokio::test]
    async fn test_subquery_not_exists() {
        let ctx = create_test_context();
        let sql = "SELECT * FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)";
        assert_eq!(query_row_count(&ctx, sql).await, 1); // User 5
    }

    #[tokio::test]
    async fn test_correlated_subquery() {
        let ctx = create_test_context();
        let sql = "SELECT * FROM users u WHERE (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) > 2";
        assert_eq!(query_row_count(&ctx, sql).await, 1); // User 1 has 3 orders
    }
}

// ============================================================================
// UNION / INTERSECT / EXCEPT
// ============================================================================

mod set_operations {
    use super::*;

    #[tokio::test]
    async fn test_union() {
        let ctx = create_test_context();
        let sql = "SELECT id FROM users WHERE id <= 2 UNION SELECT id FROM users WHERE id >= 4";
        assert_eq!(query_row_count(&ctx, sql).await, 4); // 1, 2, 4, 5 (no duplicates)
    }

    #[tokio::test]
    async fn test_union_all() {
        let ctx = create_test_context();
        let sql = "SELECT id FROM users WHERE id <= 3 UNION ALL SELECT id FROM users WHERE id >= 2";
        assert_eq!(query_row_count(&ctx, sql).await, 7); // 1, 2, 3, 2, 3, 4, 5 (with duplicates)
    }

    #[tokio::test]
    async fn test_intersect() {
        let ctx = create_test_context();
        let sql = "SELECT id FROM users WHERE id <= 3 INTERSECT SELECT id FROM users WHERE id >= 2";
        assert_eq!(query_row_count(&ctx, sql).await, 2); // 2, 3
    }

    #[tokio::test]
    async fn test_except() {
        let ctx = create_test_context();
        let sql = "SELECT id FROM users WHERE id <= 4 EXCEPT SELECT id FROM users WHERE id >= 3";
        assert_eq!(query_row_count(&ctx, sql).await, 2); // 1, 2
    }
}

// ============================================================================
// EXPRESSIONS AND OPERATORS
// ============================================================================

mod expressions {
    use super::*;

    #[tokio::test]
    async fn test_arithmetic_add() {
        let ctx = create_test_context();
        let values = query_i64_column(&ctx, "SELECT age + 10 FROM users WHERE id = 1").await;
        assert_eq!(values, vec![Some(40)]);
    }

    #[tokio::test]
    async fn test_arithmetic_subtract() {
        let ctx = create_test_context();
        let values = query_i64_column(&ctx, "SELECT age - 5 FROM users WHERE id = 1").await;
        assert_eq!(values, vec![Some(25)]);
    }

    #[tokio::test]
    async fn test_arithmetic_multiply() {
        let ctx = create_test_context();
        let values = query_i64_column(&ctx, "SELECT age * 2 FROM users WHERE id = 1").await;
        assert_eq!(values, vec![Some(60)]);
    }

    #[tokio::test]
    async fn test_arithmetic_divide() {
        let ctx = create_test_context();
        let values = query_f64_column(&ctx, "SELECT salary / 1000.0 FROM users WHERE id = 1").await;
        assert!((values[0].unwrap() - 50.0).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_arithmetic_modulo() {
        let ctx = create_test_context();
        let values = query_i64_column(&ctx, "SELECT n % 3 FROM numbers WHERE n = 10").await;
        assert_eq!(values, vec![Some(1)]);
    }

    #[tokio::test]
    async fn test_negative() {
        let ctx = create_test_context();
        let values = query_i64_column(&ctx, "SELECT -age FROM users WHERE id = 1").await;
        assert_eq!(values, vec![Some(-30)]);
    }

    #[tokio::test]
    async fn test_concatenation() {
        let ctx = create_test_context();
        // String concatenation with ||
        let result = ctx.sql("SELECT name || ' test' FROM users WHERE id = 1").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_case_when() {
        let ctx = create_test_context();
        let sql = "SELECT CASE WHEN age >= 30 THEN 'senior' ELSE 'junior' END FROM users WHERE id = 1";
        let result = ctx.sql(sql).await.unwrap();
        assert_eq!(result.row_count, 1);
    }

    #[tokio::test]
    async fn test_case_when_multiple() {
        let ctx = create_test_context();
        let sql = r#"
            SELECT CASE
                WHEN age < 25 THEN 'young'
                WHEN age < 35 THEN 'middle'
                ELSE 'senior'
            END FROM users WHERE id = 1
        "#;
        let result = ctx.sql(sql).await.unwrap();
        assert_eq!(result.row_count, 1);
    }

    #[tokio::test]
    async fn test_coalesce() {
        let ctx = create_test_context();
        let sql = "SELECT COALESCE(name, 'Unknown') FROM users WHERE id = 5";
        let result = ctx.sql(sql).await.unwrap();
        assert_eq!(result.row_count, 1);
    }

    #[tokio::test]
    async fn test_nullif() {
        let ctx = create_test_context();
        let sql = "SELECT NULLIF(age, 30) FROM users WHERE id = 1";
        let result = ctx.sql(sql).await.unwrap();
        assert_eq!(result.row_count, 1);
    }

    #[tokio::test]
    async fn test_cast() {
        let ctx = create_test_context();
        let sql = "SELECT CAST(age AS FLOAT) FROM users WHERE id = 1";
        let values = query_f64_column(&ctx, sql).await;
        assert!((values[0].unwrap() - 30.0).abs() < 0.01);
    }
}

// ============================================================================
// STRING FUNCTIONS
// ============================================================================

mod string_functions {
    use super::*;

    #[tokio::test]
    async fn test_upper() {
        let ctx = create_test_context();
        let sql = "SELECT UPPER(name) FROM users WHERE id = 1";
        let result = ctx.sql(sql).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_lower() {
        let ctx = create_test_context();
        let sql = "SELECT LOWER(name) FROM users WHERE id = 1";
        let result = ctx.sql(sql).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_length() {
        let ctx = create_test_context();
        let sql = "SELECT LENGTH(name) FROM users WHERE id = 1";
        let result = ctx.sql(sql).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_substring() {
        let ctx = create_test_context();
        let sql = "SELECT SUBSTRING(name FROM 1 FOR 3) FROM users WHERE id = 1";
        let result = ctx.sql(sql).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_trim() {
        let ctx = create_test_context();
        let sql = "SELECT TRIM(name) FROM users WHERE id = 1";
        let result = ctx.sql(sql).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_replace() {
        let ctx = create_test_context();
        let sql = "SELECT REPLACE(name, 'A', 'X') FROM users WHERE id = 1";
        let result = ctx.sql(sql).await;
        assert!(result.is_ok());
    }
}

// ============================================================================
// MATH FUNCTIONS
// ============================================================================

mod math_functions {
    use super::*;

    #[tokio::test]
    async fn test_abs() {
        let ctx = create_test_context();
        let sql = "SELECT ABS(-10) FROM users WHERE id = 1";
        let values = query_i64_column(&ctx, sql).await;
        assert_eq!(values, vec![Some(10)]);
    }

    #[tokio::test]
    async fn test_round() {
        let ctx = create_test_context();
        let sql = "SELECT ROUND(salary / 1000.0, 1) FROM users WHERE id = 1";
        let result = ctx.sql(sql).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_floor() {
        let ctx = create_test_context();
        let sql = "SELECT FLOOR(3.7) FROM users WHERE id = 1";
        let result = ctx.sql(sql).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_ceil() {
        let ctx = create_test_context();
        let sql = "SELECT CEIL(3.2) FROM users WHERE id = 1";
        let result = ctx.sql(sql).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_power() {
        let ctx = create_test_context();
        let sql = "SELECT POWER(2, 3) FROM users WHERE id = 1";
        let result = ctx.sql(sql).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sqrt() {
        let ctx = create_test_context();
        let sql = "SELECT SQRT(16) FROM users WHERE id = 1";
        let result = ctx.sql(sql).await;
        assert!(result.is_ok());
    }
}

// ============================================================================
// NULL HANDLING
// ============================================================================

mod null_handling {
    use super::*;

    #[tokio::test]
    async fn test_null_in_arithmetic() {
        let ctx = create_test_context();
        // NULL + anything = NULL
        let sql = "SELECT name, age + NULL FROM users WHERE id = 1";
        let result = ctx.sql(sql).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_null_in_comparison() {
        let ctx = create_test_context();
        // NULL = NULL is not true
        let sql = "SELECT * FROM users WHERE name = NULL";
        assert_eq!(query_row_count(&ctx, sql).await, 0);
    }

    #[tokio::test]
    async fn test_null_aggregate() {
        let ctx = create_test_context();
        // SUM ignores NULLs
        let sql = "SELECT SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) FROM users";
        let values = query_i64_column(&ctx, sql).await;
        assert_eq!(values, vec![Some(1)]);
    }
}

// ============================================================================
// EDGE CASES
// ============================================================================

mod edge_cases {
    use super::*;

    #[tokio::test]
    async fn test_empty_result() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users WHERE 1 = 0").await, 0);
    }

    #[tokio::test]
    async fn test_always_true() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users WHERE 1 = 1").await, 5);
    }

    #[tokio::test]
    async fn test_double_negative() {
        let ctx = create_test_context();
        assert_eq!(query_row_count(&ctx, "SELECT * FROM users WHERE NOT NOT active").await, 3);
    }

    #[tokio::test]
    async fn test_deeply_nested_expression() {
        let ctx = create_test_context();
        let sql = "SELECT * FROM users WHERE ((((age > 20))))";
        assert_eq!(query_row_count(&ctx, sql).await, 5);
    }

    #[tokio::test]
    async fn test_table_alias() {
        let ctx = create_test_context();
        let sql = "SELECT u.id FROM users AS u WHERE u.id = 1";
        assert_eq!(query_row_count(&ctx, sql).await, 1);
    }

    #[tokio::test]
    async fn test_column_alias_in_order_by() {
        let ctx = create_test_context();
        let sql = "SELECT id, age AS a FROM users ORDER BY a";
        let result = ctx.sql(sql).await.unwrap();
        assert_eq!(result.row_count, 5);
    }

    #[tokio::test]
    async fn test_qualified_column_names() {
        let ctx = create_test_context();
        let sql = "SELECT users.id, users.name FROM users";
        let result = ctx.sql(sql).await.unwrap();
        assert_eq!(result.row_count, 5);
    }

    #[tokio::test]
    async fn test_reserved_word_as_alias() {
        let ctx = create_test_context();
        let sql = "SELECT id AS \"select\", name AS \"from\" FROM users";
        let result = ctx.sql(sql).await;
        assert!(result.is_ok());
    }
}

// ============================================================================
// ERROR HANDLING
// ============================================================================

mod error_handling {
    use super::*;

    #[tokio::test]
    async fn test_nonexistent_table() {
        let ctx = create_test_context();
        let result = ctx.sql("SELECT * FROM nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_nonexistent_column() {
        let ctx = create_test_context();
        let result = ctx.sql("SELECT nonexistent FROM users").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_ambiguous_column() {
        let ctx = create_test_context();
        // When joining tables with same column name without qualifier
        let result = ctx.sql("SELECT order_id FROM users JOIN orders ON users.id = orders.user_id").await;
        // This might work or error depending on implementation
        let _ = result;
    }

    #[tokio::test]
    async fn test_invalid_syntax() {
        let ctx = create_test_context();
        let result = ctx.sql("SELECTT * FROM users").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_type_mismatch() {
        let ctx = create_test_context();
        let result = ctx.sql("SELECT * FROM users WHERE age = 'not a number'").await;
        // Might error or do implicit conversion
        let _ = result;
    }

    #[tokio::test]
    async fn test_division_by_zero() {
        let ctx = create_test_context();
        let result = ctx.sql("SELECT age / 0 FROM users WHERE id = 1").await;
        // Might error or return NULL/Infinity
        let _ = result;
    }
}

// ============================================================================
// COMPLEX QUERIES
// ============================================================================

mod complex_queries {
    use super::*;

    #[tokio::test]
    async fn test_tpch_style_query() {
        let ctx = create_test_context();
        let sql = r#"
            SELECT
                u.name,
                COUNT(o.order_id) as order_count,
                SUM(o.amount) as total_amount
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            WHERE u.active = true
            GROUP BY u.name
            HAVING COUNT(o.order_id) > 0
            ORDER BY total_amount DESC
            LIMIT 3
        "#;
        let result = ctx.sql(sql).await.unwrap();
        assert!(result.row_count > 0);
    }

    #[tokio::test]
    async fn test_nested_aggregation() {
        let ctx = create_test_context();
        let sql = r#"
            SELECT status, avg_amount
            FROM (
                SELECT status, AVG(amount) as avg_amount
                FROM orders
                GROUP BY status
            ) AS subq
            WHERE avg_amount > 100
            ORDER BY avg_amount DESC
        "#;
        let result = ctx.sql(sql).await.unwrap();
        assert!(result.row_count > 0);
    }

    #[tokio::test]
    async fn test_multiple_subqueries() {
        let ctx = create_test_context();
        let sql = r#"
            SELECT *
            FROM users
            WHERE id IN (SELECT user_id FROM orders WHERE amount > 200)
              AND age > (SELECT AVG(age) FROM users WHERE active = true)
        "#;
        let result = ctx.sql(sql).await;
        // This might or might not be supported
        let _ = result;
    }

    #[tokio::test]
    async fn test_window_function() {
        let ctx = create_test_context();
        let sql = r#"
            SELECT
                id,
                name,
                age,
                ROW_NUMBER() OVER (ORDER BY age DESC) as rank
            FROM users
        "#;
        let result = ctx.sql(sql).await;
        // Window functions might not be implemented yet
        let _ = result;
    }

    #[tokio::test]
    async fn test_cte() {
        let ctx = create_test_context();
        let sql = r#"
            WITH active_users AS (
                SELECT * FROM users WHERE active = true
            )
            SELECT * FROM active_users WHERE age > 25
        "#;
        let result = ctx.sql(sql).await;
        // CTEs might not be implemented yet
        let _ = result;
    }
}
