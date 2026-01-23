//! TPC-H table schemas

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::Arc;

/// Get schema for NATION table
pub fn nation_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("n_nationkey", DataType::Int64, false),
        Field::new("n_name", DataType::Utf8, false),
        Field::new("n_regionkey", DataType::Int64, false),
        Field::new("n_comment", DataType::Utf8, true),
    ]))
}

/// Get schema for REGION table
pub fn region_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("r_regionkey", DataType::Int64, false),
        Field::new("r_name", DataType::Utf8, false),
        Field::new("r_comment", DataType::Utf8, true),
    ]))
}

/// Get schema for PART table
pub fn part_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("p_partkey", DataType::Int64, false),
        Field::new("p_name", DataType::Utf8, false),
        Field::new("p_mfgr", DataType::Utf8, false),
        Field::new("p_brand", DataType::Utf8, false),
        Field::new("p_type", DataType::Utf8, false),
        Field::new("p_size", DataType::Int32, false),
        Field::new("p_container", DataType::Utf8, false),
        Field::new("p_retailprice", DataType::Float64, false),
        Field::new("p_comment", DataType::Utf8, true),
    ]))
}

/// Get schema for SUPPLIER table
pub fn supplier_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("s_suppkey", DataType::Int64, false),
        Field::new("s_name", DataType::Utf8, false),
        Field::new("s_address", DataType::Utf8, false),
        Field::new("s_nationkey", DataType::Int64, false),
        Field::new("s_phone", DataType::Utf8, false),
        Field::new("s_acctbal", DataType::Float64, false),
        Field::new("s_comment", DataType::Utf8, true),
    ]))
}

/// Get schema for PARTSUPP table
pub fn partsupp_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ps_partkey", DataType::Int64, false),
        Field::new("ps_suppkey", DataType::Int64, false),
        Field::new("ps_availqty", DataType::Int32, false),
        Field::new("ps_supplycost", DataType::Float64, false),
        Field::new("ps_comment", DataType::Utf8, true),
    ]))
}

/// Get schema for CUSTOMER table
pub fn customer_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_name", DataType::Utf8, false),
        Field::new("c_address", DataType::Utf8, false),
        Field::new("c_nationkey", DataType::Int64, false),
        Field::new("c_phone", DataType::Utf8, false),
        Field::new("c_acctbal", DataType::Float64, false),
        Field::new("c_mktsegment", DataType::Utf8, false),
        Field::new("c_comment", DataType::Utf8, true),
    ]))
}

/// Get schema for ORDERS table
pub fn orders_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderstatus", DataType::Utf8, false),
        Field::new("o_totalprice", DataType::Float64, false),
        Field::new("o_orderdate", DataType::Date32, false),
        Field::new("o_orderpriority", DataType::Utf8, false),
        Field::new("o_clerk", DataType::Utf8, false),
        Field::new("o_shippriority", DataType::Int32, false),
        Field::new("o_comment", DataType::Utf8, true),
    ]))
}

/// Get schema for LINEITEM table
pub fn lineitem_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_suppkey", DataType::Int64, false),
        Field::new("l_linenumber", DataType::Int32, false),
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_tax", DataType::Float64, false),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_linestatus", DataType::Utf8, false),
        Field::new("l_shipdate", DataType::Date32, false),
        Field::new("l_commitdate", DataType::Date32, false),
        Field::new("l_receiptdate", DataType::Date32, false),
        Field::new("l_shipinstruct", DataType::Utf8, false),
        Field::new("l_shipmode", DataType::Utf8, false),
        Field::new("l_comment", DataType::Utf8, true),
    ]))
}

/// All TPC-H table names
pub const TPCH_TABLES: &[&str] = &[
    "nation",
    "region",
    "part",
    "supplier",
    "partsupp",
    "customer",
    "orders",
    "lineitem",
];

/// Get schema for a table by name
pub fn get_table_schema(name: &str) -> Option<SchemaRef> {
    match name.to_lowercase().as_str() {
        "nation" => Some(nation_schema()),
        "region" => Some(region_schema()),
        "part" => Some(part_schema()),
        "supplier" => Some(supplier_schema()),
        "partsupp" => Some(partsupp_schema()),
        "customer" => Some(customer_schema()),
        "orders" => Some(orders_schema()),
        "lineitem" => Some(lineitem_schema()),
        _ => None,
    }
}

/// Scale factor row counts
pub struct TpchRowCounts {
    pub nation: usize,
    pub region: usize,
    pub part: usize,
    pub supplier: usize,
    pub partsupp: usize,
    pub customer: usize,
    pub orders: usize,
    pub lineitem: usize,
}

impl TpchRowCounts {
    /// Get row counts for a given scale factor
    pub fn for_scale_factor(sf: f64) -> Self {
        Self {
            nation: 25,
            region: 5,
            part: (200_000.0 * sf) as usize,
            supplier: (10_000.0 * sf) as usize,
            partsupp: (800_000.0 * sf) as usize,
            customer: (150_000.0 * sf) as usize,
            orders: (1_500_000.0 * sf) as usize,
            lineitem: (6_000_000.0 * sf) as usize,
        }
    }
}
