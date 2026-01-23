//! TPC-H data generator

use crate::execution::ExecutionContext;
use crate::tpch::schema::*;
use arrow::array::{Date32Array, Float64Array, Int32Array, Int64Array, StringBuilder};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rand::prelude::*;
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

/// TPC-H data generator
pub struct TpchGenerator {
    scale_factor: f64,
    rng: StdRng,
}

impl TpchGenerator {
    pub fn new(scale_factor: f64) -> Self {
        Self {
            scale_factor,
            rng: StdRng::seed_from_u64(42),
        }
    }

    pub fn with_seed(scale_factor: f64, seed: u64) -> Self {
        Self {
            scale_factor,
            rng: StdRng::seed_from_u64(seed),
        }
    }

    /// Generate all TPC-H tables and register with context
    pub fn generate_all(&mut self, ctx: &mut ExecutionContext) {
        let row_counts = TpchRowCounts::for_scale_factor(self.scale_factor);

        // Generate in order of dependencies
        let nation_batch = self.generate_nation();
        ctx.register_table("nation", nation_schema(), vec![nation_batch]);

        let region_batch = self.generate_region();
        ctx.register_table("region", region_schema(), vec![region_batch]);

        let part_batch = self.generate_part(row_counts.part);
        ctx.register_table("part", part_schema(), vec![part_batch]);

        let supplier_batch = self.generate_supplier(row_counts.supplier);
        ctx.register_table("supplier", supplier_schema(), vec![supplier_batch]);

        let partsupp_batch =
            self.generate_partsupp(row_counts.partsupp, row_counts.part, row_counts.supplier);
        ctx.register_table("partsupp", partsupp_schema(), vec![partsupp_batch]);

        let customer_batch = self.generate_customer(row_counts.customer);
        ctx.register_table("customer", customer_schema(), vec![customer_batch]);

        let orders_batch = self.generate_orders(row_counts.orders, row_counts.customer);
        ctx.register_table("orders", orders_schema(), vec![orders_batch]);

        let lineitem_batch = self.generate_lineitem(
            row_counts.lineitem,
            row_counts.orders,
            row_counts.part,
            row_counts.supplier,
        );
        ctx.register_table("lineitem", lineitem_schema(), vec![lineitem_batch]);
    }

    /// Generate all TPC-H tables and write to Parquet files
    pub fn generate_to_parquet(&mut self, output_dir: &Path) -> std::io::Result<()> {
        fs::create_dir_all(output_dir)?;
        let row_counts = TpchRowCounts::for_scale_factor(self.scale_factor);

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        println!(
            "Generating TPC-H data with scale factor {}",
            self.scale_factor
        );
        println!("Output directory: {}", output_dir.display());
        println!();

        // Generate all batches first to avoid borrow issues
        let nation_batch = self.generate_nation();
        let region_batch = self.generate_region();
        let part_batch = self.generate_part(row_counts.part);
        let supplier_batch = self.generate_supplier(row_counts.supplier);
        let partsupp_batch =
            self.generate_partsupp(row_counts.partsupp, row_counts.part, row_counts.supplier);
        let customer_batch = self.generate_customer(row_counts.customer);
        let orders_batch = self.generate_orders(row_counts.orders, row_counts.customer);
        let lineitem_batch = self.generate_lineitem(
            row_counts.lineitem,
            row_counts.orders,
            row_counts.part,
            row_counts.supplier,
        );

        // Write each table
        Self::write_parquet_file(output_dir, "nation", nation_schema(), nation_batch, &props)?;
        Self::write_parquet_file(output_dir, "region", region_schema(), region_batch, &props)?;
        Self::write_parquet_file(output_dir, "part", part_schema(), part_batch, &props)?;
        Self::write_parquet_file(
            output_dir,
            "supplier",
            supplier_schema(),
            supplier_batch,
            &props,
        )?;
        Self::write_parquet_file(
            output_dir,
            "partsupp",
            partsupp_schema(),
            partsupp_batch,
            &props,
        )?;
        Self::write_parquet_file(
            output_dir,
            "customer",
            customer_schema(),
            customer_batch,
            &props,
        )?;
        Self::write_parquet_file(output_dir, "orders", orders_schema(), orders_batch, &props)?;
        Self::write_parquet_file(
            output_dir,
            "lineitem",
            lineitem_schema(),
            lineitem_batch,
            &props,
        )?;

        println!("\nDone!");
        Ok(())
    }

    fn write_parquet_file(
        output_dir: &Path,
        table_name: &str,
        schema: SchemaRef,
        batch: RecordBatch,
        props: &WriterProperties,
    ) -> std::io::Result<()> {
        let path = output_dir.join(format!("{}.parquet", table_name));
        let file = File::create(&path)?;
        let mut writer = ArrowWriter::try_new(file, schema, Some(props.clone()))
            .map_err(std::io::Error::other)?;
        writer.write(&batch).map_err(std::io::Error::other)?;
        writer.close().map_err(std::io::Error::other)?;

        let file_size = fs::metadata(&path)?.len();
        println!(
            "  {}: {} rows -> {} ({} bytes)",
            table_name,
            batch.num_rows(),
            path.display(),
            file_size
        );
        Ok(())
    }

    fn generate_nation(&mut self) -> RecordBatch {
        let nations = [
            (0, "ALGERIA", 0),
            (1, "ARGENTINA", 1),
            (2, "BRAZIL", 1),
            (3, "CANADA", 1),
            (4, "EGYPT", 4),
            (5, "ETHIOPIA", 0),
            (6, "FRANCE", 3),
            (7, "GERMANY", 3),
            (8, "INDIA", 2),
            (9, "INDONESIA", 2),
            (10, "IRAN", 4),
            (11, "IRAQ", 4),
            (12, "JAPAN", 2),
            (13, "JORDAN", 4),
            (14, "KENYA", 0),
            (15, "MOROCCO", 0),
            (16, "MOZAMBIQUE", 0),
            (17, "PERU", 1),
            (18, "CHINA", 2),
            (19, "ROMANIA", 3),
            (20, "SAUDI ARABIA", 4),
            (21, "VIETNAM", 2),
            (22, "RUSSIA", 3),
            (23, "UNITED KINGDOM", 3),
            (24, "UNITED STATES", 1),
        ];

        let n_nationkey: Int64Array = nations.iter().map(|(k, _, _)| *k as i64).collect();
        let n_name: Vec<&str> = nations.iter().map(|(_, n, _)| *n).collect();
        let n_regionkey: Int64Array = nations.iter().map(|(_, _, r)| *r as i64).collect();
        let n_comment: Vec<Option<&str>> = nations.iter().map(|_| Some("comment")).collect();

        RecordBatch::try_new(
            nation_schema(),
            vec![
                Arc::new(n_nationkey),
                Arc::new(arrow::array::StringArray::from(n_name)),
                Arc::new(n_regionkey),
                Arc::new(arrow::array::StringArray::from(n_comment)),
            ],
        )
        .unwrap()
    }

    fn generate_region(&mut self) -> RecordBatch {
        let regions = [
            (0, "AFRICA"),
            (1, "AMERICA"),
            (2, "ASIA"),
            (3, "EUROPE"),
            (4, "MIDDLE EAST"),
        ];

        let r_regionkey: Int64Array = regions.iter().map(|(k, _)| *k as i64).collect();
        let r_name: Vec<&str> = regions.iter().map(|(_, n)| *n).collect();
        let r_comment: Vec<Option<&str>> = regions.iter().map(|_| Some("comment")).collect();

        RecordBatch::try_new(
            region_schema(),
            vec![
                Arc::new(r_regionkey),
                Arc::new(arrow::array::StringArray::from(r_name)),
                Arc::new(arrow::array::StringArray::from(r_comment)),
            ],
        )
        .unwrap()
    }

    fn generate_part(&mut self, count: usize) -> RecordBatch {
        let brands = [
            "Brand#11", "Brand#12", "Brand#13", "Brand#14", "Brand#15", "Brand#21", "Brand#22",
            "Brand#23", "Brand#24", "Brand#25",
        ];
        let containers = [
            "SM CASE", "SM BOX", "SM PACK", "SM PKG", "MED BAG", "MED BOX", "MED PKG", "MED PACK",
            "LG CASE", "LG BOX", "LG PACK", "LG PKG",
        ];
        let types = [
            "STANDARD ANODIZED TIN",
            "STANDARD ANODIZED NICKEL",
            "STANDARD ANODIZED BRASS",
            "STANDARD ANODIZED STEEL",
            "SMALL PLATED TIN",
            "SMALL PLATED NICKEL",
            "PROMO BURNISHED BRASS",
            "PROMO BURNISHED STEEL",
        ];

        let mut p_partkey = Vec::with_capacity(count);
        let mut p_name = StringBuilder::new();
        let mut p_mfgr = StringBuilder::new();
        let mut p_brand = StringBuilder::new();
        let mut p_type = StringBuilder::new();
        let mut p_size = Vec::with_capacity(count);
        let mut p_container = StringBuilder::new();
        let mut p_retailprice = Vec::with_capacity(count);
        let mut p_comment = StringBuilder::new();

        for i in 0..count {
            p_partkey.push((i + 1) as i64);
            p_name.append_value(format!("Part {}", i + 1));
            p_mfgr.append_value(format!("Manufacturer#{}", (i % 5) + 1));
            p_brand.append_value(brands[i % brands.len()]);
            p_type.append_value(types[i % types.len()]);
            p_size.push(self.rng.gen_range(1..50));
            p_container.append_value(containers[i % containers.len()]);
            p_retailprice.push(self.rng.gen_range(1.0..2000.0));
            p_comment.append_value("part comment");
        }

        RecordBatch::try_new(
            part_schema(),
            vec![
                Arc::new(Int64Array::from(p_partkey)),
                Arc::new(p_name.finish()),
                Arc::new(p_mfgr.finish()),
                Arc::new(p_brand.finish()),
                Arc::new(p_type.finish()),
                Arc::new(Int32Array::from(p_size)),
                Arc::new(p_container.finish()),
                Arc::new(Float64Array::from(p_retailprice)),
                Arc::new(p_comment.finish()),
            ],
        )
        .unwrap()
    }

    fn generate_supplier(&mut self, count: usize) -> RecordBatch {
        let mut s_suppkey = Vec::with_capacity(count);
        let mut s_name = StringBuilder::new();
        let mut s_address = StringBuilder::new();
        let mut s_nationkey = Vec::with_capacity(count);
        let mut s_phone = StringBuilder::new();
        let mut s_acctbal = Vec::with_capacity(count);
        let mut s_comment = StringBuilder::new();

        for i in 0..count {
            s_suppkey.push((i + 1) as i64);
            s_name.append_value(format!("Supplier#{:09}", i + 1));
            s_address.append_value(format!("Address {}", i + 1));
            s_nationkey.push(self.rng.gen_range(0..25) as i64);
            s_phone.append_value(format!(
                "{}-{}-{}-{}",
                self.rng.gen_range(10..34),
                self.rng.gen_range(100..999),
                self.rng.gen_range(100..999),
                self.rng.gen_range(1000..9999)
            ));
            s_acctbal.push(self.rng.gen_range(-999.99..9999.99));
            s_comment.append_value("supplier comment");
        }

        RecordBatch::try_new(
            supplier_schema(),
            vec![
                Arc::new(Int64Array::from(s_suppkey)),
                Arc::new(s_name.finish()),
                Arc::new(s_address.finish()),
                Arc::new(Int64Array::from(s_nationkey)),
                Arc::new(s_phone.finish()),
                Arc::new(Float64Array::from(s_acctbal)),
                Arc::new(s_comment.finish()),
            ],
        )
        .unwrap()
    }

    fn generate_partsupp(
        &mut self,
        count: usize,
        part_count: usize,
        supp_count: usize,
    ) -> RecordBatch {
        let mut ps_partkey = Vec::with_capacity(count);
        let mut ps_suppkey = Vec::with_capacity(count);
        let mut ps_availqty = Vec::with_capacity(count);
        let mut ps_supplycost = Vec::with_capacity(count);
        let mut ps_comment = StringBuilder::new();

        for i in 0..count {
            ps_partkey.push(((i % part_count) + 1) as i64);
            ps_suppkey.push(((i % supp_count) + 1) as i64);
            ps_availqty.push(self.rng.gen_range(1..9999));
            ps_supplycost.push(self.rng.gen_range(1.0..1000.0));
            ps_comment.append_value("partsupp comment");
        }

        RecordBatch::try_new(
            partsupp_schema(),
            vec![
                Arc::new(Int64Array::from(ps_partkey)),
                Arc::new(Int64Array::from(ps_suppkey)),
                Arc::new(Int32Array::from(ps_availqty)),
                Arc::new(Float64Array::from(ps_supplycost)),
                Arc::new(ps_comment.finish()),
            ],
        )
        .unwrap()
    }

    fn generate_customer(&mut self, count: usize) -> RecordBatch {
        let segments = [
            "AUTOMOBILE",
            "BUILDING",
            "FURNITURE",
            "HOUSEHOLD",
            "MACHINERY",
        ];

        let mut c_custkey = Vec::with_capacity(count);
        let mut c_name = StringBuilder::new();
        let mut c_address = StringBuilder::new();
        let mut c_nationkey = Vec::with_capacity(count);
        let mut c_phone = StringBuilder::new();
        let mut c_acctbal = Vec::with_capacity(count);
        let mut c_mktsegment = StringBuilder::new();
        let mut c_comment = StringBuilder::new();

        for i in 0..count {
            c_custkey.push((i + 1) as i64);
            c_name.append_value(format!("Customer#{:09}", i + 1));
            c_address.append_value(format!("Address {}", i + 1));
            c_nationkey.push(self.rng.gen_range(0..25) as i64);
            c_phone.append_value(format!(
                "{}-{}-{}-{}",
                self.rng.gen_range(10..34),
                self.rng.gen_range(100..999),
                self.rng.gen_range(100..999),
                self.rng.gen_range(1000..9999)
            ));
            c_acctbal.push(self.rng.gen_range(-999.99..9999.99));
            c_mktsegment.append_value(segments[i % segments.len()]);
            c_comment.append_value("customer comment");
        }

        RecordBatch::try_new(
            customer_schema(),
            vec![
                Arc::new(Int64Array::from(c_custkey)),
                Arc::new(c_name.finish()),
                Arc::new(c_address.finish()),
                Arc::new(Int64Array::from(c_nationkey)),
                Arc::new(c_phone.finish()),
                Arc::new(Float64Array::from(c_acctbal)),
                Arc::new(c_mktsegment.finish()),
                Arc::new(c_comment.finish()),
            ],
        )
        .unwrap()
    }

    fn generate_orders(&mut self, count: usize, cust_count: usize) -> RecordBatch {
        let status = ['O', 'F', 'P'];
        let priorities = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];

        let mut o_orderkey = Vec::with_capacity(count);
        let mut o_custkey = Vec::with_capacity(count);
        let mut o_orderstatus = StringBuilder::new();
        let mut o_totalprice = Vec::with_capacity(count);
        let mut o_orderdate = Vec::with_capacity(count);
        let mut o_orderpriority = StringBuilder::new();
        let mut o_clerk = StringBuilder::new();
        let mut o_shippriority = Vec::with_capacity(count);
        let mut o_comment = StringBuilder::new();

        // Base date: 1992-01-01 in days since epoch
        let base_date = 8035;
        let date_range = 2557; // About 7 years

        for i in 0..count {
            o_orderkey.push((i + 1) as i64);
            o_custkey.push(((i % cust_count) + 1) as i64);
            o_orderstatus.append_value(status[i % 3].to_string());
            o_totalprice.push(self.rng.gen_range(1000.0..500000.0));
            o_orderdate.push(base_date + self.rng.gen_range(0..date_range));
            o_orderpriority.append_value(priorities[i % priorities.len()]);
            o_clerk.append_value(format!("Clerk#{:09}", (i % 1000) + 1));
            o_shippriority.push(0);
            o_comment.append_value("order comment");
        }

        RecordBatch::try_new(
            orders_schema(),
            vec![
                Arc::new(Int64Array::from(o_orderkey)),
                Arc::new(Int64Array::from(o_custkey)),
                Arc::new(o_orderstatus.finish()),
                Arc::new(Float64Array::from(o_totalprice)),
                Arc::new(Date32Array::from(o_orderdate)),
                Arc::new(o_orderpriority.finish()),
                Arc::new(o_clerk.finish()),
                Arc::new(Int32Array::from(o_shippriority)),
                Arc::new(o_comment.finish()),
            ],
        )
        .unwrap()
    }

    fn generate_lineitem(
        &mut self,
        count: usize,
        order_count: usize,
        part_count: usize,
        supp_count: usize,
    ) -> RecordBatch {
        let returnflags = ['N', 'R', 'A'];
        let linestatus = ['O', 'F'];
        let shipinstruct = [
            "DELIVER IN PERSON",
            "COLLECT COD",
            "NONE",
            "TAKE BACK RETURN",
        ];
        let shipmode = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"];

        let mut l_orderkey: Vec<i64> = Vec::with_capacity(count);
        let mut l_partkey: Vec<i64> = Vec::with_capacity(count);
        let mut l_suppkey: Vec<i64> = Vec::with_capacity(count);
        let mut l_linenumber: Vec<i32> = Vec::with_capacity(count);
        let mut l_quantity: Vec<f64> = Vec::with_capacity(count);
        let mut l_extendedprice: Vec<f64> = Vec::with_capacity(count);
        let mut l_discount: Vec<f64> = Vec::with_capacity(count);
        let mut l_tax: Vec<f64> = Vec::with_capacity(count);
        let mut l_returnflag = StringBuilder::new();
        let mut l_linestatus = StringBuilder::new();
        let mut l_shipdate: Vec<i32> = Vec::with_capacity(count);
        let mut l_commitdate: Vec<i32> = Vec::with_capacity(count);
        let mut l_receiptdate: Vec<i32> = Vec::with_capacity(count);
        let mut l_shipinstruct = StringBuilder::new();
        let mut l_shipmode = StringBuilder::new();
        let mut l_comment = StringBuilder::new();

        // Base date: 1992-01-01 in days since epoch
        let base_date: i32 = 8035;
        let date_range: i32 = 2557;

        let mut current_order: i64 = 1;
        let mut line_num: i32 = 1;

        for i in 0..count {
            // Move to next order periodically
            if i > 0 && self.rng.gen_bool(0.25) {
                current_order = ((current_order as usize % order_count) + 1) as i64;
                line_num = 1;
            }

            l_orderkey.push(current_order);
            l_partkey.push(((i % part_count) + 1) as i64);
            l_suppkey.push(((i % supp_count) + 1) as i64);
            l_linenumber.push(line_num);
            line_num += 1;

            let qty = self.rng.gen_range(1.0..50.0);
            let price = self.rng.gen_range(900.0..100000.0);
            let disc = self.rng.gen_range(0.0..0.1);
            let tax = self.rng.gen_range(0.0..0.08);

            l_quantity.push(qty);
            l_extendedprice.push(price);
            l_discount.push(disc);
            l_tax.push(tax);

            l_returnflag.append_value(returnflags[i % 3].to_string());
            l_linestatus.append_value(linestatus[i % 2].to_string());

            let ship_date = base_date + self.rng.gen_range(0..date_range);
            l_shipdate.push(ship_date);
            l_commitdate.push(ship_date + self.rng.gen_range(0..30));
            l_receiptdate.push(ship_date + self.rng.gen_range(1..60));

            l_shipinstruct.append_value(shipinstruct[i % shipinstruct.len()]);
            l_shipmode.append_value(shipmode[i % shipmode.len()]);
            l_comment.append_value("lineitem comment");
        }

        RecordBatch::try_new(
            lineitem_schema(),
            vec![
                Arc::new(Int64Array::from(l_orderkey)),
                Arc::new(Int64Array::from(l_partkey)),
                Arc::new(Int64Array::from(l_suppkey)),
                Arc::new(Int32Array::from(l_linenumber)),
                Arc::new(Float64Array::from(l_quantity)),
                Arc::new(Float64Array::from(l_extendedprice)),
                Arc::new(Float64Array::from(l_discount)),
                Arc::new(Float64Array::from(l_tax)),
                Arc::new(l_returnflag.finish()),
                Arc::new(l_linestatus.finish()),
                Arc::new(Date32Array::from(l_shipdate)),
                Arc::new(Date32Array::from(l_commitdate)),
                Arc::new(Date32Array::from(l_receiptdate)),
                Arc::new(l_shipinstruct.finish()),
                Arc::new(l_shipmode.finish()),
                Arc::new(l_comment.finish()),
            ],
        )
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_small() {
        let mut gen = TpchGenerator::new(0.01);
        let mut ctx = ExecutionContext::new();
        gen.generate_all(&mut ctx);

        // Verify all tables were created
        let tables = ctx.table_names();
        assert!(tables.contains(&"nation".to_string()));
        assert!(tables.contains(&"region".to_string()));
        assert!(tables.contains(&"lineitem".to_string()));
    }
}
