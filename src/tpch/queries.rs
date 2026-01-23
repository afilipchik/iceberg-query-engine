//! TPC-H queries

/// Get TPC-H query by number (1-22)
pub fn get_query(num: usize) -> Option<&'static str> {
    match num {
        1 => Some(Q1),
        2 => Some(Q2),
        3 => Some(Q3),
        4 => Some(Q4),
        5 => Some(Q5),
        6 => Some(Q6),
        7 => Some(Q7),
        8 => Some(Q8),
        9 => Some(Q9),
        10 => Some(Q10),
        11 => Some(Q11),
        12 => Some(Q12),
        13 => Some(Q13),
        14 => Some(Q14),
        15 => Some(Q15),
        16 => Some(Q16),
        17 => Some(Q17),
        18 => Some(Q18),
        19 => Some(Q19),
        20 => Some(Q20),
        21 => Some(Q21),
        22 => Some(Q22),
        _ => None,
    }
}

/// Q1: Pricing Summary Report
pub const Q1: &str = r#"
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) AS sum_qty,
    SUM(l_extendedprice) AS sum_base_price,
    SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    AVG(l_quantity) AS avg_qty,
    AVG(l_extendedprice) AS avg_price,
    AVG(l_discount) AS avg_disc,
    COUNT(*) AS count_order
FROM
    lineitem
WHERE
    l_shipdate <= DATE '1998-09-02'
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus
"#;

/// Q2: Minimum Cost Supplier
pub const Q2: &str = r#"
SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
FROM
    part,
    supplier,
    partsupp,
    nation,
    region
WHERE
    p_partkey = ps_partkey
    AND s_suppkey = ps_suppkey
    AND p_size = 15
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
LIMIT 100
"#;

/// Q3: Shipping Priority
pub const Q3: &str = r#"
SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < DATE '1995-03-15'
    AND l_shipdate > DATE '1995-03-15'
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10
"#;

/// Q4: Order Priority Checking
pub const Q4: &str = r#"
SELECT
    o_orderpriority,
    COUNT(*) AS order_count
FROM
    orders
WHERE
    o_orderdate >= DATE '1993-07-01'
    AND o_orderdate < DATE '1993-10-01'
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority
"#;

/// Q5: Local Supplier Volume
pub const Q5: &str = r#"
SELECT
    n_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= DATE '1994-01-01'
    AND o_orderdate < DATE '1995-01-01'
GROUP BY
    n_name
ORDER BY
    revenue DESC
"#;

/// Q6: Forecasting Revenue Change
pub const Q6: &str = r#"
SELECT
    SUM(l_extendedprice * l_discount) AS revenue
FROM
    lineitem
WHERE
    l_shipdate >= DATE '1994-01-01'
    AND l_shipdate < DATE '1995-01-01'
    AND l_discount >= 0.05
    AND l_discount <= 0.07
    AND l_quantity < 24
"#;

/// Q7: Volume Shipping
pub const Q7: &str = r#"
SELECT
    n1.n_name AS supp_nation,
    n2.n_name AS cust_nation,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    supplier,
    lineitem,
    orders,
    customer,
    nation n1,
    nation n2
WHERE
    s_suppkey = l_suppkey
    AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey
    AND c_nationkey = n2.n_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
        OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
    AND l_shipdate >= DATE '1995-01-01'
    AND l_shipdate <= DATE '1996-12-31'
GROUP BY
    n1.n_name,
    n2.n_name
ORDER BY
    supp_nation,
    cust_nation
"#;

/// Q8: National Market Share
pub const Q8: &str = r#"
SELECT
    SUM(l_extendedprice * (1 - l_discount)) AS mkt_share
FROM
    part,
    supplier,
    lineitem,
    orders,
    customer,
    nation n1,
    region
WHERE
    p_partkey = l_partkey
    AND s_suppkey = l_suppkey
    AND l_orderkey = o_orderkey
    AND o_custkey = c_custkey
    AND c_nationkey = n1.n_nationkey
    AND n1.n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND o_orderdate >= DATE '1995-01-01'
    AND o_orderdate <= DATE '1996-12-31'
"#;

/// Q9: Product Type Profit Measure
pub const Q9: &str = r#"
SELECT
    n_name AS nation,
    SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) AS sum_profit
FROM
    part,
    supplier,
    lineitem,
    partsupp,
    orders,
    nation
WHERE
    s_suppkey = l_suppkey
    AND ps_suppkey = l_suppkey
    AND ps_partkey = l_partkey
    AND p_partkey = l_partkey
    AND o_orderkey = l_orderkey
    AND s_nationkey = n_nationkey
GROUP BY
    n_name
ORDER BY
    nation
"#;

/// Q10: Returned Item Reporting
pub const Q10: &str = r#"
SELECT
    c_custkey,
    c_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM
    customer,
    orders,
    lineitem,
    nation
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate >= DATE '1993-10-01'
    AND o_orderdate < DATE '1994-01-01'
    AND l_returnflag = 'R'
    AND c_nationkey = n_nationkey
GROUP BY
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
ORDER BY
    revenue DESC
LIMIT 20
"#;

/// Q11: Important Stock Identification
pub const Q11: &str = r#"
SELECT
    ps_partkey,
    SUM(ps_supplycost * ps_availqty) AS value
FROM
    partsupp,
    supplier,
    nation
WHERE
    ps_suppkey = s_suppkey
    AND s_nationkey = n_nationkey
    AND n_name = 'GERMANY'
GROUP BY
    ps_partkey
ORDER BY
    value DESC
LIMIT 100
"#;

/// Q12: Shipping Modes and Order Priority
pub const Q12: &str = r#"
SELECT
    l_shipmode,
    SUM(CASE
        WHEN o_orderpriority = '1-URGENT'
            OR o_orderpriority = '2-HIGH'
            THEN 1
        ELSE 0
    END) AS high_line_count,
    SUM(CASE
        WHEN o_orderpriority <> '1-URGENT'
            AND o_orderpriority <> '2-HIGH'
            THEN 1
        ELSE 0
    END) AS low_line_count
FROM
    orders,
    lineitem
WHERE
    o_orderkey = l_orderkey
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= DATE '1994-01-01'
    AND l_receiptdate < DATE '1995-01-01'
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode
"#;

/// Q13: Customer Distribution
pub const Q13: &str = r#"
SELECT
    c_custkey,
    COUNT(o_orderkey) AS c_count
FROM
    customer
    LEFT JOIN orders ON c_custkey = o_custkey
GROUP BY
    c_custkey
ORDER BY
    c_count DESC
LIMIT 100
"#;

/// Q14: Promotion Effect
pub const Q14: &str = r#"
SELECT
    SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
    lineitem,
    part
WHERE
    l_partkey = p_partkey
    AND l_shipdate >= DATE '1995-09-01'
    AND l_shipdate < DATE '1995-10-01'
"#;

/// Q15: Top Supplier
pub const Q15: &str = r#"
WITH revenue AS (
    SELECT
        l_suppkey,
        SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
    FROM
        lineitem
    WHERE
        l_shipdate >= DATE '1996-01-01'
        AND l_shipdate < DATE '1996-04-01'
    GROUP BY
        l_suppkey
)
SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM
    supplier,
    revenue
WHERE
    s_suppkey = revenue.l_suppkey
    AND total_revenue = (
        SELECT MAX(total_revenue)
        FROM revenue
    )
ORDER BY
    total_revenue DESC
"#;

/// Q16: Parts/Supplier Relationship
pub const Q16: &str = r#"
SELECT
    p_brand,
    p_type,
    p_size,
    COUNT(ps_suppkey) AS supplier_cnt
FROM
    partsupp,
    part
WHERE
    p_partkey = ps_partkey
    AND p_brand <> 'Brand#45'
    AND p_size >= 1
GROUP BY
    p_brand,
    p_type,
    p_size
ORDER BY
    supplier_cnt DESC,
    p_brand,
    p_type,
    p_size
LIMIT 100
"#;

/// Q17: Small-Quantity-Order Revenue
pub const Q17: &str = r#"
SELECT
    SUM(l_extendedprice) / 7.0 AS avg_yearly
FROM
    lineitem,
    part
WHERE
    p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container = 'MED BOX'
    AND l_quantity < (
        SELECT 0.2 * AVG(l_quantity)
        FROM lineitem
        WHERE l_partkey = p_partkey
    )
"#;

/// Q18: Large Volume Customer
pub const Q18: &str = r#"
SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    SUM(l_quantity) AS total_quantity
FROM
    customer,
    orders,
    lineitem
WHERE
    c_custkey = o_custkey
    AND o_orderkey = l_orderkey
GROUP BY
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
ORDER BY
    o_totalprice DESC,
    o_orderdate
LIMIT 100
"#;

/// Q19: Discounted Revenue
pub const Q19: &str = r#"
SELECT
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    lineitem,
    part
WHERE
    p_partkey = l_partkey
    AND (
        (p_brand = 'Brand#12'
        AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND l_quantity >= 1 AND l_quantity <= 11)
        OR (p_brand = 'Brand#23'
        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l_quantity >= 10 AND l_quantity <= 20)
        OR (p_brand = 'Brand#34'
        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l_quantity >= 20 AND l_quantity <= 30)
    )
    AND (
        (p_brand = 'Brand#12' AND l_shipmode IN ('AIR', 'AIR REG'))
        OR (p_brand = 'Brand#23' AND l_shipmode IN ('AIR', 'AIR REG'))
        OR (p_brand = 'Brand#34' AND l_shipmode IN ('AIR', 'AIR REG'))
    )
"#;

/// Q20: Potential Part Promotion
/// Note: Uses 'Part 1%' instead of 'forest%' because our generator creates simple part names
pub const Q20: &str = r#"
SELECT
    s_name,
    s_address
FROM
    supplier,
    nation
WHERE
    s_suppkey IN (
        SELECT ps_suppkey
        FROM partsupp
        WHERE ps_partkey IN (
            SELECT p_partkey
            FROM part
            WHERE p_name LIKE 'Part 1%'
        )
        AND ps_availqty > (
            SELECT 0.5 * SUM(l_quantity)
            FROM lineitem
            WHERE l_partkey = ps_partkey
            AND l_suppkey = ps_suppkey
            AND l_shipdate >= DATE '1994-01-01'
            AND l_shipdate < DATE '1995-01-01'
        )
    )
    AND s_nationkey = n_nationkey
    AND n_name = 'CANADA'
ORDER BY
    s_name
"#;

/// Q21: Suppliers Who Kept Orders Waiting
pub const Q21: &str = r#"
SELECT
    s_name,
    COUNT(*) AS numwait
FROM
    supplier,
    lineitem l1,
    orders,
    nation
WHERE
    s_suppkey = l1.l_suppkey
    AND o_orderkey = l1.l_orderkey
    AND o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS (
        SELECT *
        FROM lineitem l2
        WHERE l2.l_orderkey = l1.l_orderkey
        AND l2.l_suppkey <> l1.l_suppkey
    )
    AND NOT EXISTS (
        SELECT *
        FROM lineitem l3
        WHERE l3.l_orderkey = l1.l_orderkey
        AND l3.l_suppkey <> l1.l_suppkey
        AND l3.l_receiptdate > l3.l_commitdate
    )
    AND s_nationkey = n_nationkey
    AND n_name = 'SAUDI ARABIA'
GROUP BY
    s_name
ORDER BY
    numwait DESC,
    s_name
LIMIT 100
"#;

/// Q22: Global Sales Opportunity
pub const Q22: &str = r#"
SELECT
    cntrycode,
    COUNT(*) AS numcust,
    SUM(c_acctbal) AS totacctbal
FROM (
    SELECT
        SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode,
        c_acctbal
    FROM
        customer
    WHERE
        SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
        AND c_acctbal > (
            SELECT AVG(c_acctbal)
            FROM customer
            WHERE c_acctbal > 0.00
            AND SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
        )
        AND NOT EXISTS (
            SELECT *
            FROM orders
            WHERE o_custkey = c_custkey
        )
) AS custsale
GROUP BY
    cntrycode
ORDER BY
    cntrycode
"#;

/// All query numbers
pub const ALL_QUERIES: [usize; 22] = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_queries_exist() {
        for q in ALL_QUERIES {
            assert!(get_query(q).is_some(), "Query {} should exist", q);
        }
    }
}
