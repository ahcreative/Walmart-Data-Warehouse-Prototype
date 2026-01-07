<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
</head>
<body>

<h1>Walmart Near-Real-Time Data Warehouse</h1>

<p>A comprehensive data warehouse implementation for Walmart that processes transactional data in near-real-time using the HYBRIDJOIN stream-relation join algorithm. This project demonstrates advanced ETL processing, star schema design, and complex OLAP analytics.</p>

<details open>
  <summary>🎯 Project Overview</summary>
  <p>This system enables Walmart to analyze shopping behavior in near-real-time, facilitating dynamic promotions and personalized offers across millions of daily transactions. The implementation uses a custom HYBRIDJOIN algorithm to efficiently join streaming transactional data with master data records.</p>
</details>

<details>
  <summary>🏗️ Architecture</summary>
  <h3>Key Components</h3>
  <ul>
    <li><strong>HYBRIDJOIN Algorithm</strong>: Custom stream-based join processor with:
      <ul>
        <li>Hash table with 10,000 slots for stream tuples</li>
        <li>FIFO queue for arrival order tracking</li>
        <li>Disk buffer with 5,000 tuple partitions</li>
        <li>Thread-safe stream buffer</li>
      </ul>
    </li>
    <li><strong>Star Schema Data Warehouse</strong>: Optimized for OLAP queries with:
      <ul>
        <li>Fact Tables: <code>fact_sales</code>, <code>fact_product_association</code></li>
        <li>Dimension Tables: <code>dim_customer</code>, <code>dim_product</code>, <code>dim_time</code></li>
        <li>Materialized View: <code>view_store_quarterly_sales</code></li>
      </ul>
    </li>
    <li><strong>Multi-threaded ETL Pipeline</strong>:
      <ul>
        <li>Thread 1: Continuous stream reader from CSV</li>
        <li>Thread 2: HYBRIDJOIN processor with batch commits</li>
      </ul>
    </li>
  </ul>
</details>

<details>
  <summary>📊 Star Schema Design</summary>
  <pre>
┌─────────────────┐
│   dim_customer  │
│─────────────────│
│ Customer_ID (PK)│──┐
│ Gender          │  │
│ Age             │  │
│ City_Category   │  │
│ Marital_Status  │  │
│ Occupation      │  │
│ Stay_Duration   │  │
└─────────────────┘  │
                     │
┌─────────────────┐  │    ┌─────────────────┐
│   dim_product   │  │    │   fact_sales    │
│─────────────────│  │    │─────────────────│
│ Product_ID (PK) │──┼───▶│ Sales_ID (PK)   │
│ Category        │  │    │ Order_ID        │
│ Price           │  │    │ Customer_ID (FK)│◀─┘
│ Store_ID        │  │    │ Product_ID (FK) │◀─┐
│ Supplier_ID     │  │    │ Time_ID (FK)    │◀─┼─┐
│ Store_Name      │  │    │ Quantity        │  │ │
│ Supplier_Name   │  │    │ Total_Amount    │  │ │
└─────────────────┘  │    └─────────────────┘  │ │
                     │                          │ │
                     │    ┌─────────────────┐  │ │
                     │    │    dim_time     │  │ │
                     │    │─────────────────│  │ │
                     └───▶│ Time_ID (PK)    │──┘ │
                          │ Date            │    │
                          │ Day_Of_Week     │    │
                          │ Is_Weekend      │    │
                          │ Month_Name      │    │
                          │ Quarter         │    │
                          │ Half_Year       │    │
                          │ Year            │    │
                          │ Season          │    │
                          └─────────────────┘    │
                                                 │
          ┌──────────────────────────────────────┘
          │
          │    ┌─────────────────────────────┐
          │    │ fact_product_association    │
          │    │─────────────────────────────│
          └───▶│ Association_ID (PK)         │
               │ Order_ID                    │
               │ Product_ID_1                │
               │ Product_ID_2                │
               │ Purchase_Date               │
               └─────────────────────────────┘
  </pre>
</details>

<details>
  <summary>🚀 Features</summary>
  <h3>HYBRIDJOIN Algorithm Implementation</h3>
  <ul>
    <li>Stream Processing: Handles continuous transactional data streams</li>
    <li>Memory Management: Fixed 10,000 slot hash table with efficient collision handling</li>
    <li>Fair Processing: FIFO queue ensures oldest data processed first</li>
    <li>Batch Optimization: Commits every 200 transactions for performance</li>
    <li>Thread Safety: Lock-based synchronization between reader and processor</li>
  </ul>

  <h3>Advanced Analytics (20 OLAP Queries)</h3>
  <ul>
    <li>Revenue analysis by weekdays/weekends with monthly drill-down</li>
    <li>Customer demographics by purchase patterns</li>
    <li>Product category performance by occupation</li>
    <li>Seasonal sales trends and growth rates</li>
    <li>Store and supplier performance metrics</li>
    <li>Product affinity analysis for bundling strategies</li>
    <li>Outlier detection for demand spikes</li>
    <li>Multi-dimensional aggregations with ROLLUP</li>
  </ul>
</details>

<details>
  <summary>📋 Prerequisites & Installation</summary>
  <ul>
    <li>Python 3.8+</li>
    <li>MySQL 8.0+</li>
    <li>Power BI (Visualization & BI)</li>
    <li>Python packages:
      <pre>pandas
mysql-connector-python</pre>
    </li>
  </ul>

  <h3>Installation Steps</h3>
  <ol>
    <li>Clone the repository:
      <pre>git clone https://github.com/ahcreative/walmart-data-warehouse-prototype.git
cd walmart-data-warehouse</pre>
    </li>
    <li>Install dependencies:
      <pre>pip install pandas mysql-connector-python</pre>
    </li>
    <li>Set up the database:
      <pre>mysql -u root -p &lt; schema.sql</pre>
    </li>
    <li>Prepare data files:
      <ul>
        <li><code>transactional_data.csv</code> - Stream data</li>
        <li><code>customer_master_data.csv</code> - Customer dimension</li>
        <li><code>product_master_data.csv</code> - Product dimension</li>
      </ul>
    </li>
  </ol>
</details>

<details>
  <summary>🎬 Usage & Process Flow</summary>
  <h3>Run the ETL Pipeline</h3>
  <pre>python hybridjoin_etl.py</pre>
  <p>The system will prompt for MySQL host, username, and password.</p>

  <h3>Process Flow</h3>
  <ol>
    <li>Load master data into dimension tables</li>
    <li>Read transactional data continuously</li>
    <li>Execute HYBRIDJOIN: load hash table, probe partitions, insert into <code>fact_sales</code></li>
    <li>Product association mining</li>
    <li>View refresh for quarterly sales</li>
  </ol>

  <h3>Monitoring Output</h3>
  <pre>
[Iteration 145] Loaded 2500/2500 tuples (w now = 0)
[Iteration 145] Joined 487 tuples | Freed 487 slots | w=487 | Total joins=15234
  </pre>
</details>

<details>
  <summary>📈 Sample Analytics Queries</summary>
  <h3>Top Revenue Products by Weekend/Weekday</h3>
  <pre>
SELECT 
    dp.Product_ID,
    dt.Is_Weekend,
    dt.Month_Name,
    SUM(fs.Total_Amount) AS Revenue
FROM fact_sales fs
JOIN dim_product dp ON fs.Product_ID = dp.Product_ID
JOIN dim_time dt ON fs.Time_ID = dt.Time_ID
WHERE dt.Year = 2024
GROUP BY dp.Product_ID, dt.Is_Weekend, dt.Month_Name
ORDER BY Revenue DESC
LIMIT 5;
  </pre>

  <h3>Product Affinity Analysis</h3>
  <pre>
SELECT 
    fpa.Product_ID_1,
    fpa.Product_ID_2,
    COUNT(*) AS Co_Purchase_Count
FROM fact_product_association fpa
GROUP BY fpa.Product_ID_1, fpa.Product_ID_2
ORDER BY Co_Purchase_Count DESC
LIMIT 5;
  </pre>

  <h3>Quarterly Store Performance</h3>
  <pre>
SELECT * FROM view_store_quarterly_sales
WHERE Year = 2024
ORDER BY Store_Name, Quarter_Number;
  </pre>
</details>

<details>
  <summary>🔍 Implementation Details</summary>
  <h3>Hash Function</h3>
  <pre>
def hash_function(key):
    return int(hashlib.md5(str(key).encode()).hexdigest(), 16) % hS
  </pre>

  <h3>Time Dimension Auto-Population</h3>
  <ul>
    <li>Generates records from 2015-2025</li>
    <li>Computes season, quarter, weekend flags</li>
    <li>Supports drill-down: year → quarter → month → day</li>
  </ul>

  <h3>Performance Optimizations</h3>
  <ul>
    <li>Composite indexes</li>
    <li>Batch commits (200 transactions)</li>
    <li>On-demand partition loading</li>
    <li>Efficient node deletion</li>
  </ul>
</details>

<details>
  <summary>📊 Power BI Dashboard</summary>
    <h3>The Power BI dashboard provides:</h3>

  <ul>
    <li>Executive sales overview</li>
    <li>Customer demographic insights</li>
    <li>Product & store performance</li>
    <li>Seasonal and MoM growth analysis</li>
  </ul>
</details>

<details>
  <summary>🎓 Academic Context & License</summary>
  <ul>
    <li>Demonstrates stream-relation join algorithms (HYBRIDJOIN)</li>
    <li>Multi-dimensional star schema modeling</li>
    <li>ETL pipeline with enrichment</li>
    <li>OLAP query optimization</li>
    <li>Near-real-time DW architecture</li>
  </ul>
  <p>License: Educational purposes for Data Warehousing course assignment.</p>
</details>

<details>
  <summary>🤝 Contributing & Contact</summary>
  <p>This is an academic project. Open an issue for improvements or questions.</p>
</details>

<hr>
<p><strong>Note</strong>: Designed for educational and demonstration purposes. For production, add error handling, monitoring, and scalability improvements.</p>

</body>
</html>
