# Walmart Near-Real-Time Data Warehouse

A comprehensive data warehouse implementation for Walmart that processes transactional data in near-real-time using the HYBRIDJOIN stream-relation join algorithm. This project demonstrates advanced ETL processing, star schema design, and complex OLAP analytics.

## 🎯 Project Overview

This system enables Walmart to analyze shopping behavior in near-real-time, facilitating dynamic promotions and personalized offers across millions of daily transactions. The implementation uses a custom HYBRIDJOIN algorithm to efficiently join streaming transactional data with master data records.

## 🏗️ Architecture

### Key Components

- **HYBRIDJOIN Algorithm**: Custom stream-based join processor with:
  - Hash table with 10,000 slots for stream tuples
  - FIFO queue for arrival order tracking
  - Disk buffer with 5,000 tuple partitions
  - Thread-safe stream buffer

- **Star Schema Data Warehouse**: Optimized for OLAP queries with:
  - Fact Tables: `fact_sales`, `fact_product_association`
  - Dimension Tables: `dim_customer`, `dim_product`, `dim_time`
  - Materialized View: `view_store_quarterly_sales`

- **Multi-threaded ETL Pipeline**:
  - Thread 1: Continuous stream reader from CSV
  - Thread 2: HYBRIDJOIN processor with batch commits

## 📊 Star Schema Design
```
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
```

## 🚀 Features

### HYBRIDJOIN Algorithm Implementation
- **Stream Processing**: Handles continuous transactional data streams
- **Memory Management**: Fixed 10,000 slot hash table with efficient collision handling
- **Fair Processing**: FIFO queue ensures oldest data processed first
- **Batch Optimization**: Commits every 200 transactions for performance
- **Thread Safety**: Lock-based synchronization between reader and processor

### Advanced Analytics (20 OLAP Queries)
- Revenue analysis by weekdays/weekends with monthly drill-down
- Customer demographics by purchase patterns
- Product category performance by occupation
- Seasonal sales trends and growth rates
- Store and supplier performance metrics
- Product affinity analysis for bundling strategies
- Outlier detection for demand spikes
- Multi-dimensional aggregations with ROLLUP

## 📋 Prerequisites

- Python 3.8+
- MySQL 8.0+
- Required Python packages:
```
  pandas
  mysql-connector-python
```

## 🔧 Installation

1. **Clone the repository**
```bash
   git clone https://github.com/yourusername/walmart-data-warehouse.git
   cd walmart-data-warehouse
```

2. **Install dependencies**
```bash
   pip install pandas mysql-connector-python
```

3. **Set up the database**
```bash
   mysql -u root -p < schema.sql
```

4. **Prepare data files**
   - `transactional_data.csv` - Stream data (Order_ID, Customer_ID, Product_ID, date, quantity)
   - `customer_master_data.csv` - Customer dimension data
   - `product_master_data.csv` - Product dimension data

## 🎬 Usage

### Running the ETL Pipeline
```bash
python hybridjoin_etl.py
```

The system will prompt for:
- MySQL host (default: 127.0.0.1)
- MySQL username (default: root)
- MySQL password

### Process Flow

1. **Master Data Loading**: Populates dimension tables with customer and product data
2. **Stream Processing**: Reads transactional data continuously
3. **HYBRIDJOIN Execution**: 
   - Loads stream tuples into hash table (up to w available slots)
   - Retrieves oldest key from queue
   - Loads corresponding disk partition (5,000 products)
   - Probes hash table for matches
   - Inserts joined records into fact_sales
   - Removes matched tuples and frees slots
4. **Product Association Mining**: Identifies frequently co-purchased products
5. **View Refresh**: Updates quarterly sales aggregations

### Monitoring Output
```
[Iteration 145] Loaded 2500/2500 tuples (w now = 0)
[Iteration 145] Joined 487 tuples | Freed 487 slots | w=487 | Total joins=15234
```

## 📈 Sample Analytics Queries

### Top Revenue Products by Weekend/Weekday
```sql
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
```

### Product Affinity Analysis
```sql
SELECT 
    fpa.Product_ID_1,
    fpa.Product_ID_2,
    COUNT(*) AS Co_Purchase_Count
FROM fact_product_association fpa
GROUP BY fpa.Product_ID_1, fpa.Product_ID_2
ORDER BY Co_Purchase_Count DESC
LIMIT 5;
```

### Quarterly Store Performance
```sql
SELECT * FROM view_store_quarterly_sales
WHERE Year = 2024
ORDER BY Store_Name, Quarter_Number;
```

## 🔍 Key Implementation Details

### Hash Function
```python
def hash_function(key):
    return int(hashlib.md5(str(key).encode()).hexdigest(), 16) % hS
```

### Time Dimension Auto-Population
- Automatically generates time records from 2015-2025
- Computes derived attributes: season, quarter, weekend flags
- Supports drill-down from year → quarter → month → day

### Performance Optimizations
- Composite indexes on frequently joined columns
- Batch commits (200 transactions per commit)
- On-demand partition loading
- Efficient node deletion from hash table and queue

## 📊 Database Schema Highlights

### Fact Tables
- **fact_sales**: 8 indexes for fast OLAP queries
- **fact_product_association**: Supports market basket analysis

### Dimension Tables
- **dim_customer**: 6 indexes for demographic slicing
- **dim_product**: 7 indexes for product hierarchy analysis
- **dim_time**: 7 indexes for temporal patterns

### Stored Procedures
- `populate_time_dimension(start_date, end_date)`: Time dimension loader
- `refresh_store_quarterly_sales()`: Materialized view refresh

## 🎓 Academic Context

This project demonstrates:
- Stream-relation join algorithms (HYBRIDJOIN)
- Multi-dimensional data modeling (star schema)
- ETL pipeline design with data enrichment
- OLAP query optimization techniques
- Near-real-time data warehouse architecture

## 📁 Project Structure
```
walmart-data-warehouse/
│
├── hybridjoin_etl.py          # Main ETL implementation with HYBRIDJOIN
├── schema.sql                  # Database schema and stored procedures
├── transactional_data.csv      # Stream data (not included)
├── customer_master_data.csv    # Customer dimension (not included)
├── product_master_data.csv     # Product dimension (not included)
├── queries/                    # OLAP query implementations
│   ├── q1_revenue_weekday_weekend.sql
│   ├── q2_demographics_analysis.sql
│   └── ... (q3-q20)
│
└── README.md
```

## 🐛 Troubleshooting

### Common Issues

**Issue**: `ModuleNotFoundError: No module named 'mysql.connector'`
```bash
pip install mysql-connector-python
```

**Issue**: `Access denied for user 'root'@'localhost'`
- Verify MySQL credentials
- Ensure MySQL server is running
- Check user permissions: `GRANT ALL PRIVILEGES ON walmart_dw.* TO 'root'@'localhost';`

**Issue**: Stream buffer overflow
- Increase `STREAM_DELAY` value in configuration
- Adjust `BATCH_COMMIT_SIZE` for faster commits

## 📊 Performance Metrics

Expected performance on standard hardware:
- **Stream Processing**: ~500-1000 transactions/second
- **Join Operations**: ~100-500 joins/second
- **Memory Usage**: ~200-300 MB for hash table and buffers
- **Database Size**: ~50 MB per 10,000 transactions

## 🔐 Security Notes

- Never commit database credentials to version control
- Use environment variables for sensitive configuration
- Implement proper user authentication in production
- Regular backups of the data warehouse

## 📝 License

This project is created for educational purposes as part of a Data Warehousing course assignment.

#Contributing

This is an academic project. For improvements or suggestions, please open an issue.

## 📧 Contact

For questions or feedback about this implementation, please create an issue in the repository.

## 🙏 Acknowledgments

- Based on the HYBRIDJOIN algorithm for stream-relation joins
- Implements star schema best practices from Kimball methodology
- Uses MySQL for reliable OLAP operations

---

**Note**: This system is designed for educational and demonstration purposes. For production use, consider additional error handling, monitoring, and scalability enhancements.
