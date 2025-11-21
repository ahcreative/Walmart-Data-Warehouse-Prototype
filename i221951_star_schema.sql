DROP DATABASE IF EXISTS walmart_dw;
CREATE DATABASE walmart_dw;
USE walmart_dw;

CREATE TABLE dim_customer (
    Customer_ID INT PRIMARY KEY,
    Gender VARCHAR(10),
    Age VARCHAR(20),
    City_Category VARCHAR(10),
    Marital_Status INT,
    Occupation INT,
    Stay_In_Current_City_Years INT,
    
    -- Indexes for OLAP queries
    INDEX idx_gender (Gender),
    INDEX idx_age (Age),
    INDEX idx_city_category (City_Category),
    INDEX idx_marital_status (Marital_Status),
    INDEX idx_occupation (Occupation),
    INDEX idx_stay_duration (Stay_In_Current_City_Years)
);

CREATE TABLE dim_product (
    Product_ID VARCHAR(20) PRIMARY KEY,
    Product_Category VARCHAR(100),
    Price DECIMAL(10,2),
    Store_ID INT,
    Supplier_ID INT,
    Store_Name VARCHAR(100),
    Supplier_Name VARCHAR(100),
    
    -- Indexes for OLAP queries
    INDEX idx_product_category (Product_Category),
    INDEX idx_store_id (Store_ID),
    INDEX idx_supplier_id (Supplier_ID),
    INDEX idx_store_name (Store_Name),
    INDEX idx_supplier_name (Supplier_Name),
    INDEX idx_store_category (Store_ID, Product_Category)
);

CREATE TABLE dim_time (
    Time_ID INT AUTO_INCREMENT PRIMARY KEY,
    Date DATE UNIQUE NOT NULL,
    Day_Of_Week VARCHAR(10),           -- Monday, Tuesday, etc.
    Day_Of_Week_Number INT,            -- 1=Monday, 7=Sunday
    Is_Weekend BOOLEAN,                -- TRUE for Sat/Sun
    Week_Of_Year INT,                  -- 1-53
    Month_Number INT,                  -- 1-12
    Month_Name VARCHAR(15),            -- January, February, etc.
    Quarter VARCHAR(10),               -- Q1, Q2, Q3, Q4
    Quarter_Number INT,                -- 1, 2, 3, 4
    Half_Year VARCHAR(10),             -- H1, H2
    Year INT,
    Season VARCHAR(10),                -- Spring, Summer, Fall, Winter
    
    -- Indexes for temporal queries
    INDEX idx_date (Date),
    INDEX idx_is_weekend (Is_Weekend),
    INDEX idx_month (Month_Number, Year),
    INDEX idx_quarter (Quarter_Number, Year),
    INDEX idx_year (Year),
    INDEX idx_season (Season, Year),
    INDEX idx_day_of_week (Day_Of_Week_Number)
);


CREATE TABLE fact_sales (
    Sales_ID INT AUTO_INCREMENT PRIMARY KEY,  -- Surrogate key
    Order_ID INT NOT NULL,                    -- Business key (NOT UNIQUE - one order can have multiple products)
    Customer_ID INT NOT NULL,
    Product_ID VARCHAR(20) NOT NULL,
    Time_ID INT NOT NULL,
    Quantity INT NOT NULL,
    Total_Amount DECIMAL(12,2) NOT NULL,      -- Calculated in ETL: Price × Quantity
    
    -- Foreign Keys
    FOREIGN KEY (Customer_ID) REFERENCES dim_customer(Customer_ID) ON DELETE RESTRICT,
    FOREIGN KEY (Product_ID) REFERENCES dim_product(Product_ID) ON DELETE RESTRICT,
    FOREIGN KEY (Time_ID) REFERENCES dim_time(Time_ID) ON DELETE RESTRICT,
    
    -- Indexes for queries
    INDEX idx_order (Order_ID),
    INDEX idx_customer (Customer_ID),
    INDEX idx_product (Product_ID),
    INDEX idx_time (Time_ID),
    INDEX idx_composite_customer_time (Customer_ID, Time_ID),
    INDEX idx_composite_product_time (Product_ID, Time_ID),
    INDEX idx_composite_order_product (Order_ID, Product_ID)
);

CREATE TABLE fact_product_association (
    Association_ID INT AUTO_INCREMENT PRIMARY KEY,
    Order_ID INT NOT NULL,
    Product_ID_1 VARCHAR(20) NOT NULL,
    Product_ID_2 VARCHAR(20) NOT NULL,
    Purchase_Date DATE NOT NULL,
    
    INDEX idx_order (Order_ID),
    INDEX idx_product_pair (Product_ID_1, Product_ID_2),
    INDEX idx_date (Purchase_Date),
    
    -- Ensure we don't duplicate pairs (A,B) and (B,A)
    UNIQUE KEY unique_pair (Order_ID, Product_ID_1, Product_ID_2)
);

CREATE TABLE view_store_quarterly_sales (
    Store_ID INT,
    Store_Name VARCHAR(100),
    Year INT,
    Quarter_Number INT,
    Quarter VARCHAR(10),
    Total_Sales DECIMAL(15,2),
    Total_Quantity INT,
    Total_Orders INT,
    
    PRIMARY KEY (Store_ID, Year, Quarter_Number),
    INDEX idx_store (Store_ID),
    INDEX idx_year_quarter (Year, Quarter_Number)
);

DROP PROCEDURE IF EXISTS populate_time_dimension;

DELIMITER $

CREATE PROCEDURE populate_time_dimension(
    IN start_date DATE,
    IN end_date DATE
)
BEGIN
    DECLARE current_dt DATE;
    DECLARE day_num INT;
    DECLARE month_num INT;
    DECLARE quarter_num INT;
    DECLARE year_num INT;
    
    SET current_dt = start_date;
    
    WHILE current_dt <= end_date DO
        SET day_num = DAYOFWEEK(current_dt);
        SET month_num = MONTH(current_dt);
        SET quarter_num = QUARTER(current_dt);
        SET year_num = YEAR(current_dt);
        
        INSERT IGNORE INTO dim_time (
            Date,
            Day_Of_Week,
            Day_Of_Week_Number,
            Is_Weekend,
            Week_Of_Year,
            Month_Number,
            Month_Name,
            Quarter,
            Quarter_Number,
            Half_Year,
            Year,
            Season
        ) VALUES (
            current_dt,
            DAYNAME(current_dt),
            day_num,
            day_num IN (1, 7),  -- Sunday=1, Saturday=7
            WEEK(current_dt, 3),
            month_num,
            MONTHNAME(current_dt),
            CONCAT('Q', quarter_num),
            quarter_num,
            CASE WHEN month_num <= 6 THEN 'H1' ELSE 'H2' END,
            year_num,
            CASE 
                WHEN month_num IN (3, 4, 5) THEN 'Spring'
                WHEN month_num IN (6, 7, 8) THEN 'Summer'
                WHEN month_num IN (9, 10, 11) THEN 'Fall'
                ELSE 'Winter'
            END
        );
        
        SET current_dt = DATE_ADD(current_dt, INTERVAL 1 DAY);
    END WHILE;
END$

DELIMITER ;

DROP PROCEDURE IF EXISTS refresh_store_quarterly_sales;

DELIMITER $

CREATE PROCEDURE refresh_store_quarterly_sales()
BEGIN
    TRUNCATE TABLE view_store_quarterly_sales;
    
    INSERT INTO view_store_quarterly_sales (
        Store_ID,
        Store_Name,
        Year,
        Quarter_Number,
        Quarter,
        Total_Sales,
        Total_Quantity,
        Total_Orders
    )
    SELECT 
        dp.Store_ID,
        dp.Store_Name,
        dt.Year,
        dt.Quarter_Number,
        dt.Quarter,
        SUM(fs.Total_Amount) AS Total_Sales,
        SUM(fs.Quantity) AS Total_Quantity,
        COUNT(DISTINCT fs.Order_ID) AS Total_Orders
    FROM fact_sales fs
    INNER JOIN dim_product dp ON fs.Product_ID = dp.Product_ID
    INNER JOIN dim_time dt ON fs.Time_ID = dt.Time_ID
    GROUP BY dp.Store_ID, dp.Store_Name, dt.Year, dt.Quarter_Number, dt.Quarter
    ORDER BY dp.Store_Name, dt.Year, dt.Quarter_Number;
END$

DELIMITER ;


-- For revenue analysis by time periods
CREATE INDEX idx_time_weekend_year ON dim_time(Is_Weekend, Year, Month_Number);

-- For demographic analysis
CREATE INDEX idx_customer_demographics ON dim_customer(Gender, Age, City_Category);

-- For occupation-based analysis
CREATE INDEX idx_customer_occupation_gender ON dim_customer(Occupation, Gender);

-- For product category analysis
CREATE INDEX idx_product_store_category ON dim_product(Store_ID, Product_Category);

CALL populate_time_dimension('2015-01-01', '2025-12-31');