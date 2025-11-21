USE walmart_dw;

-- Q1
SELECT 
    dt.Month_Name,
    dt.Is_Weekend,
    fs.Product_ID,
    dp.Product_Category,
    dp.Store_Name,
    SUM(fs.Total_Amount) AS Revenue
FROM fact_sales fs
JOIN dim_product dp ON fs.Product_ID = dp.Product_ID
JOIN dim_time dt ON fs.Time_ID = dt.Time_ID
WHERE dt.Year = 2016
GROUP BY dt.Month_Name, dt.Is_Weekend, fs.Product_ID
ORDER BY dt.Month_Name, dt.Is_Weekend DESC, Revenue DESC
LIMIT 5;

-- Q2
SELECT 
    dc.Gender,
    dc.Age,
    dc.City_Category,
    SUM(fs.Total_Amount) AS Total_Purchase
FROM fact_sales fs
JOIN dim_customer dc ON fs.Customer_ID = dc.Customer_ID
GROUP BY dc.Gender, dc.Age, dc.City_Category
ORDER BY Total_Purchase DESC;

-- Q3
SELECT 
    dc.Occupation,
    dp.Product_Category,
    SUM(fs.Total_Amount) AS Total_Sales
FROM fact_sales fs
JOIN dim_customer dc ON fs.Customer_ID = dc.Customer_ID
JOIN dim_product dp ON fs.Product_ID = dp.Product_ID
GROUP BY dc.Occupation, dp.Product_Category
ORDER BY dc.Occupation, Total_Sales DESC;

-- Q4
SELECT 
    dc.Gender,
    dc.Age,
    dt.Quarter,
    SUM(fs.Total_Amount) AS Total_Purchase
FROM fact_sales fs
JOIN dim_customer dc ON fs.Customer_ID = dc.Customer_ID
JOIN dim_time dt ON fs.Time_ID = dt.Time_ID
WHERE dt.Year = 2015
GROUP BY dc.Gender, dc.Age, dt.Quarter
ORDER BY dt.Quarter, Total_Purchase DESC;

-- Q5
SELECT *
FROM (
    SELECT 
        dp.Product_Category,
        dc.Occupation,
        SUM(fs.Total_Amount) AS Revenue,
        ROW_NUMBER() OVER (PARTITION BY dp.Product_Category ORDER BY SUM(fs.Total_Amount) DESC) AS rn
    FROM fact_sales fs
    JOIN dim_customer dc ON fs.Customer_ID = dc.Customer_ID
    JOIN dim_product dp ON fs.Product_ID = dp.Product_ID
    GROUP BY dp.Product_Category, dc.Occupation
) AS t
WHERE rn <= 5;

-- Q6
SELECT 
    dc.City_Category,
    dc.Marital_Status,
    dt.Month_Name,
    SUM(fs.Total_Amount) AS Revenue
FROM fact_sales fs
JOIN dim_customer dc ON fs.Customer_ID = dc.Customer_ID
JOIN dim_time dt ON fs.Time_ID = dt.Time_ID
WHERE dt.Date >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
GROUP BY dc.City_Category, dc.Marital_Status, dt.Month_Name
ORDER BY dt.Month_Name, Revenue DESC;

-- Q7
SELECT 
    dc.Stay_In_Current_City_Years,
    dc.Gender,
    AVG(fs.Total_Amount) AS Avg_Purchase
FROM fact_sales fs
JOIN dim_customer dc ON fs.Customer_ID = dc.Customer_ID
GROUP BY dc.Stay_In_Current_City_Years, dc.Gender
ORDER BY dc.Stay_In_Current_City_Years, Avg_Purchase DESC;

-- Q8
SELECT *
FROM (
    SELECT 
        dc.City_Category,
        dp.Product_Category,
        SUM(fs.Total_Amount) AS Revenue,
        ROW_NUMBER() OVER (PARTITION BY dp.Product_Category ORDER BY SUM(fs.Total_Amount) DESC) AS rn
    FROM fact_sales fs
    JOIN dim_customer dc ON fs.Customer_ID = dc.Customer_ID
    JOIN dim_product dp ON fs.Product_ID = dp.Product_ID
    GROUP BY dc.City_Category, dp.Product_Category
) AS t
WHERE rn <= 5;

-- Q9
SELECT 
    dp.Product_Category,
    dt.Month_Number,
    SUM(fs.Total_Amount) AS Revenue,
    (SUM(fs.Total_Amount) - LAG(SUM(fs.Total_Amount)) OVER (PARTITION BY dp.Product_Category ORDER BY dt.Month_Number)) / 
    LAG(SUM(fs.Total_Amount)) OVER (PARTITION BY dp.Product_Category ORDER BY dt.Month_Number) * 100 AS MoM_Growth
FROM fact_sales fs
JOIN dim_product dp ON fs.Product_ID = dp.Product_ID
JOIN dim_time dt ON fs.Time_ID = dt.Time_ID
WHERE dt.Year = 2016
GROUP BY dp.Product_Category, dt.Month_Number
ORDER BY dp.Product_Category, dt.Month_Number;

-- Q10
SELECT 
    dc.Age,
    dt.Is_Weekend,
    SUM(fs.Total_Amount) AS Revenue
FROM fact_sales fs
JOIN dim_customer dc ON fs.Customer_ID = dc.Customer_ID
JOIN dim_time dt ON fs.Time_ID = dt.Time_ID
WHERE dt.Year = 2016
GROUP BY dc.Age, dt.Is_Weekend
ORDER BY dc.Age, dt.Is_Weekend;

-- Q11
SELECT *
FROM (
    SELECT 
        dt.Month_Name,
        dt.Is_Weekend,
        fs.Product_ID,
        dp.Product_Category,
        SUM(fs.Total_Amount) AS Revenue,
        ROW_NUMBER() OVER (PARTITION BY dt.Month_Name, dt.Is_Weekend ORDER BY SUM(fs.Total_Amount) DESC) AS rn
    FROM fact_sales fs
    JOIN dim_product dp ON fs.Product_ID = dp.Product_ID
    JOIN dim_time dt ON fs.Time_ID = dt.Time_ID
    WHERE dt.Year = 2015
    GROUP BY dt.Month_Name, dt.Is_Weekend, fs.Product_ID
) AS t
WHERE rn <= 5;

-- Q12
SELECT 
    dp.Store_Name,
    dt.Quarter,
    SUM(fs.Total_Amount) AS Revenue,
    (SUM(fs.Total_Amount) - LAG(SUM(fs.Total_Amount)) OVER (PARTITION BY dp.Store_Name ORDER BY dt.Quarter_Number)) / 
    LAG(SUM(fs.Total_Amount)) OVER (PARTITION BY dp.Store_Name ORDER BY dt.Quarter_Number) * 100 AS Growth_Rate
FROM fact_sales fs
JOIN dim_product dp ON fs.Product_ID = dp.Product_ID
JOIN dim_time dt ON fs.Time_ID = dt.Time_ID
WHERE dt.Year = 2017
GROUP BY dp.Store_Name, dt.Quarter, dt.Quarter_Number
ORDER BY dp.Store_Name, dt.Quarter_Number;

-- Q13
SELECT 
    dp.Store_Name,
    dp.Supplier_Name,
    dp.Product_ID,
    dp.Product_Category,
    SUM(fs.Total_Amount) AS Revenue
FROM fact_sales fs
JOIN dim_product dp ON fs.Product_ID = dp.Product_ID
GROUP BY dp.Store_Name, dp.Supplier_Name, dp.Product_ID
ORDER BY dp.Store_Name, dp.Supplier_Name, Revenue DESC;

-- Q14
SELECT 
    dp.Product_ID,
    dp.Product_Category,
    dt.Season,
    SUM(fs.Total_Amount) AS Revenue
FROM fact_sales fs
JOIN dim_product dp ON fs.Product_ID = dp.Product_ID
JOIN dim_time dt ON fs.Time_ID = dt.Time_ID
GROUP BY dp.Product_ID, dp.Product_Category, dt.Season
ORDER BY dp.Product_ID, dt.Season;

-- Q15
SELECT 
    dp.Store_Name,
    dp.Supplier_Name,
    dt.Month_Number,
    SUM(fs.Total_Amount) AS Revenue,
    (SUM(fs.Total_Amount) - LAG(SUM(fs.Total_Amount)) OVER (PARTITION BY dp.Store_Name, dp.Supplier_Name ORDER BY dt.Month_Number)) /
    LAG(SUM(fs.Total_Amount)) OVER (PARTITION BY dp.Store_Name, dp.Supplier_Name ORDER BY dt.Month_Number) * 100 AS Revenue_Change_Percent
FROM fact_sales fs
JOIN dim_product dp ON fs.Product_ID = dp.Product_ID
JOIN dim_time dt ON fs.Time_ID = dt.Time_ID
GROUP BY dp.Store_Name, dp.Supplier_Name, dt.Month_Number
ORDER BY dp.Store_Name, dp.Supplier_Name, dt.Month_Number;

-- Q16
SELECT 
    Product_ID_1,
    Product_ID_2,
    COUNT(*) AS Frequency
FROM fact_product_association
GROUP BY Product_ID_1, Product_ID_2
ORDER BY Frequency DESC
LIMIT 5;

-- Q17
SELECT 
    dp.Store_Name,
    dp.Supplier_Name,
    dp.Product_ID,
    SUM(fs.Total_Amount) AS Revenue
FROM fact_sales fs
JOIN dim_product dp ON fs.Product_ID = dp.Product_ID
JOIN dim_time dt ON fs.Time_ID = dt.Time_ID
WHERE dt.Year = 2016
GROUP BY dp.Store_Name, dp.Supplier_Name, dp.Product_ID WITH ROLLUP
ORDER BY dp.Store_Name, dp.Supplier_Name, dp.Product_ID;

-- Q18
SELECT 
    dp.Product_ID,
    dp.Product_Category,
    SUM(CASE WHEN dt.Half_Year='H1' THEN fs.Total_Amount ELSE 0 END) AS H1_Revenue,
    SUM(CASE WHEN dt.Half_Year='H1' THEN fs.Quantity ELSE 0 END) AS H1_Quantity,
    SUM(CASE WHEN dt.Half_Year='H2' THEN fs.Total_Amount ELSE 0 END) AS H2_Revenue,
    SUM(CASE WHEN dt.Half_Year='H2' THEN fs.Quantity ELSE 0 END) AS H2_Quantity,
    SUM(fs.Total_Amount) AS Yearly_Revenue,
    SUM(fs.Quantity) AS Yearly_Quantity
FROM fact_sales fs
JOIN dim_product dp ON fs.Product_ID = dp.Product_ID
JOIN dim_time dt ON fs.Time_ID = dt.Time_ID
WHERE dt.Year = 2015
GROUP BY dp.Product_ID, dp.Product_Category
ORDER BY Yearly_Revenue DESC;

-- Q19
SELECT 
    fs.Product_ID,
    dt.Date,
    SUM(fs.Total_Amount) AS Daily_Revenue,
    daily_avg.Daily_Avg,
    CASE WHEN SUM(fs.Total_Amount) > 2*daily_avg.Daily_Avg THEN 'Spike' ELSE 'Normal' END AS Status
FROM fact_sales fs
JOIN dim_time dt ON fs.Time_ID = dt.Time_ID
JOIN (
    SELECT 
        Product_ID,
        AVG(Total_Amount) AS Daily_Avg
    FROM fact_sales fs2
    JOIN dim_time dt2 ON fs2.Time_ID = dt2.Time_ID
    GROUP BY Product_ID
) AS daily_avg ON fs.Product_ID = daily_avg.Product_ID
GROUP BY fs.Product_ID, dt.Date, daily_avg.Daily_Avg
ORDER BY fs.Product_ID, dt.Date;

-- Q20
CREATE OR REPLACE VIEW STORE_QUARTERLY_SALES AS
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
JOIN dim_product dp ON fs.Product_ID = dp.Product_ID
JOIN dim_time dt ON fs.Time_ID = dt.Time_ID
GROUP BY dp.Store_ID, dp.Store_Name, dt.Year, dt.Quarter_Number, dt.Quarter
ORDER BY dp.Store_Name, dt.Year, dt.Quarter_Number;

select * from store_quarterly_sales;