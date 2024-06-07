-- CREATE DATABASE IF NOT EXISTS WalmartSalesData;

CREATE TABLE IF NOT EXISTS Sales(
	invoice_id VARCHAR(30) not null primary key,
     branch VARCHAR(5) NOT NULL, -- not null used to prevent empty data, helps in data cleaning/wrangling
    city VARCHAR(30) NOT NULL,
    customer_type VARCHAR(30) NOT NULL,
    gender VARCHAR(30) NOT NULL,
    product_line VARCHAR(100) NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    quantity INT NOT NULL,
    VAT FLOAT(6,4) NOT NULL,
    total DECIMAL(12, 4) NOT NULL,
    date DATETIME NOT NULL,
    time TIME NOT NULL,
    payment_type VARCHAR(15) NOT NULL,
    cogs DECIMAL(10,2) NOT NULL,
    gross_margin_pct FLOAT(11,9),
    gross_income DECIMAL(12, 4),
    rating FLOAT(2, 1)
);

-- ----------------------------------------------------------------------------------------------------------------------
-- Feature Engineering 
-- Creating a new column

-- time_of_day - to determing if its morning, Afternoon or evening.

select time,
	(CASE
		WHEN `time` Between '00:00:00' AND '11:59:59' then 'Morning'
		WHEN `time` Between '12:00:00' AND '16:00:00' then 'Afternoon'
        else 'Evening'
	end
    ) as time_of_date
from sales;

alter table sales add column time_of_day varchar(20);

-- to update a table

UPDATE sales
set time_of_day = 	(CASE
		WHEN `time` Between '00:00:00' AND '11:59:59' then 'Morning'
		WHEN `time` Between '12:00:00' AND '16:00:00' then 'Afternoon'
        else 'Evening'
	end
    );
    
    
-- day_name
SELECT 
	date,
    DAYNAME(date) 
    FROM sales;
    
    
ALTER TABLE sales add column day_name varchar(10);

update sales
set day_name = (DAYNAME(date));


-- month_name
select 
	date,
    monthname(date) AS month_name
from sales;

alter table sales add column month_name varchar(20);

update sales
set month_name = (Monthname(date));

-- ---------------------------------------------------------------------------------------------------------------------

--  -----------------------------------------------
-- Exploratory Data Analysis -------------------------------------------------------------------------------------------
-- Generic question - ----------------------

-- How many unique city does walmart data have?

select distinct city from sales;

-- How many branches do you have
select distinct branch from sales;

select distinct city, branch
from sales;


-- -------------------------------------------------------------------------------------------------------
-- Product Question---------------------------------------------------------------------------------------
-- How many uniques product lines does the data have
select count(distinct product_line)
from sales;

-- Most common payment method
select payment_type ,count(payment_type) as numbers
from sales
group by payment_type
order by numbers desc;

-- What is the most selling product line? 
select  product_line,  count(product_line) as Qty
from sales
group by product_line
order by Qty desc;

-- What is the total revenue by month
select 
month_name as Month, sum(total) as Revenue
from sales
group by Month
order by Revenue desc;

-- What month had the largest COGS?
 select 
 month_name as Month, sum(cogs) as cogs
 from sales
 group by Month
 order by cogs desc;
 
 -- What product line had the largest revenue?
select  product_line,  sum(total) as total_revenue
from sales
group by product_line
order by total_revenue desc;

-- What is the city with the largest revenue?
select city, sum(total) as Total_revenue
from sales
group by city
order by Total_revenue desc;

-- What product line had the largest VAT?
select product_line, avg(VAT) as Avg_VAT
from sales
group by product_line
order by Avg_VAT desc;

-- Which branch sold more products than average product sold?
select branch,
		sum(quantity) as Qty
from sales
group by branch
Having Sum(quantity) > (select avg(quantity) from sales);

-- What is the most common product line by gender?
select gender, count(gender) as count_by_gender, product_line
from sales
group by product_line, gender
order by count_by_gender desc;

-- What is the average rating of each product line?
select product_line, round(avg(rating), 2) as Avg_rating
from sales
group by product_line
order by Avg_rating desc;

-- Fetch each product line and add a column to those product line showing "Good", "Bad". Good if its greater than average sales
SELECT 
	 AVG(quantity) AS avg_qnty
FROM sales;

SELECT
	product_line,
	CASE
		WHEN AVG(quantity) > 6 THEN "Good"
        ELSE "Bad"
    END AS remark
FROM sales
GROUP BY product_line;


-- ---------------------------------------------------------------------------------------------------------------------
-- 2 --SALES-------------------------------------------------------------------------------------------------------------
-- Number of sales made in each time of the day per weekday
select time_of_day,
		count(*)as total_sales
from sales
where day_name = 'Monday'
group by time_of_day
order by total_sales desc;

--  Which of the customer type brings the most revenue?
SELECT customer_type, sum(total) AS Revenue
FROM sales
GROUP BY customer_type
ORDER BY Revenue DESC;

-- Which city has the largest tax percent/ VAT (Value Added Tax)?
select city, AVG(VAT) as VAT
from sales
group by city
order by VAT desc;

-- Which customer type pays the most in VAT?
select customer_type, AVG(VAT) as VAT
from sales
group by customer_type
order by VAT desc;


-- --------------------------------------------------------------------
-- --------------------------3  Customers -------------------------------
-- --------------------------------------------------------------------

-- How many unique customer types does the data have?
SELECT
	DISTINCT customer_type
FROM sales;

-- How many unique payment methods does the data have?
SELECT
	DISTINCT payment_type
FROM sales;


-- What is the most common customer type?
SELECT
	customer_type,
	count(*) as count
FROM sales
GROUP BY customer_type
ORDER BY count DESC;

-- Which customer type buys the most?
SELECT
	customer_type,
    COUNT(*)
FROM sales
GROUP BY customer_type;


-- What is the gender of most of the customers?
SELECT
	gender,
	COUNT(*) as gender_cnt
FROM sales
GROUP BY gender
ORDER BY gender_cnt DESC;

-- What is the gender distribution per branch?
SELECT
	gender,
	COUNT(*) as gender_cnt
FROM sales
WHERE branch = "C"
GROUP BY gender
ORDER BY gender_cnt DESC;
-- Gender per branch is more or less the same hence, I don't think has
-- an effect of the sales per branch and other factors.

-- Which time of the day do customers give most ratings?
SELECT
	time_of_day,
	AVG(rating) AS avg_rating
FROM sales
GROUP BY time_of_day
ORDER BY avg_rating DESC;
-- Looks like time of the day does not really affect the rating, its
-- more or less the same rating each time of the day.alter


-- Which time of the day do customers give most ratings per branch?
SELECT
	time_of_day,
	AVG(rating) AS avg_rating
FROM sales
-- WHERE branch = "B"
GROUP BY time_of_day
ORDER BY avg_rating DESC;
-- Branch A and C are doing well in ratings, branch B needs to do a 
-- little more to get better ratings.


-- Which day fo the week has the best avg ratings?
SELECT
	day_name,
	AVG(rating) AS avg_rating
FROM sales
GROUP BY day_name 
ORDER BY avg_rating DESC;
-- Mon, Tue and Friday are the top best days for good ratings
-- why is that the case, how many sales are made on these days?



-- Which day of the week has the best average ratings per branch?
SELECT 
	day_name,
	COUNT(day_name) total_sales
FROM sales
WHERE branch = "C"
GROUP BY day_name
ORDER BY total_sales DESC;

-- LIKE, BETWEEN, AND, LIMIT-------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------
-- And
select invoice_id, branch, product_line, unit_price, payment_type
from sales
where unit_price >= 20 AND payment_type = 'Cash';

-- Between
select invoice_id, branch, product_line, unit_price, payment_type
from sales
where unit_price between 20 AND 25;

-- In and Limit
select invoice_id, branch, product_line, unit_price, payment_type
from sales
where product_line IN ('Sports and travel', 'Fashion accessories')
limit 10;

-- Offset
select invoice_id, branch, product_line, unit_price, payment_type
from sales
where product_line IN ('Sports and travel', 'Fashion accessories')
limit 10
offset 2;

-- LIKE
select invoice_id, branch, product_line, unit_price, payment_type
from sales
where product_line LIKE '%and lifestyle'
