-- SQL Data Cleaning
-- Data source -  https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- CREATE DATABASE public_layoffs
-- USE public_layoffs#
-- import csv file as table "layoffs"


SELECT *
FROM layoffs;
-- Things to do
-- 0. Create a staging area - a copied data to use instead of using the main data
-- 1. Remove Duplicates
-- 2. Standardize the data
-- 3. Null values or Blsnk values
-- 4. Remove unwanted columns

-- 0
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * 
FROM layoffs_staging;

INSERT layoffs_staging
SELECT * 
FROM layoffs;

-- 1. Remove duplicate 
-- first check for duplicates

SELECT *,
ROW_NUMBER()OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) As row_num 
FROM layoffs_staging;

-- CTE

WITH duplicate_cte AS (
SELECT *,
ROW_NUMBER()OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) As row_num 
FROM layoffs_staging
)

select * 
FROM duplicate_cte
WHERE row_num > 1;
 
-- To check if they are duplicates

SELECT *
FROM layoffs_staging
where company = 'Yahoo'; -- 'Casper'

-- create same table with the row_num to enable deletion
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER()OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) As row_num 
FROM layoffs_staging;

SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;


-- 2 STANDARDIZING DATA

SELECT * 
FROM layoffs_staging2;

-- if we look at company industry it looks like we have some spaces at the beginning of some company names, null and empty rows, let's take a look at these
SELECT company, Trim(company)
FROM layoffs_staging2;
-- To trim the spaces of company
UPDATE layoffs_staging2
set company = TRIM(company);


SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';
-- notice the presence of a NUll values and difference nmaes of Crypto industry, we need to update all to just crypto

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';


SELECT distinct country
FROM layoffs_staging2
WHERE country LIKE 'United%';

UPDATE layoffs_staging2
SET country = Trim(TRAILING '.' FROM country)
WHERE country LIKE 'United%';


-- Let's also fix the date columns since its in text format:
SELECT date
FROM layoffs_staging2;

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- we can use str to date to update this field
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- now we can convert the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- 3. Look at Null Values and blank and possibly remove those that are needed to be removed

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- let's take a look at these
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'airbnb%';

-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

-

-- now if we check those are all null

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- set blank to null
UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = '';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- now we need to populate those nulls if possible

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL )
AND t2.industry IS NOT NULL;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;


SELECT *
FROM layoffs_staging2;


-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase
-- so there isn't anything I want to change with the null values


-- 4. remove any columns and rows we need to

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL;


SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


-- Recheck the data
SELECT * 
FROM layoffs_staging2;

-- delete the row_num column created earlier
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


