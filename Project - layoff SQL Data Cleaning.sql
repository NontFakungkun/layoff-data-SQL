-- Data cleaning project

select *
from world_layoffs.layoffs;
select *
from layoffs_staging
;

-- STEPS to take
-- 1. Remove Duplicates
-- 2. Standardise the Data
-- 3. Null Values or blank values
-- 4. Remove unnecessary/irrelevant Columns

-- Duplicate the table to prevent altering the original dataset from data manipulation
create table layoffs_staging
like layoffs;

insert layoffs_staging
select * from layoffs;

-- Remove Duplicates
-- To make sure, I'll be checking for duplicates
with duplicate_findings as
(
select *,
row_number() over(partition by company, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
) select * from duplicate_findings where row_num > 1;

-- Create similar table with duplicate identifier (row_num) as an extra fields
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
  `row_num` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert layoffs_staging2
select *,
row_number() over(partition by company, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

delete from layoffs_staging2 where row_num > 1;

-- Now the duplicate row is removed from the newly created table

-- Standardise the Data
Select *
from layoffs_staging2;

-- checking for whitespaces in 'company' field
select * from layoffs where company != trim(company);
-- trim those out
update layoffs_staging2 set company = trim(company);

-- checking 'industry' field
select distinct industry 
from layoffs_staging2 
order by 1;
-- it is found that Crypto Currency has 3 similar fields with bit different wording

-- so we find the most common ones
select industry, count(*) 
from layoffs_staging2 
group by industry 
having industry like '%Crypto%';
-- So the result is shown that 'Crypto' is the most common field

-- Update the lesser similar fields into 'Crypto'
update layoffs_staging2 
set industry = 'Crypto'
where industry like '%Crypto%';

-- checking 'country' field
select distinct country from layoffs_staging2 order by 1
;
-- There are two United States fields, we will cut '.' away
update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

-- Convert 'date' field into date format
update  layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging2
modify column `date` date;

-- Null Values or blank values

-- Filling 'industry' field
select * from layoffs_staging2
where industry is null or industry = '';

-- first set empty cell into null for similarity
update layoffs_staging2
set industry = null
where industry = '';

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2 
	on t1.company = t2.company
	and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null
;

-- We then update the null industry using the existing one with similar company name
update layoffs_staging2 t1
join layoffs_staging2 t2 
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

-- Remove unnecessary/irrelevant Columns
-- There are some company with no clear layoff numbers so it is doubtful whether there is any layoff at all
select * 
from layoffs_staging
where total_laid_off is null 
and percentage_laid_off is null;

-- So we remove those rows
delete 
from layoffs_staging2 
where total_laid_off is null 
and percentage_laid_off is null;

-- Lastly, remove row_num column
alter table layoffs_staging2
drop column row_num;

select * from layoffs_staging2

-- END OF CONTENT --