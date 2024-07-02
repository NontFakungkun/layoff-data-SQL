-- Exploratory Data Analysis

select * from layoffs_staging2 where country like 'T%';


-- Checking data period
select min(`date`), max(`date`) from layoffs_staging2;

-- Maximum layoff number and percentage
select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;


-- The companies that laid off all employees, likely closing down
select *
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

-- Companies with the highest number of layoff and their industries
select company, industry, sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by company, industry
order by total_laid_off desc;

-- Industries with the highest number of layoff and their industries
select industry, sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by industry
order by total_laid_off desc;

-- Year with the highest number of layoff and their industries
select year(`date`), sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by 1
order by 2 desc;

-- stage of the company with the highest number of layoff and their industries
select stage, sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by 1
order by 2 desc;


-- Explore layoff progression by month
with Rolling_Total as
(
select substring(`date`, 1, 7) as `month`, sum(total_laid_off) as total_laid_off
from layoffs_staging2
where substring(`date`, 1, 7) is not null
group by 1
order by 1 asc
)
select `month`, total_laid_off
, sum(total_laid_off) over(order by `month`) as rolling_laid_off
from Rolling_Total
;

-- Company and year with highest layoffs
select company, year(`date`), sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by company, year(`date`)
order by 3 desc;

-- Top 5 company with most layoffs separated by years
with Company_Year (company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
), 
Company_Year_Rank as (
select *, dense_rank() over(partition by years order by total_laid_off desc) as ranking
from Company_Year
where years is not null
)
select *
from Company_Year_Rank
where ranking <= 5;
