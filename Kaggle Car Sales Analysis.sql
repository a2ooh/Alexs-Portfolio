-- Inspecting data

/* We can use top 20 to view all cols for the first 20 rows of the dataset
Note: in something like mysql, we can also use limit 20 after the from statement but sql server does not recognize limit
Before moving on with analysis and querying, we can first view the dataset to get an idea of what data it holds, its condition, 
and the potential use cases for it. */
select top 20 * 
FROM [Rand Table Import DB].[dbo].[sales_data_sample]


-- Checking unique values

-- We will check the unique or distinct values for certain categorical variables in the dataset
select distinct status from [Rand Table Import DB].[dbo].[sales_data_sample] -- 6 unique statuses  
select distinct year_id from [Rand Table Import DB].[dbo].[sales_data_sample] -- 3 unique years 
select distinct productline from [Rand Table Import DB].[dbo].[sales_data_sample] -- 7 unique product lines
select distinct country from [Rand Table Import DB].[dbo].[sales_data_sample] -- 19 unique countries
select distinct dealsize from [Rand Table Import DB].[dbo].[sales_data_sample] -- 3 deal sizes (small medium large)
select distinct territory from [Rand Table Import DB].[dbo].[sales_data_sample] -- 4 territories

-- Now that we have an idea of what our data set looks like, lets begin the analysis
-- Grouping sales by product line 
select productline as 'Product Line' , sum(sales) as Revenue 
from [Rand Table Import DB].[dbo].[sales_data_sample]
group by productline 
order by Revenue desc
-- Classic cars has the highest revenue, then vintage cars, and then motorcycles 

-- Lets find out which year was the best in terms of revenue 
select year_id as Year, sum(sales) as Revenue
from [Rand Table Import DB].[dbo].[sales_data_sample]
group by year_id 
order by Revenue desc
-- 2004 had the highest revenue follwed by 2003 then 2005

/* It seems strange that 2005 has such low revenue compared to the other two years. This could be 
from a lack of marketing, manufacturing issues, etc. Lets check if the company operated throughout
the whole year. */

select distinct month_id from [Rand Table Import DB].[dbo].[sales_data_sample]
where year_id = '2005'
--where year_id = '2004'
--where year_id = '2003'
/*Here we see that the company did not operate for the whole year, just the first 5 months of 2005 
compared to the whole year in 2004 and 2003. This explains the lack of revenue for 2005 */

-- Lets see which deal sizes generate the most revenue
select dealsize as 'Deal Size', sum(sales) as Revenue 
from [Rand Table Import DB].[dbo].[sales_data_sample]
group by dealsize 
order by Revenue desc
/* The medium size deals generate the most revenue with small generating the least, this could be an indication
that the company should prioritize the medium sized deals */

-- Lets check what month had the highest revenue for a specific year and how much revenue was for that month.
select month_id as Month, sum(sales) as Revenue, count(ORDERNUMBER) as Frequency
from [Rand Table Import DB].[dbo].[sales_data_sample]
--where year_id = ' 2005' 
where year_id = ' 2004'
--where year_id = ' 2003'
group by MONTH_ID
order by Revenue desc
--For both 2003 and 2004, the best month in terms of revenue and frequency of orders was month 11, presumably November.

/* Nov seems to be the best month for sales, and from earlier analysis we see that classic cars sold the most, but is 
that truly the case for Nov? Lets check.*/
select productline as 'Product Line', sum(sales) as Revenue, count(ORDERNUMBER) as Frequency
from [Rand Table Import DB].[dbo].[sales_data_sample]
where year_id = '2004' and month_id = '11'
group by month_id, productline
order by Revenue desc
-- For the month of Nov in 2004, we see that classic cars were in fact the best for revenue 

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/* Let's figure out who the best customer was using RFM (Recency-Frequency-Monetary) analysis.

RFM analysis is a marketing technique used to analyze customer behavior based on three factors: 
Recency, Frequency, and Monetary Value. These factors are used to segment customers into groups 
based on their buying habits and help businesses identify the most valuable customers and tailor 
their marketing strategies accordingly.

Recency refers to how recently a customer has made a purchase from the business. Customers who have made a purchase more 
recently are considered to be more engaged with the business.

Frequency refers to how often a customer has made a purchase from the business. Customers who make frequent purchases are 
more likely to be loyal customers.

Monetary value refers to the amount of money a customer has spent on purchases from the business. Customers who spend more 
are considered to be more valuable to the business.


For this example: 
Recency = the date of their most recent order 
Frequency = total number of orders within the year 
Monetary value = total revenue from thier orders
*/

select 
    customername, 
    sum(sales) as MonetaryValue,
    avg(sales) as AvgMonetaryValue,
    count(ORDERNUMBER) as Frequency, 
    max(orderdate) as 'Last Order Date', -- the last time the customer ordered
    (select max(orderdate) from [Rand Table Import DB].[dbo].[sales_data_sample]) as 'Max Order Date', -- the most recent date in the dataset
    datediff(DD,  max(orderdate), (select max(orderdate) from [Rand Table Import DB].[dbo].[sales_data_sample])) as Recency -- difference in days between customer last order date and max date of the dataset
from [Rand Table Import DB].[dbo].[sales_data_sample]
group by customername

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/* Now that this is working, we will split the dataset into even buckets using ntile. 

We will also created some CTEs. The common table expression (CTE) is a powerful construct in SQL that helps simplify a 
query. CTEs work as virtual tables (with records and columns), created during the execution of a query, used by the query, 
and eliminated after query execution. CTEs often act as a bridge to transform the data in source tables to the format expected 
by the query.*/

; with rfm as -- set rfm to cte using query from above 
(
    select 
        customername, 
        sum(sales) as MonetaryValue,
        avg(sales) as AvgMonetaryValue,
        count(ORDERNUMBER) as Frequency, 
        max(orderdate) as 'Last Order Date', -- the last time the customer ordered
        (select max(orderdate) from [Rand Table Import DB].[dbo].[sales_data_sample]) as 'Max Order Date', -- the most recent date in the dataset
        datediff(DD,  max(orderdate), (select max(orderdate) from [Rand Table Import DB].[dbo].[sales_data_sample])) as Recency -- difference in days between customer last order date and max date of the dataset
    from [Rand Table Import DB].[dbo].[sales_data_sample]
    group by customername
)
select r.*, -- select all from rfm aliased as r 
    ntile(4) over(order by Recency) as rfm_recency, -- create 4 buckets and order by each of our metrics
    ntile(4) over(order by Frequency) as rfm_frequency,    
    ntile(4) over(order by MonetaryValue) as rfm_monetary
from rfm as r 

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/* Now that we have calculated our rfm, we will also set that to a cte to make it temporary. So we will create the first rfm and
make our cte, then use those results when calculating our buckets and then set that query as its own cte so both are temps.*/

; with rfm as -- set rfm to cte using query from above 
(
    select 
        customername, 
        sum(sales) as MonetaryValue,
        avg(sales) as AvgMonetaryValue,
        count(ORDERNUMBER) as Frequency, 
        max(orderdate) as 'Last Order Date', -- the last time the customer ordered
        (select max(orderdate) from [Rand Table Import DB].[dbo].[sales_data_sample]) as 'Max Order Date', -- the most recent date in the dataset
        datediff(DD,  max(orderdate), (select max(orderdate) from [Rand Table Import DB].[dbo].[sales_data_sample])) as Recency -- difference in days between customer last order date and max date of the dataset
    from [Rand Table Import DB].[dbo].[sales_data_sample]
    group by customername
),
rfm_calc as -- set rfm calc to cte using ntile query 
(
    select r.*, -- select all from rfm aliased as r 
        ntile(4) over(order by Recency) as rfm_recency, -- create 4 buckets and order by each of our metrics
        ntile(4) over(order by Frequency) as rfm_frequency,    
        ntile(4) over(order by MonetaryValue) as rfm_monetary
    from rfm as r 
)
select 
    c.*, -- select all from rfm_calc as c which is using the results from the rfm cte
    rfm_recency + rfm_frequency + rfm_monetary as rfm_cell, -- adding the values in the three cols together and setting it to rfm_cell
    cast(rfm_recency as varchar) + cast(rfm_frequency as varchar) + cast(rfm_monetary as varchar) as rfm_cell_string -- same as above but the values of each are concatenated instead of added together
from rfm_calc as c 

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/* Now we have to CTEs which are temporary but to get the results we want, we need to run all lines of this query at the same time. 
To avoid having to call the CTE all the time, we will create a temporary table containing the CTEs.*/

DROP TABLE IF EXISTS #rfm -- # is a local temp and ## is a global temp. At this point #rfm should not exist, if it does we drop it and then run the script
; with rfm as -- set rfm to cte using query from above 
(
    select 
        customername, 
        sum(sales) as MonetaryValue,
        avg(sales) as AvgMonetaryValue,
        count(ORDERNUMBER) as Frequency, 
        max(orderdate) as 'Last Order Date', -- the last time the customer ordered
        (select max(orderdate) from [Rand Table Import DB].[dbo].[sales_data_sample]) as 'Max Order Date', -- the most recent date in the dataset
        datediff(DD,  max(orderdate), (select max(orderdate) from [Rand Table Import DB].[dbo].[sales_data_sample])) as Recency -- difference in days between customer last order date and max date of the dataset
    from [Rand Table Import DB].[dbo].[sales_data_sample]
    group by customername
),
rfm_calc as -- set rfm calc to cte using ntile query 
(
    select r.*, -- select all from rfm aliased as r 
        ntile(4) over(order by Recency) as rfm_recency, -- create 4 buckets and order by each of our metrics
        ntile(4) over(order by Frequency) as rfm_frequency,    
        ntile(4) over(order by MonetaryValue) as rfm_monetary
    from rfm as r 
)
select 
    c.*, -- select all from rfm_calc as c which is using the results from the rfm cte
    rfm_recency + rfm_frequency + rfm_monetary as rfm_cell, -- adding the values in the three cols together and setting it to rfm_cell
    cast(rfm_recency as varchar) + cast(rfm_frequency as varchar) + cast(rfm_monetary as varchar) as rfm_cell_string -- same as above but the values of each are concatenated instead of added together
into #rfm -- here we are creating the temp table as we run our script
from rfm_calc as c

-- Lets check if the temp table worked 
select * 
from #rfm

-- Looks like it worked. 
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/* Now we are going to add a case statement to identify the health of a customer based on rfm.*/
select customername, rfm_recency, rfm_frequency, rfm_monetary, 
    case 
        when rfm_cell_string in ( 111, 112, 121, 122, 123, 132, 211, 212, 114, 141) then 'Lost customers'
        when rfm_cell_string in (133, 134, 143, 244, 334, 343, 344) then 'Slipping customer, cannot lose'
        when rfm_cell_string in (311, 411, 331) then 'New customers'
        when rfm_cell_string in (222, 223, 233, 322) then 'Potential churner'
        when rfm_cell_string in (323, 333, 321, 422, 332, 432) then 'Active'
        when rfm_cell_string in (433, 434, 443, 444) then 'Loyal'
    end rfm_segment
from #rfm

/* Now we have a table with customers categorized by health based on frm. */

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/*Now we will look at which products are most often sold together. We will focus on orders that have been deployed or shipped to the cusotmer*/
select ordernumber, count(*) as RowNumber 
from [Rand Table Import DB].[dbo].[sales_data_sample]
where STATUS = 'Shipped'
group by ordernumber

/* We can see that an ordernumber can have multple rows assocated with it. */
select * 
from [Rand Table Import DB].[dbo].[sales_data_sample] 
where ordernumber = 10411
/* This order number in particular has 10 lines  */

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/* Some ordernumbers might have 10+ lines associated with them. We will select the order numbers with only 2 lines associated with them 
and identify which products are most often sold in pairs. We will take our first query and make it a subquery, from that list we will select
only the orders that have row count or row number as 2 */

select ordernumber
from (
    select ordernumber, count(*) as rn 
    from [Rand Table Import DB].[dbo].[sales_data_sample]
    where status = 'Shipped'
    group by ordernumber 
) as Orders 
where rn = 2

--There are 19 order numbers with total lines associated being equal to 2 

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/* To determine which products are sold together, we need the product code. We will use the query from above as a sub query*/
select productcode
from [Rand Table Import DB].[dbo].[sales_data_sample]
where ordernumber in 
    (
        select ordernumber -- this query returns 19 order numbers with 2 lines each
        from (
            select ordernumber, count(*) as rn 
            from [Rand Table Import DB].[dbo].[sales_data_sample]
            where status = 'Shipped'
            group by ordernumber 
        ) as Orders 
        where rn = 2
    )

/* Since the inner query returns 19 order numbers with 2 lines each and we want the product code for each line, we 
should expect the query above to return 38 lines */

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/* Now we will add a comma separator so that we can have all product codes on a single column and row*/

select ',' + productcode
from [Rand Table Import DB].[dbo].[sales_data_sample]
where ordernumber in 
    (
        select ordernumber -- this query returns 19 order numbers with 2 lines each
        from (
            select ordernumber, count(*) as rn 
            from [Rand Table Import DB].[dbo].[sales_data_sample]
            where status = 'Shipped'
            group by ordernumber 
        ) as Orders 
        where rn = 2
    )
    for xml path ('')

/* We now have all product codes in a single cell separated by a comma */

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/* Let's now remove the leading comma and turn this into a string using stuff() and the query from above */
select stuff( 

    (select ',' + productcode
    from [Rand Table Import DB].[dbo].[sales_data_sample]
    where ordernumber in 
        (
            select ordernumber -- this query returns 19 order numbers with 2 lines each
            from (
                select ordernumber, count(*) as rn 
                from [Rand Table Import DB].[dbo].[sales_data_sample]
                where status = 'Shipped'
                group by ordernumber 
            ) as Orders 
            where rn = 2
        ) 
        for xml path ('')), 1, 1, '')

        -- stuff(expresion, starting position, how many character to be affected, what to replace with)
        -- all we want to do with stuff in this case is remove the leading comma and change from xml to string

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/* Now we need to get our list of order numbers back so we will select that from our table, but doing so will result in the query
returning all lines from the table. We will need to alias the internal from table as p and join that on the external from table which we will call s 
where the ordernumber in both tables is the same*/
select distinct ordernumber, stuff( 

    (select ',' + productcode
    from [Rand Table Import DB].[dbo].[sales_data_sample] as P
    where ordernumber in 
        (
            select ordernumber -- this query returns 19 order numbers with 2 lines each
            from (
                select ordernumber, count(*) as rn 
                from [Rand Table Import DB].[dbo].[sales_data_sample] 
                where status = 'Shipped'
                group by ordernumber 
            ) as Orders 
            where rn = 2 -- can change this for orders with 3 lines or products purchased together, etc.
        ) 
        and P.ordernumber = S.ordernumber
        for xml path ('')), 1, 1, '') as ProductCodes

from [Rand Table Import DB].[dbo].[sales_data_sample] as S
order by ProductCodes DESC /* before this line is run we have a list of all ordernumbers where there are 2 lines and the product codes
for those lines. Anything that does not have only two lines comes up as null. So we order by product codes desc which returns
only the lines we care about first.*\

/* So now we can see in our query that order numbers 11 and 12 and 15 and 16 are different orders but the contain the same products.
This could be helpful when running some campaign where we could advertise those two products together now knowing that customers
will buy them at the same time.*\


















