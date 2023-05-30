-- Covid Deaths and Vaccinations Analysis
--First lets make sure that we have both of our tables: covid deaths and vaccinations 
select * 
from [Rand Table Import DB]..['Covid Deaths$']
order by location, date

select * 
from [Rand Table Import DB]..['Covid Vaccinations$']
order by location, date

-- Select data that we will be using for analysis
select location, date, total_cases, new_cases, total_deaths, population
from [Rand Table Import DB]..['Covid Deaths$']
order by location, date 

-- Total cases vs total death
-- Have to convert total_deaths and total_cases to decimal for percentage since one of them was nvarchar before 
-- Mortality rate for those that contracted covid
select location, date, total_cases, total_deaths, (convert(decimal(18,2),total_deaths) / convert(decimal(18,2), total_cases))*100 as DeathPercentage 
from [Rand Table Import DB]..['Covid Deaths$']
where location like '%states%'
order by location, date 

-- Total cases vs population 
-- Shows percentage of pop that got covid
select location, date, total_cases, population, (convert(decimal(18,2),total_cases) / convert(decimal(18,2),population))*100 as InfectionRate 
from [Rand Table Import DB]..['Covid Deaths$']
--where location like '%states%'
order by location, date

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- What countries have the highest infection rate compared to pop?
select location, population, Max(total_cases) as HighestInfectionCount, Max((convert(decimal(18,2), total_cases))/(convert(decimal(18,2), population)))*100 as InfectionRate 
from [Rand Table Import DB]..['Covid Deaths$']
-- where location = 'Afghanistan'
group by location, population
order by InfectionRate desc

/* We are getting an error here. It seems like the infection rate is correct since we compared to previous queries and got the same result (check one country in particular)
but the highest infection count is not correct.

MAX() does not seem to be giving us the correct solution so we will try another method to get the highest infection count for the location along with the infection rate*/

select location, population, total_cases as HighestInfectionCount, Max((convert(decimal(18,2), total_cases))/(convert(decimal(18,2), population)))*100 as InfectionRate
from [Rand Table Import DB]..['Covid Deaths$'] as t1
where total_cases = (
    select max(total_cases)
    from [Rand Table Import DB]..['Covid Deaths$'] as t2
    where t1.location = t2.location 
)
group by location, population, total_cases
order by InfectionRate DESC

/* This seems to be giving the correct solution now. Above we selected the data we wanted, then instead of using the MAX() function, we use a subquery to select the 
max(total_cases) where the two tables t1 and t2 are the same on the location column. Then we do our grouping and ordering. 

So we can now see the countries that have the highest rate of infection in comparison to their populations*/

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- This will now show the countries with the highest death count compared to population 
select location, max(cast(Total_deaths as int)) as TotalDeathCount
from [Rand Table Import DB]..['Covid Deaths$']
-- where location like '%states'
group by location 
order by TotalDeathCount desc

/* The above gives what we want but is also grouping certain locations by continent such as NA or Europe which we don't want. This is happening because sometimes the continent
column is empty or NULL and the continent instead is in the location column. So we only want the total deaths for locations where the continent column is not null

Above we also ran into an issue with the total_deaths column which is nvarchar, so we cast it as int instead of converting like before */ 

select location, max(cast(Total_deaths as int)) as TotalDeathCount
from [Rand Table Import DB]..['Covid Deaths$']
where continent is not null 
group by location 
order by TotalDeathCount DESC
 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Now let's look at things by continents
select continent, max(cast(Total_deaths as int)) as TotalDeathCountbyContinent
from [Rand Table Import DB]..['Covid Deaths$']
where continent is not null 
group by continent
order by TotalDeathCountbyContinent desc

-- Let's check for where location is null
select location, max(cast(Total_deaths as int)) as TotalDeathCountNullLocation 
from [Rand Table Import DB]..['Covid Deaths$']
where continent is null 
group by location 
order by TotalDeathCountNullLocation

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Let''s look at global numbers 
select sum(new_cases) as total_cases, sum(cast(new_deaths as int)) as total_deaths, sum(cast(new_deaths as int))/sum(new_cases)*100 as DeathPercentage 
from [Rand Table Import DB]..['Covid Deaths$']
-- where location like '%states%'
where continent is not null
-- group by date 
order by 1,2
-- Here we can see that across the world, the death percentages is about 0.90523%

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Let's go back and look at the Covid Vaccinations table 
select * 
from [Rand Table Import DB]..['Covid Vaccinations$']

-- Now let's join the two tables 
select * 
from [Rand Table Import DB]..['Covid Deaths$'] as dea 
join [Rand Table Import DB]..['Covid Vaccinations$'] as vac
    on dea.location = vac.location and dea.date = vac.date

-- Now let's look at populations vs vaccination counts
select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
from [Rand Table Import DB]..['Covid Deaths$'] as dea
join [Rand Table Import DB]..['Covid Vaccinations$'] as vac
    on dea.location = vac.location and dea.date = vac.[date]
where dea.continent is not null
order by 2,3

-- To make this easier to understand, we can use a window function to see the totals
select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
    sum(cast(vac.new_vaccinations as int)) over (partition by dea.location order by dea.location, dea.date) as RollingVaccinationsCount
     -- Can also use SUM(CONVERT(INT,vac.new_vaccinations))
     -- Above we need to make sure to order by date and location to get the rolling total per location 
from [Rand Table Import DB]..['Covid Deaths$'] as dea
join [Rand Table Import DB]..['Covid Vaccinations$'] as vac
    on dea.location = vac.location and dea.date = vac.[date]
where dea.continent is not null
order by 2,3

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/* Now that we have a rolling total, we want to use the largest number in the RollingVaccinationsCount column we just created
to see what percentage of the population is vaccinated but we cannot do this because we have an unsaved column, so we either 
need to use a CTE or a temp table. Let's use a CTE*/

With PopvsVac (Continent, Location, Date, Population, New_Vaccinations, RollingVaccinationsCount) as 
( 
    --When making a CTE, make sure the number of columns is the same as in your inner query
select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
    sum(cast(vac.new_vaccinations as bigint)) over (partition by dea.location order by dea.location, dea.date) as RollingVaccinationsCount
     -- Can also use SUM(CONVERT(INT,vac.new_vaccinations))
     -- Above we need to make sure to order by date and location to get the rolling total per location 
from [Rand Table Import DB]..['Covid Deaths$'] as dea
join [Rand Table Import DB]..['Covid Vaccinations$'] as vac
    on dea.location = vac.location and dea.date = vac.[date]
where dea.continent is not null
-- order by 2,3 -- Can't have the order by clause in here
)
select *, (RollingVaccinationsCount/Population)*100 as RollingPercentOfVaccinatedPopulation
from PopvsVac

/* This gives a rolling percentage of the population that is vaccinated. The populations should remain the same, but as new
vaccinations are given, the percentage should increase.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Let's try the same thing but using a temp table*/

Drop table if exists #PercentPopulationVaccinated --in case we make changes to the table, we can run everything again
Create table #PercentPopulationVaccinated
(
-- Here we need to specify column names and data types
Continent NVARCHAR(255),
Location NVARCHAR(255),
Date datetime,
Population numeric,
New_Vacciantions numeric,
RollingVaccinationsCount numeric
)
Insert into #PercentPopulationVaccinated
select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
    sum(cast(vac.new_vaccinations as bigint)) over (partition by dea.location order by dea.location, dea.date) as RollingVaccinationsCount
    -- Can also use SUM(CONVERT(INT,vac.new_vaccinations))
    -- Above we need to make sure to order by date and location to get the rolling total per location 
from [Rand Table Import DB]..['Covid Deaths$'] as dea
join [Rand Table Import DB]..['Covid Vaccinations$'] as vac
    on dea.location = vac.location and dea.date = vac.[date]
where dea.continent is not null
-- order by 2,3 -- Can't have the order by clause in here

select *--, (RollingVaccinationsCount/Population)*100 as RollingPercentOfVaccinatedPopulation
from #PercentPopulationVaccinated

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Creating a view to store data for later visualizations

Create View PercentPopulationVaccinated as
select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
    sum(cast(vac.new_vaccinations as bigint)) over (partition by dea.location order by dea.location, dea.date) as RollingVaccinationsCount
    -- Can also use SUM(CONVERT(INT,vac.new_vaccinations))
    -- Above we need to make sure to order by date and location to get the rolling total per location 
from [Rand Table Import DB]..['Covid Deaths$'] as dea
join [Rand Table Import DB]..['Covid Vaccinations$'] as vac
    on dea.location = vac.location and dea.date = vac.date
where dea.continent is not null
-- order by 2,3 -- Can't have the order by clause in here

-- We can now use this view the same way we would any other table but it contains the data that we queried in temporary solutions above.
select * 
from PercentPopulationVaccinated