/*
Project: Data exploration using current COVID-19 data from https://ourworldindata.org/covid-deaths.

Purpose: Displaying handle of SQL language, ability to write structured queries,
and grasp of skills such as: joins, CTE's, data type conversion, windows and aggregate functions,
and creation of views and temp tables.
*/

-- Selecting relevent data to start with

SELECT location, date, new_cases, total_cases, total_deaths, population
FROM CovidDeaths$ 
WHERE continent IS NOT NULL 
ORDER BY 1, 2


-- Total Cases vs Total Deaths
-- Displays what percentage of cases resulted in death at any date within the United States

SELECT location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 AS PercentDeaths
FROM CovidDeaths$ 
WHERE location LIKE '%states'
ORDER BY 1, 2


-- Total Cases vs Population
-- Displays what percentage of the population has tested positive at any date within the United States

SELECT location, date, total_cases, population, (total_cases/population)*100 AS PercentInfected
FROM CovidDeaths$ 
WHERE location LIKE '%states'
ORDER BY 1, 2


-- Displays percentage of each country's population having tested positive from highest to lowest

SELECT location, population, MAX(total_cases) AS TotalInfected, MAX((total_cases/population)*100) AS PercentInfected
FROM CovidDeaths$ 
WHERE continent IS NOT NULL
GROUP BY location, population 
ORDER BY PercentInfected DESC


-- Displays total death count by country from highest to lowest

SELECT location, MAX(CAST(total_deaths AS INT)) AS TotalDeaths
FROM CovidDeaths$ 
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY TotalDeaths DESC


-- Displays total death count by continent from highest to lowest
-- Data set contains non continent values, which are removed

WITH NotContinents
AS (
SELECT '%income' AS MATCH
UNION ALL SELECT 'w%' AS MATCH
UNION ALL SELECT '%ion%' AS MATCH
)
SELECT location, MAX(CONVERT(INT,total_deaths)) AS TotalDeaths
FROM CovidDeaths$
LEFT JOIN NotContinents ON location LIKE NotContinents.MATCH
WHERE NotContinents.MATCH IS NULL AND continent IS NULL
GROUP BY location
ORDER BY TotalDeaths DESC


-- Total Deaths vs. Total Cases Worldwide, rolling sum over time

SELECT date, SUM(new_cases) AS total_cases, SUM(CAST(new_deaths AS INT)) AS total_deaths,
SUM(CAST(new_deaths AS INT))/SUM(new_cases)*100 AS PercentDeaths
FROM CovidDeaths$
WHERE continent IS NOT NULL
GROUP BY date
ORDER BY 1, 2


-- Number of people vaccinated by continent to date

WITH NotContinents
AS (
SELECT '%income' AS MATCH
UNION ALL SELECT 'w%' AS MATCH
UNION ALL SELECT '%ion%' AS MATCH
)
SELECT death.location, death.date, death.population, vacc.new_people_vaccinated_smoothed,
SUM(CONVERT(BIGINT,vacc.new_people_vaccinated_smoothed)) OVER (PARTITION BY death.location ORDER BY death.location, death.date)
AS PopulationVaccinated
FROM CovidDeaths$ death
LEFT JOIN NotContinents 
ON death.location LIKE NotContinents.MATCH
JOIN CovidVaccinations$ vacc
ON death.location = vacc.location
AND death.date = vacc.date
WHERE NotContinents.MATCH IS NULL AND death.continent IS NULL
ORDER BY 1, 2


-- People vaccinated by country to date
-- Utilize partitioning to calculate rolling number of total vaccinations

SELECT death.continent, death.location, death.date, death.population, vacc.new_vaccinations,
SUM(CONVERT(BIGINT,vacc.new_people_vaccinated_smoothed)) OVER (PARTITION BY death.location ORDER BY death.location, death.date)
AS PopulationVaccinated
FROM CovidDeaths$ death
JOIN CovidVaccinations$ vacc
ON death.location = vacc.location
AND death.date = vacc.date
WHERE death.continent IS NOT NULL
ORDER BY 2, 3


-- Total People Vaccinated vs. Population by country to date
-- Utilize CTE to perform calculations on partition in previous query

WITH PercentVaccinations (continent, location, date, population, new_people_vaccinated_smoothed, VaccinationsToDate)
AS (
SELECT death.continent, death.location, death.date, death.population, vacc.new_people_vaccinated_smoothed,
SUM(CONVERT(BIGINT,vacc.new_people_vaccinated_smoothed)) OVER (PARTITION BY death.location ORDER BY death.location, death.date)
AS VaccinationsToDate 
FROM CovidDeaths$ death
JOIN CovidVaccinations$ vacc
ON death.location = vacc.location
AND death.date = vacc.date
WHERE death.continent IS NOT NULL
)
SELECT *, (VaccinationsToDate/population)*100 AS PercentVaccinated
FROM PercentVaccinations


--Utilize temp table to perform same calculation on partition in previous query

DROP TABLE IF EXISTS #PercentPopulationVaccinated
CREATE TABLE #PercentPopulationVaccinated (
continent NVARCHAR(255),
location NVARCHAR(255),
date DATETIME,
population NUMERIC,
new_people_vaccinated_smoothed NUMERIC,
PeopleVaccinated NUMERIC
)

INSERT INTO #PercentPopulationVaccinated
SELECT death.continent, death.location, death.date, death.population, vacc.new_people_vaccinated_smoothed,
SUM(CONVERT(BIGINT,vacc.new_people_vaccinated_smoothed)) OVER (PARTITION BY death.location ORDER BY death.location, death.date)
AS PeopleVaccinated 
FROM CovidDeaths$ death
JOIN CovidVaccinations$ vacc
ON death.location = vacc.location
AND death.date = vacc.date
WHERE death.continent IS NOT NULL

SELECT *, (PeopleVaccinated/population)*100 AS PercentVaccinated
FROM #PercentPopulationVaccinated

-- Creation of Views for visualizations in Tableau

-- People vaccinated by country

CREATE VIEW VaccinationsByCountry AS
SELECT death.continent, death.location, death.date, death.population, vacc.new_people_vaccinated_smoothed,
SUM(CONVERT(BIGINT,vacc.new_people_vaccinated_smoothed)) OVER (PARTITION BY death.location ORDER BY death.location, death.date)
AS PeopleVaccinated
FROM CovidDeaths$ death
JOIN CovidVaccinations$ vacc
ON death.location = vacc.location
AND death.date = vacc.date
WHERE death.continent IS NOT NULL


-- People vaccinated by continent

CREATE VIEW VaccinationsByContinent AS
WITH NotContinents
AS (
SELECT '%income' AS MATCH
UNION ALL SELECT 'w%' AS MATCH
UNION ALL SELECT '%ion%' AS MATCH
)
SELECT death.location, death.date, death.population, vacc.new_people_vaccinated_smoothed,
SUM(CONVERT(BIGINT,vacc.new_people_vaccinated_smoothed)) OVER (PARTITION BY death.location ORDER BY death.location, death.date)
AS PopulationVaccinated
FROM CovidDeaths$ death
LEFT JOIN NotContinents 
ON death.location LIKE NotContinents.MATCH
JOIN CovidVaccinations$ vacc
ON death.location = vacc.location
AND death.date = vacc.date
WHERE NotContinents.MATCH IS NULL AND death.continent IS NULL


-- Total deaths by country

CREATE VIEW DeathsByCountry AS
SELECT continent, location, date, new_deaths, total_deaths
FROM CovidDeaths$
WHERE continent IS NOT NULL


-- Total deaths by continent

CREATE VIEW DeathsByContinent AS
WITH NotContinents
AS (
SELECT '%income' AS MATCH
UNION ALL SELECT 'w%' AS MATCH
UNION ALL SELECT '%ion%' AS MATCH
)
SELECT location, date, total_deaths
FROM CovidDeaths$
LEFT JOIN NotContinents ON location LIKE NotContinents.MATCH
WHERE NotContinents.MATCH IS NULL AND continent IS NULL
