select * from covid_19;

/* 1. Retrieve the jurisdiction residence with the highest number of COVID deaths for the latest data period end date. */

SELECT 
    Jurisdiction_Residence,
    MAX(COVID_deaths) AS Max_deaths
FROM
    covid_19
WHERE
    data_period_end = (SELECT 
            MAX(data_period_end)
        FROM
            covid_19)
GROUP BY Jurisdiction_Residence
ORDER BY Max_deaths DESC;

SELECT * FROM covid_19;

/* 2.	Calculate the week-over-week percentage change in crude COVID rate for all jurisdictions and groups,
 sorted by the highest percentage change first. */

WITH WeeklyChange AS (
  SELECT
    Jurisdiction_Residence,
    diff_group,
    data_period_end,
    LAG(crude_COVID_rate) OVER (ORDER BY data_period_end) AS previous_rate,
    crude_COVID_rate
  FROM
    covid_19
  WHERE
	Jurisdiction_Residence = 'Region 1' AND
    diff_group = 'Weekly'
)
SELECT
  Jurisdiction_Residence,
  diff_group,
  data_period_end,
  ((crude_COVID_rate - previous_rate) / previous_rate) * 100 AS WeekOverWeekPercentageChange
FROM
  WeeklyChange
WHERE
  previous_rate IS NOT NULL
ORDER BY
  WeekOverWeekPercentageChange DESC;



/* 3.	Retrieve the top 5 jurisdictions with the highest percentage difference in aa_COVID_rate compared to the 
overall crude COVID rate for the latest data period end date. */
 
SELECT 
    Jurisdiction_Residence,
    ROUND(((aa_COVID_rate - crude_COVID_rate) / crude_COVID_rate) * 100, 2) AS highest_percentage_difference
FROM
    covid_19
WHERE
    data_period_end = (SELECT 
            MAX(data_period_end)
        FROM
            covid_19)
ORDER BY highest_percentage_difference DESC
LIMIT 5;

/* 4.	Calculate the average COVID deaths per week for each jurisdiction residence and group, for the latest 4 data period end dates. */

SELECT 
    c.Jurisdiction_Residence,
    c.diff_group,
    ROUND(AVG(c.COVID_deaths)) AS avg_deaths_per_week
FROM
    covid_19 AS c
        JOIN
    (SELECT DISTINCT
        data_period_end
    FROM
        covid_19
    ORDER BY data_period_end DESC
    LIMIT 4) AS subquery ON c.data_period_end = subquery.data_period_end
        AND diff_group = 'weekly'
GROUP BY c.Jurisdiction_Residence , c.diff_group;


/* 5.	Retrieve the data for the latest data period end date, but exclude any jurisdictions that had zero COVID deaths and
 have missing values in any other column. */
 
SELECT 
    *
FROM
    covid_19
WHERE
    data_period_end = (SELECT 
            MAX(data_period_end)
        FROM
            covid_19)
        AND COVID_deaths > 0
        TAAND NOT (Jurisdiction_Residence IS NULL
        OR diff_group IS NULL
        OR data_period_start IS NULL
        OR data_period_end IS NULL
        OR COVID_pct_of_total IS NULL
        OR pct_change_wk IS NULL
        OR pct_diff_wk IS NULL
        OR crude_COVID_rate IS NULL
        OR aa_COVID_rate IS NULL);
        
        
/* 6.	Calculate the week-over-week percentage change in COVID_pct_of_total for all jurisdictions and groups, 
but only for the data period start dates after March 1, 2020. */

WITH WeeklyChange AS (
  SELECT
    Jurisdiction_Residence,
    diff_group,
    data_period_start,
    data_period_end,
    LAG(COVID_pct_of_total) OVER (PARTITION BY Jurisdiction_Residence, diff_group ORDER BY data_period_start) AS previous_pct,
    COVID_pct_of_total
  FROM
    covid_19
  WHERE
    data_period_start > '01-03-2020'
)
SELECT
  Jurisdiction_Residence,
  diff_group,
  data_period_start,
  data_period_end,
  ((COVID_pct_of_total - previous_pct) / previous_pct) * 100 AS WeekOverWeekPercentageChange
FROM
  WeeklyChange
WHERE
  previous_pct IS NOT NULL;


/* 7.	Group the data by jurisdiction residence and calculate the cumulative COVID deaths for each jurisdiction, 
but only up to the latest data period end date. */


WITH CumulativeDeaths AS (
  SELECT
    Jurisdiction_Residence,
    data_period_end,
    SUM(COVID_deaths) OVER (PARTITION BY Jurisdiction_Residence ORDER BY data_period_end) AS Cumulative_Deaths
  FROM
    covid_19
  WHERE
    data_period_end = (SELECT MAX(data_period_end) FROM covid_19)
)
SELECT
  Jurisdiction_Residence,
  MAX(data_period_end) AS Latest_Data_Period_End,
  Cumulative_Deaths
FROM
  CumulativeDeaths
GROUP BY
  Jurisdiction_Residence, Cumulative_Deaths;


/* 8.	Identify the jurisdiction with the highest percentage increase in COVID deaths from the previous week, and 
provide the actual numbers of deaths for each week. This would require a subquery to calculate the previous week's deaths. */


WITH WeeklyDeaths AS (
  SELECT
    Jurisdiction_Residence,
    data_period_start,
    COVID_deaths,
    LAG(COVID_deaths) OVER (PARTITION BY Jurisdiction_Residence ORDER BY data_period_start) AS previous_week_deaths
  FROM
    covid_19
)
SELECT
  Jurisdiction_Residence,
  data_period_start AS Current_Week_Start,
  COVID_deaths AS Current_Week_Deaths,
  previous_week_deaths AS Previous_Week_Deaths,
  ((COVID_deaths - previous_week_deaths) / previous_week_deaths) * 100 AS Percentage_Increase
FROM
  WeeklyDeaths
WHERE
  previous_week_deaths IS NOT NULL
ORDER BY
  Percentage_Increase DESC
LIMIT 1;


/* 9.	Compare the crude COVID death rates for different groups, but only for jurisdictions where
 the total number of deaths exceeds a certain threshold (e.g. 100). */
 
SELECT
    diff_group,
    SUM(COVID_deaths) AS Total_Deaths,
    ROUND((SUM(COVID_deaths) / SUM(crude_COVID_rate)) * 100, 2) AS Crude_Death_Rate
FROM
    covid_19
WHERE
    Jurisdiction_Residence IN (
        SELECT
            Jurisdiction_Residence
        FROM
            covid_19
        GROUP BY
            Jurisdiction_Residence
		HAVING
			SUM(COVID_deaths) > 100
    )
GROUP BY
    diff_group
ORDER BY
    diff_group;


 
/* 10.	Implementation of Function & Procedure-"Create a stored procedure that takes in a date range and 
calculates the average weekly percentage change in COVID deaths for each jurisdiction. The procedure should return 
the average weekly percentage change along with the jurisdiction and date range as output. 
Additionally, create a user-defined function that takes in a jurisdiction as input and 
returns the average crude COVID rate for that jurisdiction over the entire dataset. 
Use both the stored procedure and the user-defined function to compare the average weekly percentage change in COVID deaths 
for each jurisdiction to the average crude COVID rate for that jurisdiction. */

DELIMITER //

CREATE PROCEDURE CalculateAvgWeeklyPercentageChange(
    IN startDate DATE,
    IN endDate DATE
)
BEGIN
    SELECT
        Jurisdiction_Residence AS Jurisdiction,
        AVG(pct_change_wk) AS AvgWeeklyPercentageChange
    FROM
        covid_19
    WHERE
        data_period_start >= startDate
        AND data_period_end <= endDate
    GROUP BY
        Jurisdiction_Residence;
END //

DELIMITER ;

DELIMITER //
CREATE FUNCTION GetAvgCrudeCOVIDRate(jurisdiction VARCHAR(255))
RETURNS DECIMAL(10, 2)
BEGIN
    DECLARE avgRate DECIMAL(10, 2);
    SELECT
        AVG(crude_COVID_rate) INTO avgRate
    FROM
        covid_19
    WHERE
        Jurisdiction_Residence = jurisdiction;
    RETURN avgRate;
END //

DELIMITER ;

CALL CalculateAvgWeeklyPercentageChange('01-01-2022', '31-12-2022');

SELECT GetAvgCrudeCOVIDRate('Region 1');


