create table dateDimension as
select
CAST(TO_CHAR(genDate, 'yyyyMMdd') AS INTEGER) id,
TO_CHAR(genDate, 'yyyy-MM-dd') formattedDate,
TO_CHAR(genDate, 'yyyy-MM')  YearMonthNum,
CONCAT('QUARTER ', EXTRACT(QUARTER from genDate)) Calendar_Quarter,
TO_CHAR(genDate, 'MM') MonthNum,
TO_CHAR(genDate, 'MONTH') MonthName,
TO_CHAR(genDate, 'MON') MonthShortName,
CAST(EXTRACT(WEEK from genDate) AS INTEGER) WeekNum,
CAST(EXTRACT('doy' from genDate)AS INTEGER)  DayNumOfYear,
CAST(EXTRACT('day' from genDate)AS INTEGER)  DayNumOfMonth,
CAST(EXTRACT('dow' from genDate)AS INTEGER)  DayNumOfWeek,
TO_CHAR(genDate, 'DAY') DayName,
TO_CHAR(genDate, 'DY') DayShortName
from (select generate_series as genDate from
 generate_series(date '2000-01-01', date '2100-01-01', interval '1 day')) generatedDates;