SELECT extract(year from measured_on) as year, station, air_attribute, avg(value) as avg_value
FROM `buenos-aires-data.air_quality.measurements`
group by air_attribute, station, year
order by year asc

SELECT extract(month from measured_on) as month, station, air_attribute, avg(value) as avg_value
FROM `buenos-aires-data.air_quality.measurements`
group by air_attribute, station, month
order by month asc

SELECT extract(day from measured_on) as day, station, air_attribute, avg(value) as avg_value
FROM `buenos-aires-data.air_quality.measurements`
group by air_attribute, station, day
order by day asc

SELECT extract(dayofweek from measured_on) -1 as day_of_week, station, air_attribute, avg(value) as avg_value
FROM `buenos-aires-data.air_quality.measurements`
group by air_attribute, station, day_of_week
order by day_of_week asc

-- SELECT extract(hour from measured_on) as hour, station, air_attribute, avg(value) as avg_value
-- FROM `buenos-aires-data.air_quality.measurements`
-- group by air_attribute, station, hour
-- order by hour asc

SELECT air_attribute, station, avg(value) as avg, max(value) as max
FROM `buenos-aires-data.air_quality.measurements`
group by station, air_attribute
