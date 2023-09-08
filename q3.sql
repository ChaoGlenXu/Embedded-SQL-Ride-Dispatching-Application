-- Rest bylaw.

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO uber, public;
DROP TABLE IF EXISTS q3 CASCADE;

CREATE TABLE q3(
    driver_id INTEGER,
    start DATE,
    driving INTERVAL,
    breaks INTERVAL
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS intermediate_step CASCADE;
DROP VIEW IF EXISTS ride_duration_table CASCADE;
DROP VIEW IF EXISTS total_ride_12hours_of_each_day CASCADE;
DROP VIEW IF EXISTS breakTable_no_minPickupYet CASCADE;
DROP VIEW IF EXISTS oneDropoff1day CASCADE;
DROP VIEW IF EXISTS breakTable CASCADE;
DROP VIEW IF EXISTS breaktable_or_0breakDuration CASCADE;
DROP VIEW IF EXISTS maxBreakLessThan15 CASCADE;
DROP VIEW IF EXISTS intersect12and15 CASCADE;
DROP VIEW IF EXISTS ThreeDays CASCADE;
DROP VIEW IF EXISTS add3days CASCADE;
DROP VIEW IF EXISTS the_3days CASCADE;

-- Define views for your intermediate steps here:

    -- select Pickup.request_id, Dropoff.datetime, Pickup.datetime, DATEDIFF(Second, Pickup.datetime, Dropoff.datetime) AS ride_duration
    -- 12 hours = 43200 seconds = 720 minutes
    create view ride_duration_table as 
    select Pickup.request_id as Pickup_request_id , Dropoff.request_id as Dropoff_request_id, Dropoff.datetime as Dropoff_datetime, Pickup.datetime as Pickup_datetime, ClockedIn.shift_id, ClockedIn.driver_id,
    (Dropoff.datetime - Pickup.datetime) AS ride_duration
    from Dropoff, Pickup, Dispatch, ClockedIn
    where Dropoff.request_id = Pickup.request_id 
        and Dispatch.request_id = Pickup.request_id 
        and Dispatch.shift_id = ClockedIn.shift_id;
    
    -- currently incorrect -- put this on hold to work on the part two for now
    -- need to group by driver_id, datetime::DATE
    create view total_ride_12hours_of_each_day as
    select   driver_id, Dropoff_datetime::DATE as specific_day, sum(ride_duration) as DurationTheDay
    from ride_duration_table
    group by driver_id, specific_day
    having sum(ride_duration) >= ('12:00:00'::interval); -- sum(ride_duration) > 12 hours
       
    -- break table -- this table give the driver_ids , Note that the drivers here have at least one break since two tables fillter
    create view breakTable_no_minPickupYet as 
    select T1.Dropoff_request_id as T1_Dropoff_request_id, (T2.Pickup_datetime - T1.Dropoff_datetime) as break_duration, T1.Dropoff_datetime as T1_Dropoff_datetime, 
        T2.Pickup_datetime as T2_Pickup_datetime, T1.driver_id as driver_id
    from ride_duration_table T1, ride_duration_table T2
    where T1.driver_id = T2.driver_id and T1.Dropoff_datetime < T2.Pickup_datetime and T1.Pickup_request_id != T2.Pickup_request_id
        and T2.Pickup_datetime::DATE = T1.Dropoff_datetime::DATE
    --group by T1_Dropoff_datetime
    ;  --having min(T2_Pickup_datetime) >= 15

/*
    --alternative driver had ride on that day (based on the pickup date)
    create view breakTable_no_minPickupYet_0breakDuration as 
    select T1.Dropoff_request_id as T1_Dropoff_request_id, '0'::interval as The_break_duration, T1.Dropoff_datetime as T1_Dropoff_datetime, 
        T2.Pickup_datetime as T2_Pickup_datetime, T1.driver_id as driver_id
    from ride_duration_table T1, ride_duration_table T2
    where T1.driver_id = T2.driver_id and T1.Dropoff_datetime < T2.Pickup_datetime and T1.Pickup_request_id != T2.Pickup_request_id
        and T2.Pickup_datetime::DATE != T1.Dropoff_datetime::DATE  
    ;  
*/

    --only one dropoff with 1 day
    create view oneDropoff1day as 
    select count(Dropoff_request_id) , ride_duration_table.Dropoff_datetime::DATE as Dropoff_datetime,
        '0'::interval as The_break_duration, driver_id       
    from ride_duration_table
    group by driver_id, ride_duration_table.Dropoff_datetime::DATE
    having count(Dropoff_request_id) = 1
    ;

/*
    --driver had ride on that day (based on the pickup date)
    create view driver_ride_1day as 
    select driver_id, Pickup_datetime::DATE
    from  ride_duration_table
    group by driver_id, Pickup_datetime::DATE

    --driver had ride but never had a break on that day = driver had ride on that day - breakTable_no_minPickupYet
    create view driver_ride_1day_NO_break_atAll as
    (select driver_id 
    from 

    )
*/

    create view breakTable as 
    select min(break_duration) as The_break_duration, T1_Dropoff_datetime, 
        min(T2_Pickup_datetime) as earliest_pickup, driver_id
    from breakTable_no_minPickupYet    
    group by T1_Dropoff_request_id, driver_id, T1_Dropoff_datetime
    ;

    --sum up the break of the day breaktable_or_0breakDuration
    create view breaktable_or_0breakDuration as
    (select The_break_duration, driver_id, T1_Dropoff_datetime from breakTable)
    union
    (select The_break_duration, driver_id, oneDropoff1day.Dropoff_datetime as T1_Dropoff_datetime from oneDropoff1day)
    ;

    -- break that to find the max of break each driver
    -- for the purpose: if the max break < 15 mins, then  selected 
    create view maxBreakLessThan15 as 
    select sum(The_break_duration) as breaks_sum_1day, max(The_break_duration) as maxBreak, driver_id, T1_Dropoff_datetime::DATE as specific_day
    from breaktable_or_0breakDuration
    group by driver_id, specific_day
    having max(The_break_duration) <= ('0:15:00'::interval) -- maxBreak < 15:00 minutes
    ;



    --intersection between total_ride_12hours_of_each_day and maxBreakLessThan15 since "yet"
    -- only for each day
    create view intersect12and15 as
    select maxBreakLessThan15.driver_id as driver_id, maxBreakLessThan15.specific_day as the_day, breaks_sum_1day, DurationTheDay, maxBreakLessThan15.specific_day
    from total_ride_12hours_of_each_day, maxBreakLessThan15
    where total_ride_12hours_of_each_day.driver_id = maxBreakLessThan15.driver_id
        and total_ride_12hours_of_each_day.specific_day = maxBreakLessThan15.specific_day
    ;

    -- three consecutive days  --question: can I have two min() in 
    create view ThreeDays as 
    select T1.specific_day as day1, min(T2.specific_day) as day2, min(T3.specific_day) as day3, T1.driver_id as driver_id,
        T1.specific_day as start --, (T1.breaks_sum_1day + T2.breaks_sum_1day + T3.breaks_sum_1day) as breaks, 
        --(T1.DurationTheDay + T2.DurationTheDay + T3.DurationTheDay) as driving
    from intersect12and15 as T1, intersect12and15 as T2, intersect12and15 as T3
    where T1.driver_id = T2.driver_id and T2.driver_id = T3.driver_id
        and T1.specific_day < T2.specific_day and T2.specific_day < T3.specific_day
    group by T1.specific_day, T1.driver_id
    ;

    --add up the 3 days's  breaks_sum_1day and DurationTheDay
    create view add3days as 
    select T1.specific_day as day1, T2.specific_day as day2, T3.specific_day as day3, T1.driver_id as driver_id,
        T1.specific_day as start , (T1.breaks_sum_1day + T2.breaks_sum_1day + T3.breaks_sum_1day) as breaks, 
        (T1.DurationTheDay + T2.DurationTheDay + T3.DurationTheDay) as driving
    from intersect12and15 as T1, intersect12and15 as T2, intersect12and15 as T3
    where T1.driver_id = T2.driver_id and T2.driver_id = T3.driver_id
        and T1.specific_day < T2.specific_day and T2.specific_day < T3.specific_day
    ; 

    -- combine the ThreeDays and add3days
    create view the_3days as 
    select T1.driver_id, T1.start, T2.driving, T2.breaks
    from ThreeDays T1, add3days T2
    where T1.driver_id = T2.driver_id and T1.day1 = T2.day1 and T1.day2 = T2.day2 and T1.day3 = T2.day3
    ;
    --create view selected3days as 
    --select day1, min(T2_specific_day) as day2, min(T3_specific_day) as day3, driver_id, start, 
    --from ThreeDays
    --group by T1.specific_day, T1.driver_id
    --;

    
-- Your query that answers the question goes below the "insert into" line:
    INSERT INTO q3
    (select driver_id, start, driving, breaks
    from the_3days
    );