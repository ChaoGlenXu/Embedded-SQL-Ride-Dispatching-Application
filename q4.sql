-- Do drivers improve?

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO uber, public;
DROP TABLE IF EXISTS q4 CASCADE;

CREATE TABLE q4(
    type VARCHAR(9),
    number INTEGER,
    early FLOAT,
    late FLOAT
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS intermediate_step CASCADE;
DROP VIEW IF EXISTS ride_duration_tableQ4 CASCADE;
DROP VIEW IF EXISTS ride_duration_tableQ4_trained CASCADE;
DROP VIEW IF EXISTS ride_duration_tableQ4_not_trained CASCADE;
DROP VIEW IF EXISTS trained_driver_id_more_than_10_days CASCADE;
DROP VIEW IF EXISTS not_trained_driver_id_more_than_10_days CASCADE;
DROP VIEW IF EXISTS keep_trained_10_days CASCADE;
DROP VIEW IF EXISTS keep_not_trained_10_days CASCADE;
DROP VIEW IF EXISTS first_5days_trained_part CASCADE;
DROP VIEW IF EXISTS first_5days_not_trained_part CASCADE;
DROP VIEW IF EXISTS after_5days_trained CASCADE;
DROP VIEW IF EXISTS after_5days_not_trained CASCADE;
DROP VIEW IF EXISTS early_avg_trained_driver CASCADE;
DROP VIEW IF EXISTS early_avg_not_trained_driver CASCADE;
DROP VIEW IF EXISTS late_avg_trained_driver CASCADE;
DROP VIEW IF EXISTS late_avg_not_trained_driver CASCADE;
DROP VIEW IF EXISTS early_late_trained CASCADE;
DROP VIEW IF EXISTS early_late_not_trained CASCADE;



-- Define views for your intermediate steps here:
    -- from q3
    create view ride_duration_tableQ4 as 
    select rating, Pickup.request_id as Pickup_request_id , Dropoff.datetime as Dropoff_datetime, Pickup.datetime as Pickup_datetime, ClockedIn.shift_id, ClockedIn.driver_id,
    (Dropoff.datetime - Pickup.datetime) AS ride_duration
    from Dropoff, Pickup, Dispatch, ClockedIn, DriverRating
    where Dropoff.request_id = Pickup.request_id 
        and Dispatch.request_id = Pickup.request_id 
        and Dispatch.shift_id = ClockedIn.shift_id 
        and DriverRating.request_id = Pickup.request_id;

    --find the trained driver
    create view ride_duration_tableQ4_trained as
    select avg(rating) as avg_of_the_day, ( Pickup_datetime::DATE), Driver.driver_id as trained_driver_id, Driver.trained, coalesce('trained') --, count(Pickup_datetime::DATE)
    from ride_duration_tableQ4, Driver
    where ride_duration_tableQ4.driver_id = Driver.driver_id
        and Driver.trained = true
    group by  trained_driver_id, ( Pickup_datetime::DATE)
    --having count(Pickup_datetime::DATE) >= 2  -- should not add this line here
    order by (Pickup_datetime::DATE) ASC;

    --find the not trained driver
    create view ride_duration_tableQ4_not_trained as
    select avg(rating) as avg_of_the_day, ( Pickup_datetime::DATE), Driver.driver_id as not_trained_driver_id, Driver.trained, coalesce('untrained') --, count(Pickup_datetime::DATE)
    from ride_duration_tableQ4, Driver
    where ride_duration_tableQ4.driver_id = Driver.driver_id
        and Driver.trained = false
    group by not_trained_driver_id, ( Pickup_datetime::DATE)
    --having count(Pickup_datetime::DATE) >= 2   -- should not add this line here
    order by (Pickup_datetime::DATE) ASC;


   --find the trained driver more that 10 day
    create view trained_driver_id_more_than_10_days as 
    select  trained_driver_id, trained, coalesce('trained'), count(Pickup_datetime::DATE)
    from ride_duration_tableQ4_trained
    group by trained_driver_id,trained
    having count(Pickup_datetime::DATE) >= 10;


    --find the not trained driver more than 10 days
    create view not_trained_driver_id_more_than_10_days as 
    select not_trained_driver_id, trained, coalesce('untrained'), count(Pickup_datetime::DATE)
    from ride_duration_tableQ4_not_trained
    group by not_trained_driver_id, trained
    having count(Pickup_datetime::DATE) >= 10;

    --only keep the row with trained_driver_id_more_than_10_days
    create view keep_trained_10_days as 
    select avg_of_the_day, ( Pickup_datetime::DATE), trained_driver_id_more_than_10_days.trained_driver_id as the_trained_driver_id, trained_driver_id_more_than_10_days.trained, coalesce('trained')
    from trained_driver_id_more_than_10_days, ride_duration_tableQ4_trained
    where trained_driver_id_more_than_10_days.trained_driver_id = ride_duration_tableQ4_trained.trained_driver_id
    order by (Pickup_datetime::DATE) ASC;

    --only keep the row with not_trained_driver_id_more_than_10_days
    create view keep_not_trained_10_days as 
    select avg_of_the_day, ( Pickup_datetime::DATE), not_trained_driver_id_more_than_10_days.not_trained_driver_id as the_not_trained_driver_id, not_trained_driver_id_more_than_10_days.trained, coalesce('untrained')
    from not_trained_driver_id_more_than_10_days, ride_duration_tableQ4_not_trained
    where not_trained_driver_id_more_than_10_days.not_trained_driver_id = not_trained_driver_id_more_than_10_days.not_trained_driver_id
    order by (Pickup_datetime::DATE) ASC;



    

    -- early and late from keep_trained_10_days 
    create view first_5days_trained_part as 
    select * 
    from keep_trained_10_days
    fetch first 5 row only;

    -- early and late from keep_not_trained_10_days 
    create view first_5days_not_trained_part as 
    select * 
    from keep_not_trained_10_days
    fetch first 5 row only;

    -- after first 5 rows(5 day) from keep_trained_10_days
    create view after_5days_trained as 
    (select *  from keep_trained_10_days) EXCEPT (select *  from first_5days_trained_part);

    -- after first 5 rows(5 day) from keep_not_trained_10_days 
    create view after_5days_not_trained as 
    (select *  from keep_not_trained_10_days) EXCEPT (select *  from first_5days_not_trained_part);

    -- early for first 5 days tranined 
    create view early_avg_trained_driver as 
    select avg(avg_of_the_day) as early,  the_trained_driver_id 
    from first_5days_trained_part
    group by the_trained_driver_id;

    -- early for first 5 days not tranined 
    create view early_avg_not_trained_driver as 
    select avg(avg_of_the_day) as early,  the_not_trained_driver_id
    from first_5days_not_trained_part
    group by the_not_trained_driver_id;

    --late for after first 5 rows(5 day) from after_5days_trained
    create view late_avg_trained_driver as 
    select avg(avg_of_the_day) as late, the_trained_driver_id
    from after_5days_trained
    group by the_trained_driver_id;

    --late for after first 5 rows(5 day) from after_5days_not_trained
    create view late_avg_not_trained_driver as 
    select avg(avg_of_the_day) as late, the_not_trained_driver_id
    from after_5days_not_trained
    group by the_not_trained_driver_id;

    -- combine back early and late for trained
    create view early_late_trained as 
    select coalesce('trained') as type, count(*), avg(early) as early, avg(late) as late
    from early_avg_trained_driver, late_avg_trained_driver
    where early_avg_trained_driver.the_trained_driver_id = late_avg_trained_driver.the_trained_driver_id;

    -- combine back early and late for trained
    create view early_late_not_trained as 
    select coalesce('untrained') as type, count(*) as number, avg(early) as early, avg(late) as late
    from early_avg_not_trained_driver, late_avg_not_trained_driver
    where early_avg_not_trained_driver.the_not_trained_driver_id = late_avg_not_trained_driver.the_not_trained_driver_id;

    --combine the trained and untrained 
    


-- Your query that answers the question goes below the "insert into" line:
    INSERT INTO q4
    (select * from early_late_trained)
    union
    (select * from early_late_not_trained );

