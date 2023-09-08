-- Months.

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO uber, public;
DROP TABLE IF EXISTS q1 CASCADE;

CREATE TABLE q1
(
    client_id INTEGER,
    email     VARCHAR(30),
    months    INTEGER
);

-- Do this for each of the views that define your intermediate steps.
-- (But give them better names!) The IF EXISTS avoids generating an error
-- the first time this file is imported.
DROP VIEW IF EXISTS ride_months CASCADE;


-- Define views for your intermediate steps here:
-- Extract year and month for each ride for each user
CREATE VIEW ride_months AS
SELECT client.client_id, client.email, to_char(request.datetime, 'YYYY-MM') as all_months
FROM client
         LEFT OUTER JOIN (request JOIN dropoff ON request.request_id = dropoff.request_id)
                         ON client.client_id = request.client_id;


-- Your query that answers the question goes below the "insert into" line:
INSERT INTO q1

-- SOLUTION
SELECT client_id, email, count(distinct all_months) as months
FROM ride_months
GROUP BY client_id, email
