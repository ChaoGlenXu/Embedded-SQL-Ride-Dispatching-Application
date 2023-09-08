-- Lure them back.

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO uber, public;
DROP TABLE IF EXISTS q2 CASCADE;

CREATE TABLE q2
(
    client_id INTEGER,
    name      VARCHAR(41),
    email     VARCHAR(30),
    billed    FLOAT,
    decline   INTEGER
);

-- Do this for each of the views that define your intermediate steps.
-- (But give them better names!) The IF EXISTS avoids generating an error
-- the first time this file is imported.
DROP VIEW IF EXISTS big_spenders CASCADE;
DROP VIEW IF EXISTS frequent_2020_riders CASCADE;
DROP VIEW IF EXISTS lost_interest CASCADE;
DROP VIEW IF EXISTS target_clients CASCADE;

-- Define views for your intermediate steps here:

-- Clients who had rides before 2020 costing at least $500 in total
CREATE VIEW big_spenders AS
SELECT request.client_id, sum(billed.amount) as billed
FROM request
         JOIN dropoff ON request.request_id = dropoff.request_id
         JOIN billed ON request.request_id = billed.request_id
WHERE EXTRACT(YEAR FROM request.datetime) < 2020
GROUP BY request.client_id
HAVING sum(billed.amount) >= 500;

-- Clients who have had between 1 and 10 rides in 2020
CREATE VIEW frequent_2020_riders AS
SELECT request.client_id, count(request.request_id) as num_rides
FROM request
         JOIN dropoff ON request.request_id = dropoff.request_id
         JOIN client ON request.client_id = client.client_id
WHERE EXTRACT(YEAR FROM request.datetime) = 2020
GROUP BY request.client_id
HAVING count(request.request_id) >= 1
   AND count(request.request_id) <= 10;

-- Clients who have had less rides in 2021 compared to 2020
-- COALESCE replaces null value with a default value
CREATE VIEW lost_interest AS
SELECT clients_2020.client_id,
       COALESCE(Clients_2021.num_rides, 0) - clients_2020.num_rides as decline
FROM (SELECT request.client_id, count(request.request_id) as num_rides
      FROM request
               JOIN dropoff ON request.request_id = dropoff.request_id
      WHERE EXTRACT(YEAR FROM request.datetime) = 2020
      GROUP BY request.client_id) Clients_2020
         LEFT JOIN
     -- LEFT JOIN to also include clients with 0 rides in 2021
         (SELECT request.client_id, count(request.request_id) as num_rides
          FROM request
                   JOIN dropoff ON request.request_id = dropoff.request_id
          WHERE EXTRACT(YEAR FROM request.datetime) = 2021
          GROUP BY request.client_id) Clients_2021
     ON Clients_2020.client_id = Clients_2021.client_id
WHERE Clients_2020.num_rides > Clients_2021.num_rides
   OR Clients_2021.num_rides IS NULL;

-- Clients who satisfy all three conditions
CREATE VIEW target_clients AS
    (SELECT client_id FROM big_spenders)
    INTERSECT
    (SELECT client_id FROM frequent_2020_riders)
    INTERSECT
    (SELECT client_id FROM lost_interest);

-- Your query that answers the question goes below the "insert into" line:
INSERT INTO q2

-- SOLUTION
SELECT client.client_id,
       CONCAT(client.firstname, ' ', client.surname) as name,
       COALESCE(client.email, 'unknown')             as email,
       big_spenders.billed,
       lost_interest.decline
FROM target_clients
         NATURAL JOIN client
         NATURAL JOIN big_spenders
         NATURAL JOIN lost_interest;
