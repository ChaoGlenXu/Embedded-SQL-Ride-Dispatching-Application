-- Bigger and smaller spenders.

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO uber, public;
DROP TABLE IF EXISTS q5 CASCADE;

CREATE TABLE q5(
    client_id INTEGER,
    month VARCHAR(7),
    total FLOAT,
    comparison VARCHAR(30)
);

-- Do this for each of the views that define your intermediate steps.
-- (But give them better names!) The IF EXISTS avoids generating an error
-- the first time this file is imported.
DROP VIEW IF EXISTS ride_months CASCADE;
DROP VIEW IF EXISTS month_avg CASCADE;
DROP VIEW IF EXISTS all_client_month_combi CASCADE;
DROP VIEW IF EXISTS client_bill_total CASCADE;
DROP VIEW IF EXISTS combine_sum_avg CASCADE;


-- Define views for your intermediate steps here:

-- Months in which some client had a ride
CREATE VIEW ride_months AS
SELECT request.request_id, to_char(request.datetime, 'YYYY MM') as month
FROM request JOIN dropoff on request.request_id = dropoff.request_id NATURAL JOIN client;

-- Average bill for each month
CREATE VIEW month_avg AS
SELECT ride_months.month, avg(billed.amount) as avg_bill
FROM ride_months JOIN billed ON ride_months.request_id = billed.request_id
GROUP BY month;

-- All combinations of clients and months
CREATE VIEW all_client_month_combi AS
SELECT month_avg.month, month_avg.avg_bill, client.client_id
FROM month_avg, client;

-- Sum of all monthly bills for clients who have had rides in the month
CREATE VIEW client_bill_total AS
SELECT request.client_id, to_char(request.datetime, 'YYYY MM') as month,
    sum(billed.amount) as client_total
FROM request NATURAL JOIN billed
GROUP BY request.client_id, to_char(request.datetime, 'YYYY MM');

-- Combine all month avgs and all client spendings
CREATE VIEW combine_sum_avg AS
SELECT acm.client_id, acm.month, COALESCE(cbt.client_total, 0) as total, acm.avg_bill
FROM all_client_month_combi acm LEFT OUTER JOIN client_bill_total cbt
    ON acm.client_id = cbt.client_id AND
       acm.month = cbt.month;


-- Your query that answers the question goes below the "insert into" line:
INSERT INTO q5

SELECT distinct client_id, month, total,
                CASE WHEN total < avg_bill THEN 'below'
                    ELSE 'at or above' END as comparison
FROM combine_sum_avg;