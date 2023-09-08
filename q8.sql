-- Scratching backs?

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO uber, public;
DROP TABLE IF EXISTS q8 CASCADE;

CREATE TABLE q8(
    client_id INTEGER,
    reciprocals INTEGER,
    difference FLOAT
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.

-- Define views for your intermediate steps here:

-- Tables in use

-- For driver rating
-- driverrating(request_id, rating),

-- For client rating
-- request (request_id, client_id), clientrating(request_id, rating)


-- Your query that answers the question goes below the "insert into" line:
INSERT INTO q8
SELECT request.client_id, count(request.request_id) as reciprocals,
       avg(driverrating.rating - clientrating.rating)
FROM (driverrating JOIN clientrating ON driverrating.request_id = clientrating.request_id)
    JOIN request ON driverrating.request_id = request.request_id
GROUP BY request.client_id;