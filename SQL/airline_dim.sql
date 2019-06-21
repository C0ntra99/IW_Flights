DROP TABLE candidate4783.airline_dim CASCADE;
CREATE TABLE candidate4783.airline_dim
(
    Airline_ID text NOT NULL PRIMARY KEY,
    AIRLINENAME text UNIQUE,
    AIRLINECODE VARCHAR(3) 
);