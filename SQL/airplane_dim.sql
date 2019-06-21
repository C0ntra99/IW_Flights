DROP TABLE candidate4783.airplane_dim;
CREATE TABLE candidate4783.airplane_dim
(
    Airplane_ID text NOT NULL PRIMARY KEY,
    AIRLINENAME text,
    FOREIGN KEY (AIRLINENAME) REFERENCES candidate4783.airline_dim (airlinename),
    TAILNUM VARCHAR(10) UNIQUE
);