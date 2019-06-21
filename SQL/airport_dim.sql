DROP TABLE candidate4783.airport_dim;
CREATE TABLE candidate4783.airport_dim
(
    Airport_ID text NOT NULL PRIMARY KEY,
    AIRPORTNAME text UNIQUE,
    AIRPORTCODE text ,
    CITYNAME text,
    STATENAME text
);