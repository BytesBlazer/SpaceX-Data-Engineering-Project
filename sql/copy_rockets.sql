TRUNCATE TABLE Rockets;

COPY Rockets FROM '/home/osairis/Data_Engineering/SpaceX/staging/csv/spacex_rockets_transformed.csv' DELIMITER ',' CSV HEADER;
