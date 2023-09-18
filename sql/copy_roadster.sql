TRUNCATE TABLE Roadster;

COPY Roadster FROM '/home/osairis/Data_Engineering/SpaceX/staging/csv/spacex_roadster_transformed.csv' DELIMITER ',' CSV HEADER;
