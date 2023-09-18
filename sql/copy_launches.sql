TRUNCATE TABLE Launches;

COPY Launches FROM '/home/osairis/Data_Engineering/SpaceX/staging/csv/spacex_launches_transformed.csv' DELIMITER ',' CSV HEADER;


