TRUNCATE TABLE History;

COPY History FROM '/home/osairis/Data_Engineering/SpaceX/staging/csv/spacex_history_transformed.csv' DELIMITER ',' CSV HEADER;
