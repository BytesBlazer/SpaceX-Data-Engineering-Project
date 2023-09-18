TRUNCATE TABLE Payloads;
TRUNCATE TABLE Payloads_Manufacturer; 

COPY Payloads FROM '/home/osairis/Data_Engineering/SpaceX/staging/csv/spacex_payloads_transformed.csv' DELIMITER ',' CSV HEADER;
COPY Payloads_Manufacturer FROM '/home/osairis/Data_Engineering/SpaceX/staging/csv/spacex_payload_manufacturers_transformed.csv' DELIMITER ',' CSV HEADER;
