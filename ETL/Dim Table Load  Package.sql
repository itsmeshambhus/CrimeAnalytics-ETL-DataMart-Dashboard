CREATE OR REPLACE PACKAGE dim_data_pkg AS

    -- Procedure declarations for each data dimension
    PROCEDURE datadim_location;
    PROCEDURE datadim_time;
    PROCEDURE datadim_crime_type;
    PROCEDURE datadim_police_station;
    PROCEDURE datadim_crime_status;

END dim_data_pkg;
/


CREATE OR REPLACE PACKAGE BODY dim_data_pkg AS

    -- Procedure to populate DIM_LOCATION table
    PROCEDURE datadim_location AS
    BEGIN
        MERGE INTO DIM_LOCATION D
        USING (
            SELECT DISTINCT region_id, region_name, area, city, source_system
            FROM TRANSFORM_LOCATION
        ) T
        ON (D.region_id = T.region_id AND D.region_name = T.region_name AND D.area = T.area AND D.city = T.city)

        WHEN NOT MATCHED THEN
            INSERT (region_id, region_name, area, city, data_source)
            VALUES (T.region_id, T.region_name, T.area, T.city, T.source_system);

        COMMIT;
    END datadim_location;

    -- Procedure to populate DIM_TIME table
    PROCEDURE datadim_time AS
    BEGIN
        MERGE INTO DIM_TIME D
        USING (
            SELECT DISTINCT year, month, day, quarter
            FROM TRANSFORM_TIME
        ) T
        ON (D.year = T.year AND D.month = T.month AND D.day = T.day AND D.quarter = T.quarter)

        WHEN NOT MATCHED THEN
            INSERT (year, month, day, quarter)
            VALUES (T.year, T.month, T.day, T.quarter);

        COMMIT;
    END datadim_time;

    -- Procedure to populate DIM_CRIME_TYPE table
    PROCEDURE datadim_crime_type AS
    BEGIN
        MERGE INTO DIM_CRIME_TYPE D
        USING (
            SELECT DISTINCT crime_type, crime_category, severity_level, source_system
            FROM TRANSFORM_CRIME_TYPE
        ) T
        ON (D.crime_type_name = T.crime_type AND D.crime_category = T.crime_category AND D.data_source = T.source_system)

        WHEN NOT MATCHED THEN
            INSERT (crime_type_name, crime_category, severity_level, data_source)
            VALUES (T.crime_type, T.crime_category, T.severity_level, T.source_system);

        COMMIT;
    END datadim_crime_type;

    -- Procedure to populate DIM_POLICE_STATION table
    PROCEDURE datadim_police_station AS
    BEGIN
        MERGE INTO DIM_POLICE_STATION D
        USING (
            SELECT DISTINCT station_id, station_name, region_id, source_system
            FROM TRANSFORM_POLICE_STATION
        ) T
        ON (D.station_id = T.station_id AND D.station_name = T.station_name)

        WHEN NOT MATCHED THEN
            INSERT (station_id, station_name, data_source)
            VALUES (T.station_id, T.station_name, T.source_system);

        COMMIT;
    END datadim_police_station;

    -- Procedure to populate DIM_CRIME_STATUS table
    PROCEDURE datadim_crime_status AS
    BEGIN
        MERGE INTO DIM_CRIME_STATUS D
        USING ( 
            SELECT DISTINCT crime_status, source_system
            FROM TRANSFORM_CRIME_STATUS
        ) T
        ON (D.crime_status = T.crime_status AND D.data_source = T.source_system)

        WHEN NOT MATCHED THEN
            INSERT (crime_status, data_source)
            VALUES 
                (T.crime_status, T.source_system);

        COMMIT;
    END datadim_crime_status;

END dim_data_pkg;
/


BEGIN
    -- Populate data dimensions and fact tables
    dim_data_pkg.datadim_location;
    dim_data_pkg.datadim_time;
    dim_data_pkg.datadim_crime_type;
    dim_data_pkg.datadim_police_station;
    dim_data_pkg.datadim_crime_status;
END;
/
