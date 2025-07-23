CREATE OR REPLACE PACKAGE transform_pkg AS
    -- Function declarations
    FUNCTION get_year(p_date DATE) RETURN NUMBER;
    FUNCTION get_month(p_date DATE) RETURN NUMBER;
    FUNCTION get_quarter(p_date DATE) RETURN NUMBER;

    -- Procedure declarations
    PROCEDURE PR_TRANS_LOCATION;
    PROCEDURE PR_TRANS_TIME;
    PROCEDURE PR_TRANS_CRIME_TYPE;
    PROCEDURE PR_TRANS_POLICE_STATION;
    PROCEDURE PR_TRANS_CRIME_STATUS;
END transform_pkg;
/


CREATE OR REPLACE PACKAGE BODY transform_pkg AS

    -- Function to extract the year from a date
    FUNCTION get_year(p_date DATE) RETURN NUMBER IS
    BEGIN
        RETURN EXTRACT(YEAR FROM p_date);
    END get_year;

    -- Function to extract the month from a date
    FUNCTION get_month(p_date DATE) RETURN NUMBER IS
    BEGIN
        RETURN EXTRACT(MONTH FROM p_date);
    END get_month;

    -- Function to calculate the quarter based on the month of a date
    FUNCTION get_quarter(p_date DATE) RETURN NUMBER IS
    BEGIN
        RETURN CASE
                   WHEN EXTRACT(MONTH FROM p_date) IN (1, 2, 3) THEN 1
                   WHEN EXTRACT(MONTH FROM p_date) IN (4, 5, 6) THEN 2
                   WHEN EXTRACT(MONTH FROM p_date) IN (7, 8, 9) THEN 3
                   ELSE 4
               END;
    END get_quarter;

    -- Procedure to transform location data
    PROCEDURE PR_TRANS_LOCATION IS
    BEGIN
        MERGE INTO TRANSFORM_LOCATION T
        USING GOOD_BAD_DATA G
        ON (T.region_id = G.REGION_ID)
        WHEN MATCHED THEN
            UPDATE SET 
                T.region_name = G.REGION_NAME,
                T.area = G.CRIME_LOCATION,
                T.city = G.STATION_NAME,
                T.source_system = G.SOURCE_SYSTEM
        WHEN NOT MATCHED THEN
            INSERT (region_id, region_name, area, city, source_system)
            VALUES 
                (G.REGION_ID, 
                 G.REGION_NAME, 
                 G.CRIME_LOCATION, 
                 G.STATION_NAME, 
                 G.SOURCE_SYSTEM);
    END PR_TRANS_LOCATION;

    -- Procedure to transform time data (including Reported and Closed dates)
    PROCEDURE PR_TRANS_TIME IS
    BEGIN
        -- For Reported Date
        MERGE INTO TRANSFORM_TIME T
        USING GOOD_BAD_DATA G
        ON (T.crime_id = G.CRIME_ID AND T.full_date = G.REPORT_DATE AND T.date_type = 'Reported')
        WHEN MATCHED THEN
            UPDATE SET 
                T.year = get_year(G.REPORT_DATE),
                T.month = get_month(G.REPORT_DATE),
                T.day = EXTRACT(DAY FROM G.REPORT_DATE),
                T.quarter = get_quarter(G.REPORT_DATE)
        WHEN NOT MATCHED THEN
            INSERT (crime_id, full_date, year, month, day, quarter, date_type)
            VALUES 
                (G.CRIME_ID, G.REPORT_DATE, 
                 get_year(G.REPORT_DATE),
                 get_month(G.REPORT_DATE),
                 EXTRACT(DAY FROM G.REPORT_DATE),
                 get_quarter(G.REPORT_DATE),
                 'Reported');

        -- For Closed Date
        MERGE INTO TRANSFORM_TIME T
        USING GOOD_BAD_DATA G
        ON (T.crime_id = G.CRIME_ID AND T.full_date = G.CLOSE_DATE AND T.date_type = 'Closed')
        WHEN MATCHED THEN
            UPDATE SET 
                T.year = get_year(G.CLOSE_DATE),
                T.month = get_month(G.CLOSE_DATE),
                T.day = EXTRACT(DAY FROM G.CLOSE_DATE),
                T.quarter = get_quarter(G.CLOSE_DATE)
        WHEN NOT MATCHED THEN
            INSERT (crime_id, full_date, year, month, day, quarter, date_type)
            VALUES 
                (G.CRIME_ID, G.CLOSE_DATE, 
                 get_year(G.CLOSE_DATE),
                 get_month(G.CLOSE_DATE),
                 EXTRACT(DAY FROM G.CLOSE_DATE),
                 get_quarter(G.CLOSE_DATE),
                 'Closed');
    END PR_TRANS_TIME;

    -- Procedure to transform crime type data
    PROCEDURE PR_TRANS_CRIME_TYPE IS
    BEGIN
        MERGE INTO TRANSFORM_CRIME_TYPE T
        USING GOOD_BAD_DATA G
        ON (T.crime_id = G.CRIME_ID)
        WHEN MATCHED THEN
            UPDATE SET 
                T.crime_type = G.CRIME_TYPE,
                T.region_id = G.region_id,
                T.crime_category = NULL,  -- Assuming category is not present in source data
                T.severity_level = G.CRIME_SEVERITY,
                T.source_system = G.SOURCE_SYSTEM
        WHEN NOT MATCHED THEN
            INSERT (crime_id, crime_type, region_id, crime_category, severity_level, source_system)
            VALUES 
                (G.CRIME_ID, G.CRIME_TYPE, G.region_id, NULL, G.CRIME_SEVERITY, G.SOURCE_SYSTEM);
    END PR_TRANS_CRIME_TYPE;

    -- Procedure to transform police station data
    PROCEDURE PR_TRANS_POLICE_STATION IS
    BEGIN
        MERGE INTO TRANSFORM_POLICE_STATION T
        USING GOOD_BAD_DATA G
        ON (T.station_id = G.STATION_ID)
        WHEN MATCHED THEN
            UPDATE SET 
                T.station_name = G.STATION_NAME,
                T.region_id = G.REGION_ID,
                T.source_system = G.SOURCE_SYSTEM
        WHEN NOT MATCHED THEN
            INSERT (station_id, station_name, region_id, source_system)
            VALUES 
                (G.STATION_ID, G.STATION_NAME, G.REGION_ID, G.SOURCE_SYSTEM);
    END PR_TRANS_POLICE_STATION;

    -- Procedure to transform crime status data
    PROCEDURE PR_TRANS_CRIME_STATUS IS
    BEGIN
        MERGE INTO TRANSFORM_CRIME_STATUS T
        USING GOOD_BAD_DATA G
        ON (T.crime_status_id = G.CRIME_ID)
        WHEN MATCHED THEN
            UPDATE SET 
                T.crime_status = G.CRIME_STATUS,
                T.source_system = G.SOURCE_SYSTEM
        WHEN NOT MATCHED THEN
            INSERT (crime_status_id, crime_status, source_system)
            VALUES 
                (G.CRIME_ID, G.CRIME_STATUS, G.SOURCE_SYSTEM);
    END PR_TRANS_CRIME_STATUS;

END transform_pkg;
/


BEGIN
    transform_pkg.PR_TRANS_LOCATION;
    transform_pkg.PR_TRANS_TIME;
    transform_pkg.PR_TRANS_CRIME_TYPE;
    transform_pkg.PR_TRANS_POLICE_STATION;
    transform_pkg.PR_TRANS_CRIME_STATUS;
END;
/
