DROP TABLE TRANSFORM_LOCATION;

CREATE TABLE TRANSFORM_LOCATION (
    region_key NUMBER PRIMARY KEY,
    region_id NUMBER ,
    region_name VARCHAR2(50),
    area VARCHAR2(50),
    city VARCHAR2(50),
    source_system VARCHAR2(50)
);

DROP SEQUENCE TRANSFORM_LOCATION_SEQ;

-- Create a new sequence
CREATE SEQUENCE TRANSFORM_LOCATION_SEQ
    INCREMENT BY 1
    START WITH 1
    CACHE 20
    NOORDER
    NOCYCLE;


-- Create the trigger for generating the surrogate key before insert
CREATE OR REPLACE TRIGGER TRANSFORM_LOCATION_TRIGGER
    BEFORE INSERT ON TRANSFORM_LOCATION
    FOR EACH ROW
BEGIN
    :NEW.region_key := TRANSFORM_LOCATION_SEQ.NEXTVAL;  
END;
/


CREATE OR REPLACE PROCEDURE PR_TRANS_LOCATION
AS
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
            (
             G.REGION_ID, 
             G.REGION_NAME, 
             G.CRIME_LOCATION, 
             G.STATION_NAME, 
             G.SOURCE_SYSTEM);
END;
/



DROP TABLE TRANSFORM_TIME;
CREATE TABLE TRANSFORM_TIME (
    date_id NUMBER PRIMARY KEY, 
    crime_id number,
    full_date DATE,
    year NUMBER,
    month NUMBER,
    day NUMBER,
    quarter NUMBER,
    date_type VARCHAR2(20)
);

-- Drop the sequence if it exists
DROP SEQUENCE TRANSFORM_TIME_SEQ;

-- Create a new sequence
CREATE SEQUENCE TRANSFORM_TIME_SEQ
    INCREMENT BY 1
    START WITH 1
    CACHE 20
    NOORDER
    NOCYCLE;


-- Create the trigger for generating the surrogate key before insert
CREATE OR REPLACE TRIGGER TRANSFORM_TIME_TRIGGER
    BEFORE INSERT ON TRANSFORM_TIME
    FOR EACH ROW
BEGIN
    :NEW.date_id := TRANSFORM_TIME_SEQ.NEXTVAL;  
END;
/


CREATE OR REPLACE PROCEDURE PR_TRANS_TIME
AS
BEGIN
    MERGE INTO TRANSFORM_TIME T
    USING GOOD_BAD_DATA G
    ON (T.crime_id = G.CRIME_ID AND T.full_date = G.REPORT_DATE AND T.date_type = 'Reported')
    WHEN MATCHED THEN
        UPDATE SET 
            T.year = EXTRACT(YEAR FROM G.REPORT_DATE),
            T.month = EXTRACT(MONTH FROM G.REPORT_DATE),
            T.day = EXTRACT(DAY FROM G.REPORT_DATE),
            T.quarter = CASE 
                           WHEN EXTRACT(MONTH FROM G.REPORT_DATE) IN (1, 2, 3) THEN 1
                           WHEN EXTRACT(MONTH FROM G.REPORT_DATE) IN (4, 5, 6) THEN 2
                           WHEN EXTRACT(MONTH FROM G.REPORT_DATE) IN (7, 8, 9) THEN 3
                           ELSE 4
                       END
    WHEN NOT MATCHED THEN
        INSERT (crime_id, full_date, year, month, day, quarter, date_type)
        VALUES 
            (G.CRIME_ID, G.REPORT_DATE, 
             EXTRACT(YEAR FROM G.REPORT_DATE),
             EXTRACT(MONTH FROM G.REPORT_DATE),
             EXTRACT(DAY FROM G.REPORT_DATE),
             CASE 
                WHEN EXTRACT(MONTH FROM G.REPORT_DATE) IN (1, 2, 3) THEN 1
                WHEN EXTRACT(MONTH FROM G.REPORT_DATE) IN (4, 5, 6) THEN 2
                WHEN EXTRACT(MONTH FROM G.REPORT_DATE) IN (7, 8, 9) THEN 3
                ELSE 4
              END,
             'Reported');

    MERGE INTO TRANSFORM_TIME T
    USING GOOD_BAD_DATA G
    ON (T.crime_id = G.CRIME_ID AND T.full_date = G.CLOSE_DATE AND T.date_type = 'Closed')
    WHEN MATCHED THEN
        UPDATE SET 
            T.year = EXTRACT(YEAR FROM G.CLOSE_DATE),
            T.month = EXTRACT(MONTH FROM G.CLOSE_DATE),
            T.day = EXTRACT(DAY FROM G.CLOSE_DATE),
            T.quarter = CASE 
                           WHEN EXTRACT(MONTH FROM G.CLOSE_DATE) IN (1, 2, 3) THEN 1
                           WHEN EXTRACT(MONTH FROM G.CLOSE_DATE) IN (4, 5, 6) THEN 2
                           WHEN EXTRACT(MONTH FROM G.CLOSE_DATE) IN (7, 8, 9) THEN 3
                           ELSE 4
                       END
    WHEN NOT MATCHED THEN
        INSERT (crime_id, full_date, year, month, day, quarter, date_type)
        VALUES 
            (G.CRIME_ID, G.CLOSE_DATE, 
             EXTRACT(YEAR FROM G.CLOSE_DATE),
             EXTRACT(MONTH FROM G.CLOSE_DATE),
             EXTRACT(DAY FROM G.CLOSE_DATE),
             CASE 
                WHEN EXTRACT(MONTH FROM G.CLOSE_DATE) IN (1, 2, 3) THEN 1
                WHEN EXTRACT(MONTH FROM G.CLOSE_DATE) IN (4, 5, 6) THEN 2
                WHEN EXTRACT(MONTH FROM G.CLOSE_DATE) IN (7, 8, 9) THEN 3
                ELSE 4
              END,
             'Closed');
END;
/




DROP TABLE TRANSFORM_CRIME_TYPE;

CREATE TABLE TRANSFORM_CRIME_TYPE (
    crime_type_key NUMBER PRIMARY KEY,
    crime_id NUMBER ,
    crime_type VARCHAR2(50),
    region_id varchar2(50),
    crime_category VARCHAR2(50),
    severity_level VARCHAR2(15),
    source_system VARCHAR2(50)
);

-- Drop the sequence if it exists
DROP SEQUENCE TRANSFORM_CRIME_TYPE_SEQ;

-- Create a new sequence
CREATE SEQUENCE TRANSFORM_CRIME_TYPE_SEQ
    INCREMENT BY 1
    START WITH 1
    CACHE 20
    NOORDER
    NOCYCLE;


-- Create the trigger for generating the surrogate key before insert
CREATE OR REPLACE TRIGGER TRANSFORM_CRIME_TYPE_TRIGGER
    BEFORE INSERT ON TRANSFORM_CRIME_TYPE
    FOR EACH ROW
BEGIN
    :NEW.crime_type_key := TRANSFORM_CRIME_TYPE_SEQ.NEXTVAL;  
END;
/


CREATE OR REPLACE PROCEDURE PR_TRANS_CRIME_TYPE
AS
BEGIN
    MERGE INTO TRANSFORM_CRIME_TYPE T
    USING GOOD_BAD_DATA G
    ON (T.crime_id = G.CRIME_ID)
    WHEN MATCHED THEN
        UPDATE SET 
            T.crime_type = G.CRIME_TYPE,
            T.region_id = G.region_id,
            T.crime_category = NULL,  
            T.severity_level = G.CRIME_SEVERITY,
            T.source_system = G.SOURCE_SYSTEM
    WHEN NOT MATCHED THEN
        INSERT (crime_id, crime_type, region_id, crime_category, severity_level, source_system)
        VALUES 
            (G.CRIME_ID, G.CRIME_TYPE, G.region_id, NULL, G.CRIME_SEVERITY, G.SOURCE_SYSTEM);
END;
/


DROP TABLE TRANSFORM_POLICE_STATION;

CREATE TABLE TRANSFORM_POLICE_STATION (
    station_key NUMBER PRIMARY KEY,
    station_id NUMBER ,
    station_name VARCHAR2(50),
    region_id NUMBER,
    source_system VARCHAR2(50)
);

DROP SEQUENCE TRANSFORM_POLICE_STATION_SEQ;

CREATE SEQUENCE TRANSFORM_POLICE_STATION_SEQ
    INCREMENT BY 1
    START WITH 1
    CACHE 20
    NOORDER
    NOCYCLE;


-- Create the trigger for generating the surrogate key before insert
CREATE OR REPLACE TRIGGER TRANSFORM_POLICE_STATION_TRIGGER
    BEFORE INSERT ON TRANSFORM_POLICE_STATION
    FOR EACH ROW
BEGIN
    :NEW.station_key := TRANSFORM_POLICE_STATION_SEQ.NEXTVAL;  
END;
/


CREATE OR REPLACE PROCEDURE PR_TRANS_POLICE_STATION
AS
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
END;
/


DROP TABLE TRANSFORM_CRIME_STATUS;

CREATE TABLE TRANSFORM_CRIME_STATUS (
    crime_status_key NUMBER PRIMARY KEY,
    crime_status_id NUMBER,
    crime_status VARCHAR2(50),
    source_system VARCHAR2(50)
);

DROP SEQUENCE TRANSFORM_CRIME_STATUS_SEQ;

CREATE SEQUENCE TRANSFORM_CRIME_STATUS_SEQ
    INCREMENT BY 1
    START WITH 1
    CACHE 20
    NOORDER
    NOCYCLE;


-- Create the trigger for generating the surrogate key before insert
CREATE OR REPLACE TRIGGER TRANSFORM_CRIME_STATUS_TRIGGER
    BEFORE INSERT ON TRANSFORM_CRIME_STATUS
    FOR EACH ROW
BEGIN
    :NEW.crime_status_key := TRANSFORM_CRIME_STATUS_SEQ.NEXTVAL;  
END;
/


CREATE OR REPLACE PROCEDURE PR_TRANS_CRIME_STATUS
AS
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
END;
/


BEGIN
    PR_TRANS_LOCATION;
    PR_TRANS_TIME;
    PR_TRANS_CRIME_TYPE;
    PR_TRANS_POLICE_STATION;
    PR_TRANS_CRIME_STATUS;
END;
/


