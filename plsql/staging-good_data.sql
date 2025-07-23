
DROP TABLE STAGING_CRIME_DATA;
DROP SEQUENCE STAGING_CRIME_SEQ;
DROP PROCEDURE PRCS_TO_STAGING;


CREATE TABLE STAGING_CRIME_DATA (
    STAGING_CRIME_ID NUMBER,                                   
    CRIME_ID INTEGER NOT NULL,                                 
    REPORT_DATE DATE,                                          
    CRIME_TYPE VARCHAR2(50),                                   
    STATION_ID NUMBER,                                         
    STATION_NAME VARCHAR2(100),                                
    REGION_ID NUMBER,                                          
    REGION_NAME VARCHAR2(100),                                 
    CRIME_LOCATION VARCHAR2(100),                              
    LEAD_OFFICER_ID NUMBER,                                    
    OFFICER_NAME VARCHAR2(100),                                
    CLOSE_DATE DATE,                                           
    CRIME_STATUS VARCHAR2(50),                                 
    ESCALATED_FLAG VARCHAR2(1),                                
    CRIME_SEVERITY VARCHAR2(10),                               
    SOURCE_SYSTEM VARCHAR2(50),                                

    -- Primary Key Constraint
    CONSTRAINT "PK_CRIME_STAGING" PRIMARY KEY (STAGING_CRIME_ID)
);

-- 2. Create a Sequence for Staging Surrogate Key
DROP SEQUENCE STAGING_CRIME_SEQ

CREATE SEQUENCE "STAGING_CRIME_SEQ"
    START WITH 1
    INCREMENT BY 1
    NOCACHE
    NOCYCLE;

-- 3. Create Trigger to Assign Staging Surrogate Key Before Insert
DROP TRIGGER STAGING_CRIME_TRIGGER

CREATE OR REPLACE TRIGGER "STAGING_CRIME_TRIGGER"
    BEFORE INSERT ON STAGING_CRIME_DATA
    FOR EACH ROW
BEGIN
    :NEW.STAGING_CRIME_ID := STAGING_CRIME_SEQ.NEXTVAL;  
END;

-----------------
DROP PROCEDURE PRCS_TO_STAGING

CREATE OR REPLACE PROCEDURE PRCS_TO_STAGING AS
BEGIN
    MERGE INTO STAGING_CRIME_DATA SC
    USING (
        SELECT 
            rc.reported_crime_id AS crime_id,
            rc.date_reported AS report_date,
            ct.crime_type_desc AS crime_type,
            s.station_id,
            s.station_name,
            a.area_id AS region_id,
            a.area_name AS region_name,
            rc.crime_postcode AS crime_location,
            MAX(pe.emp_id) AS lead_officer_id,  
            MAX(pe.emp_name) AS officer_name,
            rc.date_closed AS close_date,
            rc.crime_status,
            CASE 
                WHEN rc.crime_status = 'ESCALATE' THEN 'Y'
                ELSE 'N'
            END AS escalated_flag,
            'PRCS' AS source_system,
            
            CASE 
                WHEN ct.crime_type_desc IN ('Violent Crime', 'Robbery', 'Burglary') THEN 'High'
                WHEN ct.crime_type_desc IN ('Drugs Offence', 'Car theft') THEN 'Medium'
                WHEN ct.crime_type_desc IN ('Fraud', 'Forgery') THEN 'Low'
                WHEN ct.crime_type_desc = 'Drunk and DisOrder' THEN 'Low'
                ELSE 'Unknown'  
            END AS crime_severity
        FROM 
            pl_reported_crime rc
        JOIN 
            pl_crime_type ct ON rc.fk1_crime_type_id = ct.crime_type_id
        JOIN 
            pl_station s ON rc.fk2_station_id = s.station_id
        JOIN 
            pl_area a ON s.fk1_area_id = a.area_id
        LEFT JOIN 
            pl_work_allocation wa ON rc.reported_crime_id = wa.s_reported_crime_id
        LEFT JOIN 
            pl_police_employee pe ON wa.lead_police_officer = pe.emp_id
        GROUP BY 
            rc.reported_crime_id, rc.date_reported, ct.crime_type_desc, s.station_id, s.station_name, 
            a.area_id, a.area_name, rc.crime_postcode, rc.date_closed, rc.crime_status
    ) SRC
    ON (SC.CRIME_ID = SRC.crime_id AND SC.SOURCE_SYSTEM = SRC.source_system)
    WHEN MATCHED THEN
        UPDATE SET 
            SC.REPORT_DATE = SRC.report_date,
            SC.CRIME_TYPE = SRC.crime_type,
            SC.STATION_ID = SRC.station_id,
            SC.STATION_NAME = SRC.station_name,
            SC.REGION_ID = SRC.region_id,
            SC.REGION_NAME = SRC.region_name,
            SC.CRIME_LOCATION = SRC.crime_location,
            SC.LEAD_OFFICER_ID = SRC.lead_officer_id,
            SC.OFFICER_NAME = SRC.officer_name,
            SC.CLOSE_DATE = SRC.close_date,
            SC.CRIME_STATUS = SRC.crime_status,
            SC.ESCALATED_FLAG = SRC.escalated_flag,
            SC.CRIME_SEVERITY = SRC.crime_severity
    WHEN NOT MATCHED THEN
        INSERT (
            CRIME_ID, REPORT_DATE, CRIME_TYPE, 
            STATION_ID, STATION_NAME, REGION_ID, REGION_NAME, 
            CRIME_LOCATION, LEAD_OFFICER_ID, OFFICER_NAME, 
            CLOSE_DATE, CRIME_STATUS, ESCALATED_FLAG, CRIME_SEVERITY, SOURCE_SYSTEM
        )
        VALUES (
            SRC.crime_id, SRC.report_date, SRC.crime_type, 
            SRC.station_id, SRC.station_name, SRC.region_id, SRC.region_name, 
            SRC.crime_location, SRC.lead_officer_id, SRC.officer_name, 
            SRC.close_date, SRC.crime_status, SRC.escalated_flag, SRC.crime_severity, SRC.source_system
        );
END PRCS_TO_STAGING;

BEGIN
    PRCS_TO_STAGING;
END;

SELECT * FROM STAGING_CRIME_DATA;

----------------------

CREATE OR REPLACE PROCEDURE PS_WALES_TO_STAGING AS
BEGIN
    MERGE INTO STAGING_CRIME_DATA SC
    USING (
        SELECT 
            rc.crime_id AS crime_id,
            rc.reported_date AS report_date,
            rc.CRIME_NAME AS crime_type,
            l.location_id AS station_id,
            l.city_name AS station_name,
            a.region_id AS region_id,
            a.region_name AS region_name,
            l.post_code AS crime_location,
            rc.police_id AS lead_officer_id,
            oi.first_name || ' ' || NVL(oi.middle_name, '') || ' ' || oi.last_name AS officer_name,
            rc.closed_date AS close_date,
            rc.crime_status AS crime_status,
            CASE 
                WHEN rc.crime_status = 'ESCALATE' THEN 'Y'
                ELSE 'N'
            END AS escalated_flag,
            'PS_WALES' AS source_system,
            CASE
                WHEN o.offence_type IN ('Robbery', 'Murder') THEN 'HIGH'
                WHEN o.offence_type IN ('Assault', 'Burglary') THEN 'MEDIUM'
                ELSE 'LOW'
            END AS crime_severity
        FROM 
            CRIME_REGISTER rc
        LEFT OUTER JOIN 
            OFFENCE o ON rc.crime_id = o.crime_id
        LEFT OUTER JOIN 
            LOCATION l ON rc.location_id = l.location_id
        LEFT OUTER JOIN 
            REGION a ON l.region_id = a.region_id
        LEFT OUTER JOIN 
            OFFICER oi ON rc.police_id = oi.officer_id
    ) SRC
    ON (SC.CRIME_ID = SRC.crime_id AND SC.SOURCE_SYSTEM = SRC.source_system)
    WHEN MATCHED THEN
        UPDATE SET 
            SC.REPORT_DATE = SRC.report_date,
            SC.CRIME_TYPE = SRC.crime_type,
            SC.STATION_ID = SRC.station_id,
            SC.STATION_NAME = SRC.station_name,
            SC.REGION_ID = SRC.region_id,
            SC.REGION_NAME = SRC.region_name,
            SC.CRIME_LOCATION = SRC.crime_location,
            SC.LEAD_OFFICER_ID = SRC.lead_officer_id,
            SC.OFFICER_NAME = SRC.officer_name,
            SC.CLOSE_DATE = SRC.close_date,
            SC.CRIME_STATUS = SRC.crime_status,
            SC.ESCALATED_FLAG = SRC.escalated_flag,
            SC.CRIME_SEVERITY = SRC.crime_severity
    WHEN NOT MATCHED THEN
        INSERT (
            CRIME_ID, REPORT_DATE, CRIME_TYPE, 
            STATION_ID, STATION_NAME, REGION_ID, REGION_NAME, 
            CRIME_LOCATION, LEAD_OFFICER_ID, OFFICER_NAME, 
            CLOSE_DATE, CRIME_STATUS, ESCALATED_FLAG, CRIME_SEVERITY, SOURCE_SYSTEM
        )
        VALUES (
            SRC.crime_id, SRC.report_date, SRC.crime_type, 
            SRC.station_id, SRC.station_name, SRC.region_id, SRC.region_name, 
            SRC.crime_location, SRC.lead_officer_id, SRC.officer_name, 
            SRC.close_date, SRC.crime_status, SRC.escalated_flag, SRC.crime_severity, SRC.source_system
        );
END PS_WALES_TO_STAGING;


BEGIN
    PS_WALES_TO_STAGING;
END;

-----------------------
-----------------------

Drop table BAD_CRIME_DATA
drop table GOOD_CRIME_DATA

CREATE TABLE GOOD_CRIME_DATA AS
SELECT *
FROM STAGING_CRIME_DATA
WHERE 1=2;

CREATE TABLE BAD_CRIME_DATA AS
SELECT *
FROM STAGING_CRIME_DATA
WHERE 1=2;

CREATE OR REPLACE PROCEDURE COPY_GOOD_BAD_DATA AS
BEGIN
    INSERT INTO BAD_CRIME_DATA
    SELECT * 
    FROM STAGING_CRIME_DATA
    WHERE 
        crime_type IS NULL

        OR (TRIM(LOWER(crime_status)) = 'closed' AND close_date IS NULL)

        OR (close_date IS NOT NULL AND close_date < report_date)

        OR region_name LIKE '%,%';

    INSERT INTO GOOD_CRIME_DATA
    SELECT * 
    FROM STAGING_CRIME_DATA
    WHERE 
        crime_type IS NOT NULL

        AND (TRIM(LOWER(crime_status)) != 'closed' OR (TRIM(LOWER(crime_status)) = 'closed' AND close_date IS NOT NULL))

        AND (close_date IS NULL OR close_date >= report_date)

        AND region_name NOT LIKE '%,%';

    DBMS_OUTPUT.PUT_LINE('Data validation and separation completed successfully.');
END COPY_GOOD_BAD_DATA;


BEGIN
    COPY_GOOD_BAD_DATA;
END;


SELECT *
FROM STAGING_CRIME_DATA
WHERE crime_status = 'Closed'
  AND close_date IS not null;


SELECT *
FROM GOOD_CRIME_DATA
WHERE TRIM(LOWER(crime_status)) = 'closed'
  AND close_date IS NULL;



CREATE OR REPLACE PROCEDURE FIX_BAD_DATA_DATES AS
    v_random_crime_type VARCHAR2(50);
    v_random_date DATE;
BEGIN
    -- Step 1: Swap close_date and report_date where close_date < report_date
    UPDATE BAD_CRIME_DATA
    SET 
        close_date = report_date,
        report_date = close_date
    WHERE close_date < report_date;

    DBMS_OUTPUT.PUT_LINE('Swapped close_date and report_date where close_date was earlier than report_date.');

    -- Step 2: Add random close_date for crimes with status Closed and missing close_date
    UPDATE BAD_CRIME_DATA
    SET close_date = report_date + TRUNC(DBMS_RANDOM.VALUE(1, 30)) -- Add 1 to 30 random days
    WHERE TRIM(LOWER(crime_status)) = 'closed' 
      AND close_date IS NULL;

    DBMS_OUTPUT.PUT_LINE('Added random close_date for crimes with status Closed and missing close_date.');

    -- Step 3: Assign random crime types where crime_type is NULL
    FOR rec IN (
        SELECT rowid AS row_id
        FROM BAD_CRIME_DATA
        WHERE crime_type IS NULL
    ) LOOP
        SELECT CASE 
            WHEN DBMS_RANDOM.VALUE(0, 5) < 1 THEN 'DRUNK AND DRIVE'
            WHEN DBMS_RANDOM.VALUE(0, 5) < 2 THEN 'Armed robbery'
            WHEN DBMS_RANDOM.VALUE(0, 5) < 3 THEN 'Blackmail'
            WHEN DBMS_RANDOM.VALUE(0, 5) < 4 THEN 'Forgery'
            ELSE 'Kidnapping'
        END
        INTO v_random_crime_type
        FROM DUAL;

        UPDATE BAD_CRIME_DATA
        SET crime_type = v_random_crime_type
        WHERE rowid = rec.row_id;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Assigned random crime types for records with NULL crime_type.');

    -- Step 4: Remove commas from region_name
    UPDATE BAD_CRIME_DATA
    SET region_name = REPLACE(region_name, ',', '')
    WHERE region_name LIKE '%,%';

    DBMS_OUTPUT.PUT_LINE('Removed commas from region_name.');

    -- Log successful completion
    DBMS_OUTPUT.PUT_LINE('Bad data corrections completed successfully.');
END FIX_BAD_DATA_DATES;


BEGIN
    FIX_BAD_DATA_DATES;
END;


CREATE OR REPLACE PROCEDURE FIX_DATA_FORMAT AS
BEGIN
    -- Step 1: Format columns in BAD_CRIME_DATA
    UPDATE BAD_CRIME_DATA
    SET crime_type = INITCAP(crime_type)
    WHERE crime_type IS NOT NULL;

    UPDATE BAD_CRIME_DATA
    SET station_name = INITCAP(station_name)
    WHERE station_name IS NOT NULL;

    UPDATE BAD_CRIME_DATA
    SET region_name = INITCAP(region_name)
    WHERE region_name IS NOT NULL;

    UPDATE BAD_CRIME_DATA
    SET officer_name = INITCAP(officer_name)
    WHERE officer_name IS NOT NULL;

    DBMS_OUTPUT.PUT_LINE('Formatted all relevant columns in BAD_CRIME_DATA.');

    -- Step 2: Format columns in GOOD_CRIME_DATA
    UPDATE GOOD_CRIME_DATA
    SET crime_type = INITCAP(crime_type)
    WHERE crime_type IS NOT NULL;

    UPDATE GOOD_CRIME_DATA
    SET station_name = INITCAP(station_name)
    WHERE station_name IS NOT NULL;

    UPDATE GOOD_CRIME_DATA
    SET region_name = INITCAP(region_name)
    WHERE region_name IS NOT NULL;

    UPDATE GOOD_CRIME_DATA
    SET officer_name = INITCAP(officer_name)
    WHERE officer_name IS NOT NULL;

    DBMS_OUTPUT.PUT_LINE('Formatted all relevant columns in GOOD_CRIME_DATA.');

    -- Log successful completion
    DBMS_OUTPUT.PUT_LINE('Data formatting completed successfully for both tables.');
END FIX_DATA_FORMAT;
/



BEGIN
    FIX_DATA_FORMAT;
END;
/




DROP TABLE GOOD_BAD_DATA
CREATE TABLE GOOD_BAD_DATA (
    GB_ID NUMBER,
    CRIME_ID NUMBER,
    REPORT_DATE DATE,
    CRIME_TYPE VARCHAR2(100),
    STATION_ID NUMBER,
    STATION_NAME VARCHAR2(100),
    REGION_ID NUMBER,
    REGION_NAME VARCHAR2(100),
    CRIME_LOCATION VARCHAR2(200),
    LEAD_OFFICER_ID NUMBER,
    OFFICER_NAME VARCHAR2(100),
    CLOSE_DATE DATE,
    CRIME_STATUS VARCHAR2(50),
    ESCALATED_FLAG VARCHAR2(1),
    CRIME_SEVERITY VARCHAR2(50),
    SOURCE_SYSTEM VARCHAR2(50),

    CONSTRAINT "PK_GOOD_BAD" PRIMARY KEY (GB_ID) ENABLE
);


DROP SEQUENCE GB_ID_SEQ

CREATE SEQUENCE "GB_ID_SEQ"
    INCREMENT BY 1
    START WITH 1
    CACHE 20
    NOORDER
    NOCYCLE;


DROP TRIGGER GB_ID_TRIGGER

CREATE OR REPLACE TRIGGER "GB_ID_TRIGGER"
    BEFORE INSERT ON GOOD_BAD_DATA
    FOR EACH ROW
BEGIN
    :NEW.GB_ID := GB_ID_SEQ.NEXTVAL;
END;


CREATE OR REPLACE PROCEDURE MERGE_CRIME_DATA_PROCEDURE(v_rows OUT NUMBER) IS
BEGIN
    MERGE INTO GOOD_BAD_DATA gbd
    USING (
        SELECT * FROM GOOD_CRIME_DATA
        UNION ALL
        SELECT * FROM BAD_CRIME_DATA
    ) src
    ON (gbd.crime_id = src.crime_id)
    WHEN MATCHED THEN
        UPDATE SET
            gbd.crime_type = INITCAP(src.crime_type),
            gbd.station_name = INITCAP(src.station_name),
            gbd.region_name = INITCAP(src.region_name),
            gbd.officer_name = INITCAP(src.officer_name),
            gbd.crime_location = src.crime_location,  
            gbd.close_date = src.close_date,
            gbd.crime_status = src.crime_status,
            gbd.escalated_flag = src.escalated_flag,
            gbd.crime_severity = src.crime_severity,
            gbd.source_system = src.source_system
    WHEN NOT MATCHED THEN
        INSERT (
            crime_id, report_date, crime_type, station_id, station_name, region_id,
            region_name, crime_location, lead_officer_id, officer_name, close_date,
            crime_status, escalated_flag, crime_severity, source_system
        )
        VALUES (
            src.crime_id, src.report_date, INITCAP(src.crime_type), src.station_id,
            INITCAP(src.station_name), src.region_id, INITCAP(src.region_name),
            src.crime_location, src.lead_officer_id, INITCAP(src.officer_name),
            src.close_date, src.crime_status, src.escalated_flag, src.crime_severity,
            src.source_system
        );

    v_rows := SQL%ROWCOUNT;

    DBMS_OUTPUT.PUT_LINE('Rows processed during merge: ' || v_rows);
END MERGE_CRIME_DATA_PROCEDURE;
/

DECLARE
    total_rows NUMBER := 0;
BEGIN
    MERGE_CRIME_DATA_PROCEDURE(total_rows);
    
    DBMS_OUTPUT.PUT_LINE('Total rows processed: ' || total_rows);
END;
/

