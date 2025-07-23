CREATE OR REPLACE PACKAGE crime_data_pkg AS
    PROCEDURE Insert_Data_To_Staging_PRCS;
    PROCEDURE Insert_Data_To_Staging_PS_Wales;
    PROCEDURE COPY_GOOD_BAD_DATA;
    PROCEDURE FIX_BAD_DATA_DATES;
    PROCEDURE FIX_DATA_FORMAT;
END crime_data_pkg;
/



CREATE OR REPLACE PACKAGE BODY crime_data_pkg AS

    -- Procedure to insert data from PRCS system into the staging table
    PROCEDURE Insert_Data_To_Staging_PRCS AS
    BEGIN
        INSERT INTO STAGING_CRIME_DATA (
            STAGING_CRIME_ID,
            CRIME_ID,
            REPORT_DATE,
            CRIME_TYPE,
            STATION_ID,
            STATION_NAME,
            LEAD_OFFICER_ID,
            OFFICER_NAME,
            CLOSE_DATE,
            ESCALATED_FLAG,
            SOURCE_SYSTEM
        )
        SELECT 
            STAGING_CRIME_SEQ.NEXTVAL,
            pr.reported_crime_id, 
            pr.date_reported,
            ct.crime_type_desc,
            s.station_id,
            s.station_name,
            pe.emp_id,
            pe.emp_name,
            pr.date_closed,
            CASE WHEN pr.crime_status = 'Escalated' THEN 'Y' ELSE 'N' END,
            'PRCS'
        FROM 
            pl_reported_crime pr
        JOIN 
            pl_crime_type ct ON pr.fk1_crime_type_id = ct.crime_type_id
        JOIN 
            pl_station s ON pr.fk2_station_id = s.station_id
        JOIN 
            pl_police_employee pe ON pe.emp_id = pr.fk1_crime_type_id
        WHERE
            pr.date_reported BETWEEN TO_DATE('2024-01-01', 'YYYY-MM-DD') AND TO_DATE('2024-12-31', 'YYYY-MM-DD');

        COMMIT;
    END Insert_Data_To_Staging_PRCS;

    -- Procedure to insert data from PS_Wales system into the staging table
    PROCEDURE Insert_Data_To_Staging_PS_Wales AS
    BEGIN
        INSERT INTO STAGING_CRIME_DATA (
            STAGING_CRIME_ID,
            CRIME_ID,
            REPORT_DATE,
            CRIME_TYPE,
            STATION_ID,
            STATION_NAME,
            LEAD_OFFICER_ID,
            OFFICER_NAME,
            CLOSE_DATE,
            ESCALATED_FLAG,
            SOURCE_SYSTEM
        )
        SELECT 
            STAGING_CRIME_SEQ.NEXTVAL,
            pr.reported_crime_id,
            pr.date_reported,
            ct.crime_type_desc,
            s.station_id,
            s.station_name,
            pe.emp_id,
            pe.emp_name,
            pr.date_closed,
            CASE WHEN pr.crime_status = 'Escalated' THEN 'Y' ELSE 'N' END,
            'PS_Wales'
        FROM 
            ps_reported_crime pr
        JOIN 
            ps_crime_type ct ON pr.fk1_crime_type_id = ct.crime_type_id
        JOIN 
            ps_station s ON pr.fk2_station_id = s.station_id
        JOIN 
            ps_police_employee pe ON pe.emp_id = pr.fk1_crime_type_id
        WHERE
            pr.date_reported BETWEEN TO_DATE('2024-01-01', 'YYYY-MM-DD') AND TO_DATE('2024-12-31', 'YYYY-MM-DD');

        COMMIT;
    END Insert_Data_To_Staging_PS_Wales;

    -- Procedure to separate good and bad data from the staging table
    PROCEDURE COPY_GOOD_BAD_DATA AS
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

    -- Procedure to fix bad data dates (close_date and report_date)
    PROCEDURE FIX_BAD_DATA_DATES AS
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
            -- Randomly assign a crime type
            SELECT CASE 
                WHEN DBMS_RANDOM.VALUE(0, 5) < 1 THEN 'DRUNK AND DRIVE'
                WHEN DBMS_RANDOM.VALUE(0, 5) < 2 THEN 'Armed robbery'
                WHEN DBMS_RANDOM.VALUE(0, 5) < 3 THEN 'Blackmail'
                WHEN DBMS_RANDOM.VALUE(0, 5) < 4 THEN 'Forgery'
                ELSE 'Kidnapping'
            END
            INTO v_random_crime_type
            FROM DUAL;

            -- Update the record with the random crime type
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

    -- Procedure to format data in GOOD_CRIME_DATA and BAD_CRIME_DATA
    PROCEDURE FIX_DATA_FORMAT AS
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

END crime_data_pkg;
/
