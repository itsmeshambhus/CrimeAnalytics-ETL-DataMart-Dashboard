CREATE OR REPLACE TRIGGER DIM_LOCATION_TRIGGER
    BEFORE INSERT ON DIM_LOCATION
    FOR EACH ROW
BEGIN
    :NEW.region_key := DIM_LOCATION_SEQ.NEXTVAL;  
END;


CREATE OR REPLACE PROCEDURE datadim_location
AS
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
END;

BEGIN
    datadim_location;
END;

------------

CREATE OR REPLACE TRIGGER DIM_TIME_TRIGGER
    BEFORE INSERT ON DIM_TIME
    FOR EACH ROW
    BEGIN
        :NEW.time_key := DIM_TIME_SEQ.NEXTVAL;  
    END;

CREATE OR REPLACE PROCEDURE datadim_time
AS
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
END;
/

BEGIN
    datadim_time;
END;

--------------------


CREATE OR REPLACE TRIGGER DIM_CRIME_TYPE_TRIGGER
    BEFORE INSERT ON DIM_CRIME_TYPE
    FOR EACH ROW
BEGIN
    :NEW.crime_type_key := DIM_CRIME_TYPE_SEQ.NEXTVAL;  
END;


CREATE OR REPLACE PROCEDURE datadim_crime_type
AS
BEGIN
    MERGE INTO DIM_CRIME_TYPE D
    USING (
        SELECT DISTINCT crime_id, crime_type, crime_category, severity_level, source_system
        FROM TRANSFORM_CRIME_TYPE
    ) T
    ON (D.crime_id = T.crime_id AND D.crime_type_name = T.crime_type AND D.crime_category = T.crime_category)

    WHEN NOT MATCHED THEN
        INSERT (crime_id, crime_type_name, crime_category, severity_level, data_source)
        VALUES (T.crime_id, T.crime_type, T.crime_category, T.severity_level, T.source_system);

    COMMIT;
END;

BEGIN
    datadim_crime_type;
END;

----------------
CREATE OR REPLACE TRIGGER DIM_POLICE_STATION_TRIGGER
    BEFORE INSERT ON DIM_POLICE_STATION
    FOR EACH ROW
BEGIN
    :NEW.police_station_key := DIM_POLICE_STATION_SEQ.NEXTVAL;  
END;


CREATE OR REPLACE PROCEDURE datadim_police_station
AS
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
END;

BEGIN
    datadim_police_station;
END;

------------------


CREATE OR REPLACE PROCEDURE datadim_crime_status
AS
BEGIN
    -- Insert unique records from TRANSFORM_CRIME_STATUS that don't exist in DIM_CRIME_STATUS
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
END;

BEGIN
    datadim_crime_status;
END;



------
drop sequence FACT_CRIME_SEQ
CREATE SEQUENCE FACT_CRIME_SEQ
    START WITH 1
    INCREMENT BY 1
    CACHE 20
    NOCYCLE;

CREATE OR REPLACE TRIGGER FACT_CRIME_TRIGGER
    BEFORE INSERT ON FACT_CRIME
    FOR EACH ROW
BEGIN
    :NEW.fact_crime_key := FACT_CRIME_SEQ.NEXTVAL;
END;
/


CREATE OR REPLACE PROCEDURE Populate_FACT_CRIME
AS
BEGIN
    MERGE INTO FACT_CRIME F
    USING (
        SELECT 
            T.date_id AS fk2_time_key, 
            L.region_key AS fk1_region_key, 
            P.station_key AS fk4_police_station_key, 
            CT.crime_type_key AS fk3_crime_type_key, 
            CS.crime_status_key AS fk5_crime_status_key,
            COUNT(*) AS total_crime,
            SUM(CASE WHEN CS.crime_status = 'Resolved' THEN 1 ELSE 0 END) AS resolved_crime,
            SUM(CASE WHEN CS.crime_status = 'Unresolved' THEN 1 ELSE 0 END) AS unresolved_crime,
            SUM(CASE WHEN CS.crime_status = 'Escalated' THEN 1 ELSE 0 END) AS escalated_flag,
            MAX(CT.severity_level) AS crime_severity
        FROM TRANSFORM_TIME T
        INNER JOIN TRANSFORM_LOCATION L ON L.region_id = T.crime_id  
        INNER JOIN TRANSFORM_POLICE_STATION P ON P.station_id = T.crime_id  
        INNER JOIN TRANSFORM_CRIME_TYPE CT ON CT.crime_id = T.crime_id  
        INNER JOIN TRANSFORM_CRIME_STATUS CS ON CS.crime_status_id = T.crime_id  
        GROUP BY 
            T.date_id, L.region_key, P.station_key, CT.crime_type_key, CS.crime_status_key
    ) T
    ON (
        F.fk2_time_key = T.fk2_time_key 
        AND F.fk1_region_key = T.fk1_region_key
        AND F.fk4_police_station_key = T.fk4_police_station_key
        AND F.fk3_crime_type_key = T.fk3_crime_type_key
        AND F.fk5_crime_status_key = T.fk5_crime_status_key
    )
    WHEN MATCHED THEN
        UPDATE SET
            F.total_crime = T.total_crime,
            F.resolved_crime = T.resolved_crime,
            F.unresolved_crime = T.unresolved_crime,
            F.escalated_flag = T.escalated_flag,
            F.crime_severity = T.crime_severity
    WHEN NOT MATCHED THEN
        INSERT (
            fk1_region_key, fk2_time_key, fk3_crime_type_key, 
            fk4_police_station_key, fk5_crime_status_key, total_crime, resolved_crime, 
            unresolved_crime, escalated_flag, crime_severity
        )
        VALUES (
            T.fk1_region_key, T.fk2_time_key, T.fk3_crime_type_key, 
            T.fk4_police_station_key, T.fk5_crime_status_key, T.total_crime, 
            T.resolved_crime, T.unresolved_crime, T.escalated_flag, T.crime_severity
        );

    COMMIT;
END;
/


BEGIN
    Populate_FACT_CRIME;
END;

CREATE OR REPLACE PROCEDURE POPULATE_FACT_CRIME AS
BEGIN
    FOR rec IN (
        SELECT
            t.time_key,
            c.crime_type_key,
            p.police_station_key,
            s.crime_status_key,
            SUM(CASE WHEN s.crime_status = 'Resolved' THEN 1 ELSE 0 END) AS resolved_crime,
            SUM(CASE WHEN s.crime_status = 'Unresolved' THEN 1 ELSE 0 END) AS unresolved_crime,
            COUNT(*) AS total_crime,
            CASE 
                WHEN COUNT(*) > 100 THEN 1 
                ELSE 0
            END AS escalated_flag,
            CASE 
                WHEN ct.severity_level = 'High' THEN 3
                WHEN ct.severity_level = 'Medium' THEN 2
                WHEN ct.severity_level = 'Low' THEN 1
                ELSE 0
            END AS crime_severity
        FROM
            DIM_TIME t
        JOIN
            DIM_CRIME_TYPE ct ON t.time_key = ct.time_key
        JOIN
            DIM_POLICE_STATION p ON ct.crime_type_key = p.crime_type_key
        JOIN
            DIM_CRIME_STATUS s ON p.police_station_key = s.police_station_key
        -- Join other tables if necessary (like DIM_LOCATION, etc.)
        GROUP BY
            t.time_key,
            ct.crime_type_key,
            p.police_station_key,
            s.crime_status_key
    ) LOOP
        -- Insert into the FACT_CRIME table
        INSERT INTO FACT_CRIME (
            fact_crime_key,
            total_crime,
            resolved_crime,
            unresolved_crime,
            escalated_flag,
            crime_severity,
            fk1_region_key, 
            fk2_time_key, 
            fk3_crime_type_key, 
            fk4_police_station_key, 
            fk5_crime_status_key 
        )
        VALUES (
            FACT_CRIME_SEQ.NEXTVAL,  
            rec.total_crime,
            rec.resolved_crime,
            rec.unresolved_crime,
            rec.escalated_flag,
            rec.crime_severity,
            rec.region_key, 
            rec.time_key,
            rec.crime_type_key,
            rec.police_station_key,
            rec.crime_status_key
        );
    END LOOP;
    COMMIT;
END;
