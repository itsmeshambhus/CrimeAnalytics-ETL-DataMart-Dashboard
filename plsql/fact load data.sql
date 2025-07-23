DROP SEQUENCE FACT_CRIME_SEQ;

CREATE SEQUENCE FACT_CRIME_SEQ
    INCREMENT BY 1
    START WITH 1
    CACHE 20
    NOORDER
    NOCYCLE;


CREATE OR REPLACE TRIGGER FACT_CRIME_TRIGGER
    BEFORE INSERT ON FACT_CRIME
    FOR EACH ROW
BEGIN
    :NEW.FACT_CRIME_KEY := FACT_CRIME_SEQ.NEXTVAL;  -- Auto-generating the Surrogate Key
END;
/


CREATE OR REPLACE PROCEDURE PR_FACT_CRIME AS
BEGIN
    -- Insert aggregated data into FACT_CRIME table
    INSERT INTO FACT_CRIME (
        TOTAL_CRIME, 
        RESOLVED_CRIME, 
        UNRESOLVED_CRIME, 
        ESCALATED_FLAG, 
        CRIME_SEVERITY, 
        FK1_REGION_KEY, 
        FK2_TIME_KEY, 
        FK3_CRIME_TYPE_KEY, 
        FK4_POLICE_STATION_KEY, 
        FK5_CRIME_STATUS_KEY
    )
    SELECT 
        COUNT(*) AS total_crime,
        SUM(CASE WHEN dcs.crime_status = 'CLOSED' THEN 1 ELSE 0 END) AS resolved_crime,
        SUM(CASE WHEN dcs.crime_status != 'CLOSED' THEN 1 ELSE 0 END) AS unresolved_crime,
        MAX(CASE 
            WHEN dct.severity_level = 'HIGH' AND dcs.crime_status != 'CLOSED' THEN 1 
            ELSE 0 
        END) AS escalated_flag,
        CASE 
            WHEN dct.severity_level = 'HIGH' THEN 3
            WHEN dct.severity_level = 'MEDIUM' THEN 2
            WHEN dct.severity_level = 'LOW' THEN 1
            ELSE 0
        END AS crime_severity,
        dl.region_key AS FK1_REGION_KEY,
        dt.time_key AS FK2_TIME_KEY,
        dct.crime_type_key AS FK3_CRIME_TYPE_KEY,
        dps.police_station_key AS FK4_POLICE_STATION_KEY,
        dcs.crime_status_key AS FK5_CRIME_STATUS_KEY
    FROM 
        TRANSFORM_LOCATION tl
    JOIN TRANSFORM_POLICE_STATION tps ON tl.region_id = tps.region_id
    JOIN TRANSFORM_CRIME_TYPE tct ON tps.region_id = tct.region_id  
    JOIN TRANSFORM_CRIME_STATUS tcs ON tct.crime_id = tcs.crime_status_id  
    JOIN TRANSFORM_TIME tt ON tct.crime_id = tt.crime_id  
    JOIN DIM_LOCATION dl ON tl.region_key = dl.region_key
    JOIN DIM_POLICE_STATION dps ON tps.station_key = dps.police_station_key
    JOIN DIM_CRIME_TYPE dct ON tct.crime_type_key = dct.crime_type_key
    JOIN DIM_CRIME_STATUS dcs ON tcs.crime_status_key = dcs.crime_status_key
    JOIN DIM_TIME dt ON tt.date_id = dt.time_key
    GROUP BY 
        dl.region_key,
        dt.time_key,
        dct.crime_type_key,
        dps.police_station_key,
        dcs.crime_status_key,
        dct.severity_level;
    
    COMMIT;  
END PR_FACT_CRIME;
/

begin
    PR_FACT_CRIME;
end;