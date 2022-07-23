-- THE FOLLOWING TABLES HAVE BEEN IMPORTED TO THE ORACLE DATABASE USING CSV FILES

select * from dp_drivers;
select * from dp_constructors;
select * from dp_circuits;

-- Information is repeated unnecessarily in drivers and circuits table.
-- 1) Create a new table that consists of race details
-- 2) Drop columns from drivers and circuits table that are no longer needed
-- 3) Remove special characters from columns of drivers and circuits table
-- 4) Replace \N values with null and change datatype of the column
-- 4) Perform analysis on the data

-- 1) New table
CREATE TABLE DP_RACE_DETAILS 
(
  RACEID NUMBER(38) NOT NULL ,
  DRIVER_NUMBER VARCHAR2(26),
  DRIVER_CODE VARCHAR2(26),
  RESULTID NUMBER(38),
  result_number NUMBER(38), 
  START_POSITION NUMBER(38),
  FINAL_POSITION VARCHAR2(26),
  POSITIONORDER NUMBER(38),
  points NUMBER(38),
  time VARCHAR2(26),
  fastestlap VARCHAR2(26),
  rank VARCHAR2(26),
  fastestlaptime VARCHAR2(26),
  fastestlapspeed VARCHAR2(26),
  status VARCHAR2(26),
  race_name VARCHAR2(64),
  race_round NUMBER(38),
  race_date DATE,
  race_time VARCHAR2(26),
  year NUMBER(38),
  race_url VARCHAR2(128),
  season_url VARCHAR2(128),
  circuit_reference VARCHAR2(26)
  
);

    INSERT INTO dp_race_details (
        raceid,
        driver_number,
        driver_code,
        resultid,
        result_number, 
        start_position,
        final_position,
        positionorder,
        points,
        time,
        fastestlap,
        rank,
        fastestlaptime,
        fastestlapspeed,
        status,
        race_name,
        race_round,
        race_date,
        race_time,
        year,
        race_url,
        season_url,
        circuit_reference
    )
        SELECT
            dr.raceid,
            dr.driver_number,
            dr.driver_code,
            dr.resultid,
            dr.result_number, 
            dr.start_position,
            dr.final_position,
            dr.positionorder,
            dr.points,
            dr.time,
            dr.fastestlap,
            dr.rank,
            dr.fastestlaptime,
            dr.fastestlapspeed,
            dr.status,
            dc.race_name,
            dc.race_round,
            dc.race_date,
            dc.race_time,
            dc.year,
            dc.race_url,
            dc.season_url,
            dc.circuit_reference
        FROM
                 dp_drivers dr
            INNER JOIN dp_circuits dc ON dr.raceid = dc.raceid;
            
select * from dp_race_details;

--2)
--To drop multiple columns we create a stored procedure to help us
CREATE OR REPLACE PROCEDURE dp_drop_multi_columns (
    table_name   VARCHAR2,
    column_names VARCHAR2
) IS
BEGIN
DBMS_OUTPUT.PUT_LINE( 'ALTER TABLE '|| table_name|| ' DROP ('||COLUMN_NAMES||') ') ;
    EXECUTE IMMEDIATE 'ALTER TABLE '|| table_name|| ' DROP ('||COLUMN_NAMES||') ';
END dp_drop_multi_columns;
      

-- Dropping tables from drivers and circuits
SET SERVEROUTPUT ON;
EXEC dp_drop_multi_columns('dp_drivers', 'raceid,resultid,result_number,start_position,final_position,positiontext,positionorder,points,time,fastestlap,rank,fastestlaptime,fastestlapspeed,status');
EXEC dp_drop_multi_columns('dp_circuits', 'raceid,race_name,race_round,race_date,race_time,year,race_url,season_url');


--3) To remove duplicate values we create a stored procedure to achieve it

create or replace procedure dp_delete_duplicate_data(
    table_name   VARCHAR2,
    column_names VARCHAR2
)
is

begin

DBMS_OUTPUT.PUT_LINE('DELETE FROM '|| table_name ||' where rowid not in ( select max(rowid) from '|| table_name ||' GROUP BY '|| column_names || ' )') ;
EXECUTE IMMEDIATE 
'DELETE FROM '|| table_name ||
' where rowid not in 
    ( select 
        max(rowid) 
    from '|| 
        table_name ||
    ' GROUP BY '||
        column_names || 
    ' )';

end;

-- Removing the duplicate data from following columns
SET SERVEROUTPUT ON;
EXEC dp_delete_duplicate_data('dp_drivers', 'driverref, driver_number, driver_code,driver_dob');
EXEC dp_delete_duplicate_data('dp_circuits', 'circuit_reference, circuit_name, circuit_location');

/

-- Creating Stored Procedure to help remove special characters
CREATE OR REPLACE PROCEDURE dp_remove_all_special_characters (
    table_name VARCHAR2, column_name VARCHAR2
) 
IS 
  query2 varchar2(50) := ' = regexp_replace(regexp_replace(';
  query3 varchar2(50) := ' , ''[^A-Za-z0-9 ]'', ''''), '' {2,}'', '' '') ';

BEGIN
    DBMS_OUTPUT.PUT_LINE(' update '|| table_name ||' set ' ||column_name || query2 || column_name || query3) ;
    EXECUTE IMMEDIATE ' update '|| table_name || ' set ' ||column_name || query2 || column_name || query3;

END dp_remove_all_special_characters;

--Removing special characters from following columns
SET SERVEROUTPUT ON;
EXEC dp_remove_all_special_characters('dp_drivers','driver_forename')
EXEC dp_remove_all_special_characters('dp_drivers','driver_surname')
EXEC dp_remove_all_special_characters('dp_circuits','circuit_name')
EXEC dp_remove_all_special_characters('dp_circuits','circuit_location')

/

-- 4) replace missing values
CREATE OR REPLACE PROCEDURE dp_replace_values (
    old_value varchar2, 
    new_value varchar2, 
    column_name varchar2,
    table_name varchar2
) 
IS
BEGIN
  EXECUTE IMMEDIATE 'UPDATE '|| table_name ||' ' ||
                    '   SET ' || column_name || ' = :1 WHERE ' || column_name || ' = :2 '
    USING new_value, old_value;
END;

--Replacing placeholder \N value with NULL
EXEC dp_replace_values('\N',NULL,'driver_number','dp_drivers');
EXEC dp_replace_values('\N',NULL,'driver_code','dp_drivers');
EXEC dp_replace_values('\N',NULL,'DRIVER_NUMBER','DP_RACE_DETAILS');
EXEC dp_replace_values('\N',NULL,'DRIVER_CODE','DP_RACE_DETAILS');
EXEC dp_replace_values('\N',NULL,'FINAL_POSITION','DP_RACE_DETAILS');
EXEC dp_replace_values('\N',NULL,'TIME','DP_RACE_DETAILS');
EXEC dp_replace_values('\N',NULL,'FASTESTLAP','DP_RACE_DETAILS');
EXEC dp_replace_values('\N',NULL,'RANK','DP_RACE_DETAILS');
EXEC dp_replace_values('\N',NULL,'FASTESTLAPTIME','DP_RACE_DETAILS');
EXEC dp_replace_values('\N',NULL,'FASTESTLAPSPEED','DP_RACE_DETAILS');
EXEC dp_replace_values('\N',NULL,'RACE_TIME','DP_RACE_DETAILS');


/


CREATE OR REPLACE PROCEDURE CONVERT_COLUMN_TO_NUMBER(
    table_name varchar2,
    existing_column_name varchar2,
    new_column_name varchar2,
    new_column_dtype varchar2
)
IS
BEGIN
-- ADDING NEW COLUMN
EXECUTE IMMEDIATE 'ALTER TABLE
   '|| table_name || '
ADD
(
    ' || new_column_name || ' ' || new_column_dtype || '
) ';

-- COPYING VALUE FROM OLD TO NEW CLUMN
EXECUTE IMMEDIATE ' UPDATE '|| table_name || ' set ' || new_column_name || ' = cast(' || existing_column_name || ' as int) ';
                        
-- DROPPING OLD COLUMN
EXECUTE IMMEDIATE ' ALTER TABLE '|| table_name ||' DROP COLUMN '|| existing_column_name ||' ';

--RENAMING NEW COLUMN TO OLD COLUMN
EXECUTE IMMEDIATE ' ALTER TABLE '|| table_name ||' RENAME COLUMN ' || new_column_name || ' TO ' || existing_column_name ; 

END;

-- Changing Datatype of the following columns
EXEC CONVERT_COLUMN_TO_NUMBER('DP_RACE_DETAILS','FASTESTLAPSPEED','TEMPPP','NUMBER(6,3)');
EXEC CONVERT_COLUMN_TO_NUMBER('DP_RACE_DETAILS','DRIVER_NUMBER','TEMPPP','NUMBER(6)');
EXEC CONVERT_COLUMN_TO_NUMBER('DP_RACE_DETAILS','RANK','TEMPPP','NUMBER(6)');
EXEC CONVERT_COLUMN_TO_NUMBER('DP_RACE_DETAILS','FINAL_POSITION','TEMPPP','NUMBER(3)');
EXEC CONVERT_COLUMN_TO_NUMBER('DP_RACE_DETAILS','FASTESTLAP','TEMPPP','NUMBER(6)');

-- 5) Analysis

-- IDENTIFY DRIVER WITH MOST POINTS EVERY YEAR

WITH CTE AS (
SELECT 
    DD.DRIVER_FORENAME,
    DD.DRIVER_SURNAME,
    RD.DRIVER_CODE, 
    RD.YEAR, SUM(POINTS) AS TOT_POINTS, 
    RANK() OVER(PARTITION BY RD.YEAR ORDER BY RD.YEAR ASC, SUM(RD.POINTS) DESC, RD.DRIVER_CODE) AS RANKK 

FROM 
    DP_RACE_DETAILS RD
INNER JOIN
    DP_DRIVERS DD
ON
    RD.DRIVER_CODE = DD.DRIVER_CODE
WHERE
    RD.DRIVER_CODE IS NOT NULL
GROUP BY 
    RD.DRIVER_CODE, RD.YEAR,DD.DRIVER_FORENAME,DD.DRIVER_SURNAME
)

SELECT DRIVER_FORENAME ||' '|| DRIVER_SURNAME AS DRIVER_NAME, YEAR, TOT_POINTS FROM CTE WHERE RANKK = 1 ORDER BY YEAR DESC, TOT_POINTS DESC, DRIVER_CODE;

-- Top 5 CIRCUIT LOCATIONS for racing throughout F1 history

select circuit_reference as circuit_name, total_races from
(
    select 
        circuit_reference, 
        count(circuit_reference) as total_races,
        rank() over(order by count(circuit_reference) desc) as rankk 
    from 
        dp_race_details
    group by 
        circuit_reference
    ) a
where rankk <= 5;
    

-- IDENTIFY CAR MANUFACTURER WITH MOST WINS every year

select * from dp_constructors;

with merged_data as (
select 
    dc.constructor_name, 
    dc.constructor_nationality, 
    dc.constructor_points,
    dc.wins,
    rd.year
from 
    dp_constructors dc
inner join
    DP_RACE_DETAILS rd
on 
    dc.raceid = rd.raceid
)

select 
    constructor_name, 
    constructor_nationality, 
    total_wins, 
    total_points, 
    year
    from (
        select
            constructor_name, 
            constructor_nationality, 
            sum(wins) as total_wins,
            sum(constructor_points) as total_points,
            rank() over (partition by year order by sum(wins) desc, sum(constructor_points) desc) as rankk,
            year
        from
            merged_data
        group by 
            year, constructor_name, constructor_nationality
        ) b
where rankk = 1
order by year desc, total_wins desc;
    