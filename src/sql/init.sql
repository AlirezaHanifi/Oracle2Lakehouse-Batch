-- Connect to the database
CONNECT bank/oracleadmin@localhost:1521/FREEPDB1;

--------------------------------------------------------------------------------
-- SECTION: Drop Existing Objects
--------------------------------------------------------------------------------
BEGIN
   EXECUTE IMMEDIATE 'DROP TRIGGER trg_employees_b_ud';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4080 THEN RAISE; END IF;
END;
/
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE job_history';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;
END;
/
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE employees';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;
END;
/
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE departments';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;
END;
/
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE locations';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;
END;
/
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE countries';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;
END;
/
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE regions';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

--------------------------------------------------------------------------------
-- SECTION: Create Tables
--------------------------------------------------------------------------------
CREATE TABLE regions (
    region_id NUMBER PRIMARY KEY,
    region_name VARCHAR2(25)
);

CREATE TABLE countries (
    country_id CHAR(2) PRIMARY KEY,
    country_name VARCHAR2(60),
    region_id NUMBER REFERENCES regions(region_id)
);

CREATE TABLE locations (
    location_id NUMBER PRIMARY KEY,
    street_address VARCHAR2(40),
    postal_code VARCHAR2(12),
    city VARCHAR2(30) NOT NULL,
    state_province VARCHAR2(25),
    country_id CHAR(2) REFERENCES countries(country_id)
);

CREATE TABLE departments (
    department_id NUMBER PRIMARY KEY,
    department_name VARCHAR2(30) NOT NULL,
    location_id NUMBER REFERENCES locations(location_id)
);

CREATE TABLE employees (
    employee_id    NUMBER PRIMARY KEY,
    first_name     VARCHAR2(50),
    last_name      VARCHAR2(50),
    email          VARCHAR2(100),
    phone_number   VARCHAR2(20),
    hire_date      DATE,
    job_title      VARCHAR2(35),
    salary         NUMBER(10, 2),
    commission_pct NUMBER(2, 2),
    manager_id     NUMBER,
    department_id  NUMBER REFERENCES departments(department_id),
    last_update_ts TIMESTAMP WITH TIME ZONE
);

CREATE TABLE job_history (
    employee_id   NUMBER,
    start_date    DATE,
    end_date      DATE,
    job_title     VARCHAR2(35),
    department_id NUMBER,
    CONSTRAINT jhist_pk PRIMARY KEY (employee_id, start_date),
    CONSTRAINT jhist_emp_fk FOREIGN KEY (employee_id) REFERENCES employees(employee_id),
    CONSTRAINT jhist_dept_fk FOREIGN KEY (department_id) REFERENCES departments(department_id)
);

ALTER TABLE employees ADD CONSTRAINT emp_manager_fk FOREIGN KEY (manager_id) REFERENCES employees(employee_id);

--------------------------------------------------------------------------------
-- SECTION: Insert Static Data
--------------------------------------------------------------------------------
INSERT INTO regions (region_id, region_name) VALUES (1, 'Europe');
INSERT INTO regions (region_id, region_name) VALUES (2, 'Americas');
INSERT INTO regions (region_id, region_name) VALUES (3, 'Asia');
INSERT INTO regions (region_id, region_name) VALUES (4, 'Middle East and Africa');

INSERT INTO countries (country_id, country_name, region_id) VALUES ('US', 'United States of America', 2);
INSERT INTO countries (country_id, country_name, region_id) VALUES ('CA', 'Canada', 2);
INSERT INTO countries (country_id, country_name, region_id) VALUES ('UK', 'United Kingdom', 1);
INSERT INTO countries (country_id, country_name, region_id) VALUES ('DE', 'Germany', 1);
INSERT INTO countries (country_id, country_name, region_id) VALUES ('IT', 'Italy', 1);
INSERT INTO countries (country_id, country_name, region_id) VALUES ('JP', 'Japan', 3);

INSERT INTO locations (location_id, street_address, postal_code, city, state_province, country_id) VALUES (1000, '1297 Via Cola di Rie', '00989', 'Roma', 'Rome', 'IT');
INSERT INTO locations (location_id, street_address, postal_code, city, state_province, country_id) VALUES (1100, '93091 Calle della Testa', '10934', 'Venice', 'Venice', 'IT');
INSERT INTO locations (location_id, street_address, postal_code, city, state_province, country_id) VALUES (1200, '2017 Shinjuku-ku', '1689', 'Tokyo', 'Tokyo Prefecture', 'JP');
INSERT INTO locations (location_id, street_address, postal_code, city, state_province, country_id) VALUES (1400, '2014 Jabberwocky Rd', '26192', 'Southlake', 'Texas', 'US');
INSERT INTO locations (location_id, street_address, postal_code, city, state_province, country_id) VALUES (1500, '2011 Interiors Blvd', '99236', 'South San Francisco', 'California', 'US');
INSERT INTO locations (location_id, street_address, postal_code, city, state_province, country_id) VALUES (1700, '2004 Charade Rd', '98199', 'Seattle', 'Washington', 'US');
INSERT INTO locations (location_id, street_address, postal_code, city, state_province, country_id) VALUES (1800, '147 Spadina Ave', 'M5V 2L7', 'Toronto', 'Ontario', 'CA');

INSERT INTO departments (department_id, department_name, location_id) VALUES (10, 'Administration', 1700);
INSERT INTO departments (department_id, department_name, location_id) VALUES (20, 'Marketing', 1800);
INSERT INTO departments (department_id, department_name, location_id) VALUES (30, 'Purchasing', 1700);
INSERT INTO departments (department_id, department_name, location_id) VALUES (40, 'Human Resources', 1500);
INSERT INTO departments (department_id, department_name, location_id) VALUES (50, 'Shipping', 1500);
INSERT INTO departments (department_id, department_name, location_id) VALUES (60, 'IT', 1400);
INSERT INTO departments (department_id, department_name, location_id) VALUES (80, 'Sales', 1500);
INSERT INTO departments (department_id, department_name, location_id) VALUES (90, 'Executive', 1700);
INSERT INTO departments (department_id, department_name, location_id) VALUES (100, 'Finance', 1700);

COMMIT;

--------------------------------------------------------------------------------
-- SECTION: Insert Generated Employee Data
--------------------------------------------------------------------------------
INSERT INTO employees (employee_id, first_name, last_name, email, phone_number, hire_date, job_title, salary, commission_pct, manager_id, department_id, last_update_ts)
VALUES (1, 'John', 'King', 'jking@oracle2lakehouse.com', '+1 650-555-0100', TRUNC((SYSTIMESTAMP AT TIME ZONE 'Asia/Tehran') - DBMS_RANDOM.VALUE(0, 2)), 'President', 300000, NULL, NULL, 90, (SYSTIMESTAMP AT TIME ZONE 'Asia/Tehran') - DBMS_RANDOM.VALUE(0, 2));

INSERT INTO employees (employee_id, first_name, last_name, email, phone_number, hire_date, job_title, salary, commission_pct, manager_id, department_id, last_update_ts)
WITH names AS (
    SELECT 2 AS emp_id, 'James' AS fname, 'Smith' AS lname, 90 AS dept_id FROM dual UNION ALL
    SELECT 3, 'Michael', 'Johnson', 60 FROM dual UNION ALL
    SELECT 4, 'William', 'Brown', 80 FROM dual UNION ALL
    SELECT 5, 'David', 'Davis', 100 FROM dual
)
SELECT
    n.emp_id, n.fname, n.lname,
    LOWER(SUBSTR(n.fname, 1, 1) || n.lname) || '@oracle2lakehouse.com',
    '+1 ' || TRUNC(DBMS_RANDOM.VALUE(200, 999)) || '-' || TRUNC(DBMS_RANDOM.VALUE(200, 999)) || '-' || TRUNC(DBMS_RANDOM.VALUE(1000, 9999)),
    TRUNC((SYSTIMESTAMP AT TIME ZONE 'Asia/Tehran') - DBMS_RANDOM.VALUE(0, 2)),
    'Vice President',
    ROUND(DBMS_RANDOM.VALUE(200000, 250000), 2),
    NULL,
    1,
    n.dept_id,
    (SYSTIMESTAMP AT TIME ZONE 'Asia/Tehran') - DBMS_RANDOM.VALUE(0, 2)
FROM names n;

INSERT INTO employees (employee_id, first_name, last_name, email, phone_number, hire_date, job_title, salary, commission_pct, manager_id, department_id, last_update_ts)
WITH emp_data AS (
    SELECT
        LEVEL + 5 AS emp_id,
        (SELECT name FROM (SELECT column_value as name FROM TABLE(sys.dbms_debug_vc2coll('Robert','Mary','Patricia','Jennifer','Linda','David','Richard','Joseph','Thomas','Charles','Susan','Jessica','Sarah','Margaret')) ORDER BY DBMS_RANDOM.VALUE) WHERE ROWNUM = 1) AS fname,
        (SELECT name FROM (SELECT column_value as name FROM TABLE(sys.dbms_debug_vc2coll('Jones','Garcia','Miller','Martinez','Hernandez','Lopez','Gonzalez','Wilson','Anderson','Taylor','Moore','Jackson','Martin','Lee')) ORDER BY DBMS_RANDOM.VALUE) WHERE ROWNUM = 1) AS lname,
        (SELECT title FROM (SELECT column_value as title FROM TABLE(sys.dbms_debug_vc2coll('Manager','Software Engineer','Data Analyst','Sales Representative','Accountant','Product Manager','HR Specialist','System Administrator')) ORDER BY DBMS_RANDOM.VALUE) WHERE ROWNUM = 1) AS job,
        (SELECT dept_id FROM (SELECT department_id as dept_id FROM departments ORDER BY DBMS_RANDOM.VALUE) WHERE ROWNUM = 1) as dept
    FROM dual
    CONNECT BY LEVEL <= 1500
)
SELECT
    emp_id, fname, lname,
    LOWER(SUBSTR(fname, 1, 1) || lname) || '@oracle2lakehouse.com',
    '+1 ' || TRUNC(DBMS_RANDOM.VALUE(200, 999)) || '-' || TRUNC(DBMS_RANDOM.VALUE(200, 999)) || '-' || TRUNC(DBMS_RANDOM.VALUE(1000, 9999)),
    TRUNC((SYSTIMESTAMP AT TIME ZONE 'Asia/Tehran') - DBMS_RANDOM.VALUE(0, 2)), -- Hire date within last 48 hours
    job,
    ROUND(DBMS_RANDOM.VALUE(50000, 150000), 2),
    DECODE(MOD(emp_id, 4), 0, ROUND(DBMS_RANDOM.VALUE(0.1, 0.4), 2), NULL),
    (SELECT employee_id FROM (SELECT employee_id FROM employees WHERE employee_id <= 5 ORDER BY DBMS_RANDOM.VALUE) WHERE ROWNUM = 1),
    dept,
    (SYSTIMESTAMP AT TIME ZONE 'Asia/Tehran') - DBMS_RANDOM.VALUE(0, 2) -- last_update_ts within last 48 hours
FROM emp_data;

COMMIT;

--------------------------------------------------------------------------------
-- SECTION: Insert Job History and Create Trigger
--------------------------------------------------------------------------------
INSERT INTO job_history (employee_id, start_date, end_date, job_title, department_id)
WITH history_base AS (
    SELECT
        employee_id,
        TRUNC((SYSTIMESTAMP AT TIME ZONE 'Asia/Tehran') - DBMS_RANDOM.VALUE(1, 2)) as s_date, -- Start date between 24-48 hours ago
        job_title,
        department_id
    FROM employees
    WHERE ROWNUM <= 300 -- Create history for 300 employees
)
SELECT
    employee_id,
    s_date as start_date,
    s_date + DBMS_RANDOM.VALUE(4, 12)/24 as end_date,
    'Junior ' || job_title,
    department_id
FROM history_base;

COMMIT;

CREATE OR REPLACE TRIGGER trg_employees_b_ud
BEFORE UPDATE ON employees
FOR EACH ROW
BEGIN
    :new.last_update_ts := (SYSTIMESTAMP AT TIME ZONE 'Asia/Tehran');
END;
/

--------------------------------------------------------------------------------
-- SECTION: Data Verification
--------------------------------------------------------------------------------
SELECT 'Regions' as table_name, COUNT(*) as record_count FROM regions
UNION ALL
SELECT 'Countries', COUNT(*) FROM countries
UNION ALL
SELECT 'Locations', COUNT(*) FROM locations
UNION ALL
SELECT 'Departments', COUNT(*) FROM departments
UNION ALL
SELECT 'Employees', COUNT(*) FROM employees
UNION ALL
SELECT 'Job History', COUNT(*) FROM job_history
ORDER BY table_name;