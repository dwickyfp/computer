-- 1. Employee Table with validation
CREATE TABLE tbl_rosetta_employee (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    department VARCHAR(50) CHECK (department IN ('Sales', 'Retail', 'Management', 'Support')),
    is_active BOOLEAN DEFAULT TRUE,
    daily_target DECIMAL(10, 2) DEFAULT 500.00
);

-- 2. Transaction Table with relational constraints
CREATE TABLE tbl_rosetta_transaction (
    transaction_id SERIAL PRIMARY KEY,
    employee_id INT NOT NULL REFERENCES tbl_rosetta_employee(employee_id) ON DELETE CASCADE,
    transaction_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    amount DECIMAL(15, 2) NOT NULL CHECK (amount > 0),
    category VARCHAR(30) NOT NULL, -- e.g., 'Hardware', 'Software', 'Service'
    currency VARCHAR(3) DEFAULT 'USD'
);

CREATE TABLE tbl_rosetta_aggregate_transaction_test (
    transaction_id INT PRIMARY KEY ,
    employee_name VARCHAR(100),
    sale_amount DECIMAL(15, 2),
    -- Complex Aggregates below
    emp_total_sales_to_date DECIMAL(15, 2), -- SUM
    is_emp_max_sale BOOLEAN,                -- MAX check
    dept_avg_sale_amt DECIMAL(15, 2),       -- AVG
    distinct_categories_sold_by_emp INT,    -- COUNT DISTINCT
    sale_rank_in_dept INT                   -- RANK
);

-- 3. Aggregate Table (Target for the custom SQL)
INSERT INTO tbl_rosetta_employee (first_name, last_name, department) VALUES 
('Elena', 'Rosetta', 'Sales'),
('Marcus', 'Vance', 'Retail'),
('Sasha', 'Grey', 'Retail');

INSERT INTO tbl_rosetta_transaction (employee_id, transaction_date, amount, category) VALUES 
(1, '2026-02-14 09:00:00', 1200.50, 'Hardware'),
(2, '2026-02-14 10:30:00', 450.00, 'Software'),
(1, '2026-02-14 12:15:00', 300.25, 'Hardware'),
(3, '2026-02-14 14:00:00', 2100.00, 'Software'),
(2, '2026-02-14 15:45:00', 50.00, 'Service');

WITH enriched_data AS (
    SELECT 
        t.transaction_id,
        e.first_name AS employee_name,
        t.amount AS sale_amount,
        -- 1. SUM: Total sales for this specific employee so far
        SUM(t.amount) OVER(PARTITION BY t.employee_id) as emp_total_sales,
        -- 2. MAX: Is this the biggest sale for this employee?
        MAX(t.amount) OVER(PARTITION BY t.employee_id) as emp_max_val,
        -- 3. AVG: Average sale amount in the specific department
        AVG(t.amount) OVER(PARTITION BY e.department) as dept_avg,
        -- 4. DISTINCT COUNT: Categories covered by this employee
        -- Note: Standard window functions don't support COUNT DISTINCT, 
        -- so we use a subquery or a dense_rank trick.
        (SELECT COUNT(DISTINCT category) FROM tbl_rosetta_transaction t2 WHERE t2.employee_id = t.employee_id) as unique_cats,
        -- 5. RANK: Where does this sale rank in its department by value?
        RANK() OVER(PARTITION BY e.department ORDER BY t.amount DESC) as dept_rank
    FROM tbl_rosetta_transaction t
    JOIN pg_src_production_db.tbl_rosetta_employee e ON t.employee_id = e.employee_id
)

SELECT 
    transaction_id,
    employee_name,
    sale_amount,
    emp_total_sales AS emp_total_sales_to_date,
    (sale_amount = emp_max_val) AS is_emp_max_sale,
    ROUND(dept_avg, 2) AS dept_avg_sale_amt,
    unique_cats AS distinct_categories_sold_by_emp,
    dept_rank AS sale_rank_in_dept
FROM enriched_data;