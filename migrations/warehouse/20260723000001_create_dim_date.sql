CREATE TABLE IF NOT EXISTS dim_date (
    date_key  INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year      INT NOT NULL,
    month     INT NOT NULL,
    quarter   INT NOT NULL
);