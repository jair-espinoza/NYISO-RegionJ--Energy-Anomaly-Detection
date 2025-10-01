CREATE TABLE IF NOT EXISTS eia_data (
    period TIMESTAMPTZ NOT NULL,
    subba TEXT NOT NULL, 
    value DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS weather_data (
    period TIMESTAMPTZ,
    city TEXT NOT NULL,
    temp FLOAT,
    humidity FLOAT, 
    wind FLOAT,
    cloud FLOAT,
    precip FLOAT, 
    PRIMARY KEY (period, city)
);
