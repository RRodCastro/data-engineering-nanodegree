DROP TABLE IF EXISTS public.visa_codes, public.us_cities_demographics, public.states_codes, public.country_codes, public.airport_codes, public.immigration;

CREATE TABLE IF NOT EXISTS public.visa_codes (
    code varchar(45),
    type varchar(20)
);

CREATE TABLE IF NOT EXISTS public.us_cities_demographics  (
    city VARCHAR,
    state VARCHAR,
    median_age FLOAT,
    male_population FLOAT,
    female_population FLOAT,
    total_population FLOAT,
    number_of_veterans FLOAT,
    foreign_born FLOAT,
    average_household_size FLOAT,
    state_code VARCHAR,
    race VARCHAR,
    count INT
);

CREATE TABLE IF NOT EXISTS public.states_codes  (
	code VARCHAR,
    state VARCHAR
);

CREATE TABLE IF NOT EXISTS public.country_codes  (
	code VARCHAR,
    country VARCHAR
);

CREATE TABLE IF NOT EXISTS public.airport_codes  (
	ident VARCHAR,
   	type VARCHAR,
   	name VARCHAR,
   	elevation_ft VARCHAR,
   	continent VARCHAR,
    iso_country VARCHAR,
    iso_region VARCHAR,
    municipality VARCHAR,
    gps_code VARCHAR,
    iata_code VARCHAR,
    local_code VARCHAR,
    lat FLOAT,
    long FLOAT
);

CREATE TABLE IF NOT EXISTS public.immigration   (
	cicid FLOAT,
    i94yr FLOAT,
    i94mon FLOAT,
    i94cit FLOAT,
    i94res FLOAT,
    i94port VARCHAR,
    arrdate FLOAT,
    i94mode FLOAT,
    i94addr VARCHAR,
    depdate FLOAT,
    i94bir FLOAT,
    i94visa FLOAT,
    count FLOAT,
    dtadfile VARCHAR,
    visapost VARCHAR,
    occup VARCHAR,
    entdepa VARCHAR,
    entdepd VARCHAR,
    entdepu VARCHAR,
    matflag VARCHAR,
    biryear FLOAT,
    dtaddto VARCHAR,
    gender VARCHAR,
    insnum VARCHAR,
    airline VARCHAR,
    admnum FLOAT,
    fltno VARCHAR,
    visatype VARCHAR
)


