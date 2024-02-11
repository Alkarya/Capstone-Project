CREATE TABLE IF NOT EXISTS public.fact_sales(
    sale_id BIGINT identity(1, 1),
    order_num BIGINT,
    quantity_ordered INT,
    price_per_item DECIMAL(18,5),
    order_line_num INT,
    total_price DECIMAL(18,5),
    order_date TIMESTAMP WITHOUT TIME ZONE,
    product_id varchar(256),
    customer_name varchar(256),
    status varchar(256),
    CONSTRAINT sale_id_pkey PRIMARY KEY (sale_id)
)
DISTSTYLE KEY
DISTKEY (sale_id)
SORTKEY (order_date);


CREATE TABLE IF NOT EXISTS public.dim_date(
    order_date TIMESTAMP WITHOUT TIME ZONE,
    quarter SMALLINT,
    day SMALLINT,
    month SMALLINT,
    year SMALLINT,
    CONSTRAINT order_date_pkey PRIMARY KEY (order_date)
)
DISTSTYLE KEY
DISTKEY (order_date)
SORTKEY (order_date);


CREATE TABLE IF NOT EXISTS public.dim_customer(
    customer_id BIGINT identity(1, 1),
    customer_name VARCHAR(256),
    phone VARCHAR(256),
    address_line1 VARCHAR(256),
    address_line2 VARCHAR(256),
    city VARCHAR(256),
    postal_code VARCHAR(256),
    country VARCHAR(256),
    territory VARCHAR(256),
    first_name VARCHAR(256),
    last_name VARCHAR(256),
    CONSTRAINT customer_id_pkey PRIMARY KEY (customer_id)
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (customer_id);


CREATE TABLE IF NOT EXISTS public.dim_products(
    product_id VARCHAR(256),
    product_line VARCHAR(256),
    msrp DECIMAL(18,5),
    CONSTRAINT product_id_pkey PRIMARY KEY (product_id)
)
DISTSTYLE KEY
DISTKEY (product_id)
SORTKEY (product_id);


CREATE TABLE IF NOT EXISTS public.dim_cities(
    id BIGINT,
    city VARCHAR(256),
    city_ascii VARCHAR(256),
    lat DECIMAL(18,5),
    lng DECIMAL(18,5),
    country VARCHAR(256),
    iso2 VARCHAR(256),
    iso3 VARCHAR(256),
    admin_name VARCHAR(256),
    capital VARCHAR(256),
    population BIGINT,
    CONSTRAINT id_pkey PRIMARY KEY (id)
)
DISTSTYLE KEY
DISTKEY (id)
SORTKEY (id);