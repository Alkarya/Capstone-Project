class SqlQueries:
    sales_table_insert = ("""
        insert into public.fact_sales(order_num, quantity_ordered, price_per_item, order_line_num, total_price, order_date, product_id, customer_name, status)
        select ordernumber,
            quantityordered,
            priceeach,
            orderlinenumber,
            sales,
            orderdate::timestamp,
            productcode,
            customername,
            status
        from public.sales_data_sample;
    """)

    customer_table_insert = ("""
        insert into public.dim_customer(customer_name, phone, address_line1, address_line2, city, postal_code, country, territory, first_name, last_name)
        select distinct customername,
            phone,
            addressline1,
            addressline2,
            city,
            postalcode,
            country,
            territory,
            contactfirstname,
            contactlastname
        from public.sales_data_sample;
    """)

    products_table_insert = ("""
        insert into public.dim_products(product_id, product_line, msrp)
        select distinct productcode,
            productline,
            msrp
        from public.sales_data_sample;
    """)

    date_table_insert = ("""
        insert into public.dim_date(order_date, quarter, day, month, year)
        select distinct orderdate::timestamp,
            qtr_id,
            extract(day from orderdate::timestamp),
            month_id,
            year_id
        from public.sales_data_sample;
    """)

    cities_table_insert = ("""
        insert into public.dim_cities(id, city, city_ascii, lat, lng, country, iso2, iso3, admin_name, capital, population)
        select id,
            city,
            city_ascii,
            lat,
            lng,
            country,
            iso2,
            iso3,
            admin_name,
            capital,
            population
        from public.worldcities;
    """)