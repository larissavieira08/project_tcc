from src.main.python.connection_database.connection import connect_database, connect_database_dw, connection_dw_url, \
    connection_url
from src.main.python.operation_db import create_table, insert_values_dimension, read_table, execute_query

connection = connect_database()
connection_dw = connect_database_dw()
engine_dw = connection_dw_url()
engine = connection_url()


def drop_tables():
    create_query = """DROP TABLE IF EXISTS dim_location cascade;
        DROP TABLE IF EXISTS dim_categories cascade;
        DROP TABLE IF EXISTS dim_order_details cascade;
        DROP TABLE IF EXISTS dim_orders cascade;
        DROP TABLE IF EXISTS dim_products cascade;
        DROP TABLE IF EXISTS dim_shippers cascade;
        DROP TABLE IF EXISTS dim_suppliers cascade;
        DROP TABLE IF EXISTS dim_employees cascade;
        DROP TABLE IF EXISTS dim_customers cascade;
        DROP TABLE IF EXISTS fact_orders cascade;"""
    execute_query(create_query, connection_dw)


def alter_references():
    create_query = """ALTER TABLE dim_orders ADD PRIMARY KEY (order_id);
    ALTER TABLE dim_products ADD PRIMARY KEY (product_id);
    ALTER TABLE dim_categories ADD PRIMARY KEY (category_id);
    ALTER TABLE dim_shippers ADD PRIMARY KEY (shipper_id);
    ALTER TABLE dim_customers ADD PRIMARY KEY (customer_id);
    ALTER TABLE dim_employees ADD PRIMARY KEY (employee_id);
    ALTER TABLE dim_suppliers ADD PRIMARY KEY (supplier_id);"""
    execute_query(create_query, connection_dw)


def alter_foreign():
    create_query = """
    ALTER TABLE fact_orders ADD CONSTRAINT fk_order_id FOREIGN KEY (order_id) REFERENCES dim_orders(order_id);
    ALTER TABLE fact_orders ADD CONSTRAINT fk_product_id FOREIGN KEY (product_id) REFERENCES dim_products(product_id);
    ALTER TABLE fact_orders ADD CONSTRAINT fk_shipper_id FOREIGN KEY (shipper_id) REFERENCES dim_shippers(shipper_id);
    ALTER TABLE fact_orders ADD CONSTRAINT fk_customer_id FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id);
    ALTER TABLE fact_orders ADD CONSTRAINT fk_employee_id FOREIGN KEY (employee_id) REFERENCES dim_employees(employee_id);
    ALTER TABLE fact_orders ADD CONSTRAINT fk_supplier_id FOREIGN KEY (supplier_id) REFERENCES dim_suppliers(supplier_id);
    ALTER TABLE dim_location ADD CONSTRAINT fk_loc_employee_id FOREIGN KEY (employee_id) REFERENCES dim_employees(employee_id);
    ALTER TABLE dim_products ADD CONSTRAINT fk_category_id FOREIGN KEY (category_id) REFERENCES dim_categories(category_id);
    ALTER TABLE dim_order_details ADD CONSTRAINT fk_order_id FOREIGN KEY (order_id) REFERENCES dim_orders(order_id);"""
    execute_query(create_query, connection_dw)


def dimension_location():
    populate_df = """select employee_territories.employee_id
                    ,territories.territory_id
                    ,region.region_id
            from employee_territories
            left join territories ON territories.territory_id = employee_territories.territory_id
            left join region ON region.region_id = territories.region_id
            limit 10;
            """

    insert_values_dimension(read_table(populate_df, connection), engine_dw, 'dim_location', False, 'replace')


def dimension_category():
    populate_df = """select category_id, category_name,category_name,description from categories
            """
    insert_values_dimension(read_table(populate_df, connection), engine_dw, 'dim_categories', False, 'replace')


def dimension_order_detail():
    populate_df = """select order_id,product_id,unit_price, quantity, discount from order_details
            """
    insert_values_dimension(read_table(populate_df, connection), engine_dw, 'dim_order_details', False, 'replace')


def dimension_order():
    populate_df = """select * from orders
            """
    insert_values_dimension(read_table(populate_df, connection), engine_dw, 'dim_orders', False, 'replace')


def dimension_product():
    populate_df = """select product_id,
                        product_name,
                        supplier_id,
                        category_id,
                        quantity_per_unit,
                        unit_price,
                        units_in_stock,
                        units_on_order,
                        reorder_level,
                        discontinued from products
            """
    insert_values_dimension(read_table(populate_df, connection), engine_dw, 'dim_products', False, 'replace')


def dimension_shippers():
    populate_df = """select shipper_id, company_name, phone from shippers limit 3
            """
    insert_values_dimension(read_table(populate_df, connection), engine_dw, 'dim_shippers', False, 'replace')


def dimension_suppliers():
    populate_df = """select * from suppliers
            """
    insert_values_dimension(read_table(populate_df, connection), engine_dw, 'dim_suppliers', False, 'replace')


def dimension_customers():
    populate_df = """select * from customers
            """
    insert_values_dimension(read_table(populate_df, connection), engine_dw, 'dim_customers', False, 'replace')


def dimensions_employees():
    populate_df = """select * from employees
            """
    insert_values_dimension(read_table(populate_df, connection), engine_dw, 'dim_employees', False, 'replace')


def fact_create():
    populate_df = """select orders.order_id
                    ,customers.customer_id
                    ,order_details.product_id
                    ,suppliers.supplier_id
                    ,employees.employee_id
                    ,shippers.shipper_id
                    ,order_details.quantity
                    ,order_details.unit_price
                    ,order_details.quantity * order_details.unit_price as total
            FROM orders
            left join customers ON customers.customer_id = orders.customer_id
            left join order_details ON order_details.order_id = orders.order_id
            left join products ON products.product_id = order_details.product_id
            left join suppliers ON suppliers.supplier_id = products.supplier_id
            left join employees ON employees.employee_id = orders.employee_id
            left join shippers ON shippers.shipper_id = orders.ship_via;
            """
    insert_values_dimension(read_table(populate_df, connection), engine_dw, 'fact_orders', False, 'replace')
