from src.main.python.connection_database.connection import connect_database, connect_database_dw, connection_dw_url, \
    connection_url
from src.main.python.operation_db import read_table, update_values_dimension

connection = connect_database()
connection_dw = connect_database_dw()
engine_dw = connection_dw_url()
engine = connection_url()


def update_dimension_shippers():
    query_origen = """select shipper_id, company_name, phone from shippers"""
    df_origen = read_table(query_origen, connection)

    query_dw = """select shipper_id, company_name, phone from dim_shippers"""
    df_dw = read_table(query_dw, connection_dw)

    diff = df_origen[~df_origen.isin(df_dw)].dropna() # comparação entre as tabelas
    update_values_dimension(diff, engine_dw, 'dim_shippers', False, 'append') #append na diff


def update_dim_location():
    query_origen = """select employee_territories.employee_id
                    ,territories.territory_id
                    ,region.region_id
            from employee_territories
            left join territories ON territories.territory_id = employee_territories.territory_id
            left join region ON region.region_id = territories.region_id;"""
    df_origen = read_table(query_origen, connection)

    query_dw = """select * from dim_location"""
    df_dw = read_table(query_dw, connection_dw)

    diff = df_origen[~df_origen.isin(df_dw)].dropna()
    update_values_dimension(diff, engine_dw, 'dim_location', False, 'append')


def update_dim_categories():
    query_origen = """select category_id, category_name,category_name,description from categories"""
    df_origen = read_table(query_origen, connection)

    query_dw = """select * from dim_categories"""
    df_dw = read_table(query_dw, connection_dw)

    diff = df_origen[~df_origen.isin(df_dw)].dropna()
    update_values_dimension(diff, engine_dw, 'dim_categories', False, 'append')


def update_dim_order_details():
    query_origen = """select order_id,product_id,unit_price, quantity, discount from order_details"""
    df_origen = read_table(query_origen, connection)

    query_dw = """select * from dim_order_details"""
    df_dw = read_table(query_dw, connection_dw)

    diff = df_origen[~df_origen.isin(df_dw)].dropna()
    update_values_dimension(diff, engine_dw, 'dim_order_details', False, 'append')


def update_dim_orders():
    query_origen = """select * from orders"""
    df_origen = read_table(query_origen, connection)

    query_dw = """select * from dim_orders"""
    df_dw = read_table(query_dw, connection_dw)

    diff = df_origen[~df_origen.isin(df_dw)].dropna()
    update_values_dimension(diff, engine_dw, 'dim_orders', False, 'append')


def update_dim_products():
    query_origen = """select product_id,
                        product_name,
                        supplier_id,
                        category_id,
                        quantity_per_unit,
                        unit_price,
                        units_in_stock,
                        units_on_order,
                        reorder_level,
                        discontinued from products"""
    df_origen = read_table(query_origen, connection)

    query_dw = """select * from dim_products"""
    df_dw = read_table(query_dw, connection_dw)

    diff = df_origen[~df_origen.isin(df_dw)].dropna()
    update_values_dimension(diff, engine_dw, 'dim_products', False, 'append')


def update_dim_customers():
    query_origen = """select * from customers"""
    df_origen = read_table(query_origen, connection)

    query_dw = """select * from dim_customers"""
    df_dw = read_table(query_dw, connection_dw)

    diff = df_origen[~df_origen.isin(df_dw)].dropna()
    update_values_dimension(diff, engine_dw, 'dim_customers', False, 'append')


def update_dim_suppliers():
    query_origen = """select * from suppliers"""
    df_origen = read_table(query_origen, connection)

    query_dw = """select * from dim_suppliers"""
    df_dw = read_table(query_dw, connection_dw)

    diff = df_origen[~df_origen.isin(df_dw)].dropna()
    update_values_dimension(diff, engine_dw, 'dim_suppliers', False, 'append')


def update_dim_employees():
    query_origen = """select * from employees"""
    df_origen = read_table(query_origen, connection)

    query_dw = """select * from dim_employees"""
    df_dw = read_table(query_dw, connection_dw)

    diff = df_origen[~df_origen.isin(df_dw)].dropna()
    update_values_dimension(diff, engine_dw, 'dim_employees', False, 'append')


def update_fact():
    query_origen = """select orders.order_id
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
            left join shippers ON shippers.shipper_id = orders.ship_via;"""
    df_origen = read_table(query_origen, connection)

    query_dw = """select * from fact_orders"""
    df_dw = read_table(query_dw, connection_dw)

    diff = df_origen[~df_origen.isin(df_dw)].dropna()
    update_values_dimension(diff, engine_dw, 'fact_orders', False, 'append')
