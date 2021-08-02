from src.main.python.create_populate_dimensions import dimension_location, dimension_category, dimension_order_detail, \
    dimension_order, dimension_product, dimension_shippers, dimension_suppliers, dimension_customers, \
    dimensions_employees, fact_create, drop_tables, alter_references, alter_foreign
from src.main.python.update_fact_dimensions import update_dimension_shippers, update_dim_location, \
    update_dim_categories, update_dim_orders, update_dim_products, update_dim_customers, update_dim_suppliers, \
    update_dim_employees, update_fact, update_dim_order_details


def create_dimensions():
    drop_tables()
    dimension_category()
    dimension_order()
    dimension_order_detail()
    dimension_product()
    dimension_shippers()
    dimension_suppliers()
    dimension_customers()
    dimensions_employees()
    dimension_location()
    alter_references()
    fact_create()
    alter_foreign()


def update_values():
    update_dimension_shippers()
    update_dim_location()
    update_dim_categories()
    update_dim_order_details()
    update_dim_orders()
    update_dim_products()
    update_dim_customers()
    update_dim_suppliers()
    update_dim_employees()
    update_fact()
