import os
import psycopg2
from constants import OLIST_BRAZILIAN_ECOMMERCE
from utils.connect_db import connect_to_db

def create_database():

    connection_params = connect_to_db()

    conn = psycopg2.connect(**connection_params)
    conn.autocommit = True

    with conn.cursor() as cursor:
        cursor.execute("DROP DATABASE IF EXISTS e_brazil;")
        cursor.execute("CREATE DATABASE e_brazil;")

def create_schemas():

    connection_params = connect_to_db()
    connection_params['database'] = 'e_brazil'
    
    # Conexión a PostgreSQL
    conn = psycopg2.connect(**connection_params)
    conn.autocommit = True

    with conn.cursor() as cursor:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS finance;")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS marketing;")
    
    print("Schemas created.")

def load_marketing_tables():

    connection_params = connect_to_db()
    connection_params['database'] = 'e_brazil'
    # Conexión a PostgreSQL
    conn = psycopg2.connect(**connection_params)
    conn.autocommit = True

    with conn.cursor() as cursor:
        cursor.execute("SET search_path TO marketing;")
        # # Cargar la tabla de customers
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id VARCHAR(255) PRIMARY KEY,
                customer_unique_id VARCHAR(255) UNIQUE,
                customer_zip_code_prefix INTEGER,
                customer_city VARCHAR(255),
                customer_state VARCHAR(255)
            );
            COPY customers FROM 'C:\\Users\\aalva\\Documents\\c16-97-m-data-bi\\ETL\\data\\raw\\brazilian-ecommerce\\olist_customers_dataset.csv' WITH CSV HEADER;
        """)

        # # Cargar la tabla de órdenes
        # cursor.execute("""
        #     CREATE TABLE IF NOT EXISTS orders (
        #         order_id VARCHAR(255) PRIMARY KEY,
        #         customer_id VARCHAR(255),
        #         order_status VARCHAR(255),
        #         order_purchase_timestamp TIMESTAMP,
        #         order_approved_at TIMESTAMP,
        #         order_delivered_carrier_date TIMESTAMP,
        #         order_delivered_customer_date TIMESTAMP,
        #         order_estimated_delivery_date TIMESTAMP
        #     );
        #     COPY orders FROM '../data/raw/brazilian-ecommerce/olist_orders_dataset.csv' WITH CSV HEADER;
        # """)

        # # Cargar la tabla de revisiones de pedidos
        # cursor.execute("""
        #     CREATE TABLE IF NOT EXISTS order_reviews (
        #         review_id VARCHAR(255) PRIMARY KEY,
        #         order_id VARCHAR(255),
        #         review_score INTEGER,
        #         review_comment_title VARCHAR(255),
        #         review_comment_message TEXT,
        #         review_creation_date TIMESTAMP,
        #         review_answer_timestamp TIMESTAMP
        #     );
        #     COPY order_reviews FROM '../data/raw/brazilian-ecommerce/olist_order_reviews_dataset.csv' WITH CSV HEADER;
        # """)

        # # Cargar la tabla de productos
        # cursor.execute("""
        #     CREATE TABLE IF NOT EXISTS products (
        #         product_id VARCHAR(255) PRIMARY KEY,
        #         product_category_name VARCHAR(255),
        #         product_name_length INTEGER,
        #         product_description_length INTEGER,
        #         product_photos_qty INTEGER,
        #         product_weight_g INTEGER,
        #         product_length_cm INTEGER,
        #         product_height_cm INTEGER,
        #         product_width_cm INTEGER
        #     );
        #     COPY products FROM '../data/raw/brazilian-ecommerce/olist_products_dataset.csv' WITH CSV HEADER;
        # """)

        # # Cargar la tabla de órdenes y artículos
        # cursor.execute("""
        #     CREATE TABLE IF NOT EXISTS order_items (
        #         order_id VARCHAR(255),
        #         order_item_id INTEGER,
        #         product_id VARCHAR(255),
        #         seller_id VARCHAR(255),
        #         shipping_limit_date TIMESTAMP,
        #         price FLOAT,
        #         freight_value FLOAT
        #     );
        #     COPY order_items FROM 'C:\Users/aalva/Documents/c16-97-m-data-bi/ETL/data/raw/brazilian-ecommerce/olist_order_items_dataset.csv' WITH CSV HEADER;
        # """)

def load():
    try:
        # Crear la base de datos
        # create_database()

        # # Crear los schemas
        # create_schemas()

        # # Cargar tablas en el schema de marketing
        load_marketing_tables()

        # # Cargar tablas en el schema de finanzas
        # load_finance_schema()

        print("Carga exitosa.")
    except Exception as e:
        print(f"Error durante la carga: {str(e)}")

if __name__ == "__main__":
    load()