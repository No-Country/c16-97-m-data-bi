from pathlib import Path

OLIST = 'olistbr/'
BRAZILIAN_ECOMMERCE = 'brazilian-ecommerce'
MARKETING_FUNNEL = 'marketing-funnel-olist'
OLIST_BRAZILIAN_ECOMMERCE = OLIST + BRAZILIAN_ECOMMERCE

ROOT = Path('.').resolve()
DATA_RAW_DIR = ROOT.joinpath('data', 'raw')

PROCESSED_DATA_DIR = ROOT.joinpath('data', 'processed')

# data parquet files
CUSTOMERS = DATA_RAW_DIR / BRAZILIAN_ECOMMERCE / 'olist_customers_dataset.parquet'
GEOLOCATION = DATA_RAW_DIR / BRAZILIAN_ECOMMERCE / 'olist_geolocation_dataset.parquet'
ORDERS = DATA_RAW_DIR / BRAZILIAN_ECOMMERCE / 'olist_orders_dataset.parquet'
ORDER_REVIEWS = DATA_RAW_DIR / BRAZILIAN_ECOMMERCE / 'olist_order_reviews_dataset.parquet'
ORDER_PAYMENTS = DATA_RAW_DIR / BRAZILIAN_ECOMMERCE / 'olist_order_payments_dataset.parquet'
ORDER_ITEMS = DATA_RAW_DIR / BRAZILIAN_ECOMMERCE / 'olist_order_items_dataset.parquet'
PRODUCTS = DATA_RAW_DIR / BRAZILIAN_ECOMMERCE / 'olist_products_dataset.parquet'
PRODUCTS_CATEGORY_NAME_TRANSLATION = DATA_RAW_DIR / BRAZILIAN_ECOMMERCE / 'olist_products_category_name_translation.parquet'
SELLERS = DATA_RAW_DIR / BRAZILIAN_ECOMMERCE / 'olist_sellers_dataset.parquet'
