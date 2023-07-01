import sqlite3
import requests
from celery import Celery

# Celery Implementation
app = Celery('product_tasks', broker='amqp://guest@localhost//', backend='rpc://')

@app.task
def get_product_data():
    api_url = "https://my.api.mockaroo.com/product_data?key=6ad64af0"
    response = requests.get(api_url)
    return response.json()

@app.task
def process_product_data(product_data):
    if product_data:
        # Connect to the SQLite db
        db_conn = sqlite3.connect('products.db')
        cursor = db_conn.cursor()

        for product in product_data:
            product_name = product.get('product_name')
            description = product.get('description')
            price = product.get('price')

            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='products'")
            table = cursor.fetchone()
        
            if not table:
                cursor.execute('CREATE TABLE products (id INTEGER PRIMARY KEY,product_name TEXT,description TEXT,price REAL)')

            query = "INSERT INTO Products (product_name, description, price) VALUES (?, ?, ?)"
            cursor.execute(query, (product_name, description, price))

        db_conn.commit()
        db_conn.close()

# if __name__ == '__main__':
#     app.worker_main(['worker','--loglevel=info'])