import pika
import json
import sqlite3

def get_data_from_queue():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))  
    channel = connection.channel()  

    channel.queue_declare(queue='product_queue')  

    def callback(ch, method, properties, body):
        task_data = json.loads(body)

        # Connect to the SQLite database
        db_conn = sqlite3.connect('products.db')
        cursor = db_conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='products'")
        table = cursor.fetchone()
    
        if not table:
            cursor.execute('CREATE TABLE products (id INTEGER PRIMARY KEY,product_name TEXT,description TEXT,price REAL)')

        query = "INSERT INTO Products (product_name, description, price) VALUES (?, ?, ?)"
        cursor.execute(query, (task_data['product_name'], task_data['description'], task_data['price']))

        db_conn.commit()
        db_conn.close()

        ch.basic_ack(delivery_tag=method.delivery_tag)

     
    channel.basic_consume(queue='product_queue', on_message_callback=callback)

    try:
        print("Consumer is waiting for messages...")
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Consumer process terminated.")
