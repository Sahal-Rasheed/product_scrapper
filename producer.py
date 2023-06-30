import pika
import json

def push_data_to_queue(data):
    # Connecting to RabbitMq
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))  
    channel = connection.channel()  
    channel.queue_declare(queue='product_queue')  

    # Push each task to queue
    for item in data:
        task = json.dumps(item)
        channel.basic_publish(exchange='',  
                      routing_key='product_queue',  
                      body=task)  
    
    connection.close()  