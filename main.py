import requests
from producer import push_data_to_queue
from consumer import get_data_from_queue
from tasks import get_product_data, process_product_data


# Sending a request to our MockAPI created, to get Product data
api_url = "https://my.api.mockaroo.com/product_data?key=6ad64af0"
response = requests.get(api_url)
product_data = response.json()

celery = True

# Switch b/w celery / pika
if celery:
     data = get_product_data.delay().get()
     process_product_data.delay(data)
else:
    push_data_to_queue(product_data)
    get_data_from_queue()