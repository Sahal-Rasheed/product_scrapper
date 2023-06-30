import requests
from producer import push_data_to_queue
from consumer import get_data_from_queue

# Sending a request to our MockAPI created, to get Product data
api_url = "https://my.api.mockaroo.com/product_data?key=6ad64af0"
response = requests.get(api_url)
product_data = response.json()

push_data_to_queue(product_data)
get_data_from_queue()