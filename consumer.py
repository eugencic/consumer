from flask import Flask, request
from threading import Thread
import requests
from time import sleep
import queue
from products import products

app = Flask(__name__)

threads = []

orders_queue = queue.Queue()
orders_queue.join()

@app.route('/consumer', methods = ['GET', 'POST'])

def producer_aggregator():
    data = request.get_json()
    print(f'Order nr.{data["order_id"]} is received. Checking for the product\n')
    sleep(3)
    split_order(data)
    return {'isSuccess': True}

def split_order(input_order):
    order = {
        'order_id': input_order['order_id'],
        'client_id': input_order['client_id'],
        'product_id': input_order['product_id'],
    }
    orders_queue.put(order)

def send_order(name):
    try:
        order = orders_queue.get()
        orders_queue.task_done()
        order_item = None
        for i, item in enumerate(products):
            if item['id'] == order['product_id']:
                order_item = item
        payload = dict({'order_id': order['order_id'], 'client_id': order['client_id'], 'product_id': order['product_id']})
        print(f'{order_item["name"]} from order nr.{order["order_id"]} is sent to the delivery service\n')
        requests.post('http://localhost:4040/consumer_aggregator', json = payload, timeout = 0.0000000001)
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError):
        pass
    
def run_consumer():
    consumer_thread = Thread(target = lambda: app.run(host = '0.0.0.0', port = 4080, debug = False, use_reloader = False), daemon = True)
    threads.append(consumer_thread)
    sleep(2)
    consumer_thread_name = 1
    for _ in range(7):
        thread = Thread(target = send_order, args = (consumer_thread_name,), name = str(consumer_thread_name))
        threads.append(thread)
        consumer_thread_name += 1
    for thread in threads:
        thread.start()
        sleep(2)
    for thread in threads:
        thread.join()
    while True:
        pass
    
if __name__ == '__main__':
    run_consumer()