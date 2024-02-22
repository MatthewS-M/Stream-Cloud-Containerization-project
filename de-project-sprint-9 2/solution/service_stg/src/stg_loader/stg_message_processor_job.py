import time
import json
from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis import RedisClient
from stg_loader.repository.stg_repository import StgRepository

class StgMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis_client: RedisClient,
                 stg_repository: StgRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis_client
        self._stg_repository = stg_repository
        self._batch_size = batch_size
        self._logger = logger

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        for i in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._process_message(msg)

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def _process_message(self, msg: dict) -> None:
        payload = msg['payload']
        products = payload.get('order_items', [])

        # diggin redis
        user_name = self._redis.get(payload['user']['id']).get('name', '')
        rest_msg = self._redis.get(payload['restaurant']['id'])
        

        menu = rest_msg.get('menu', [])

        # start to collect out_msg
        out_msg = {
            "object_id": msg['object_id'],
            "object_type": msg['object_type'],
            "payload": {
                "id": msg['object_id'],
                "date": payload.get('date', ''),
                "cost": payload.get('cost', ''),
                "payment": payload.get('payment', ''),
                "status": payload.get('final_status', ''),
                "restaurant": {
                    "id": payload['restaurant'].get('id', ''),
                    "name": rest_msg.get('name', '')
                },
                "user": {
                    "id": payload['user'].get('id', ''),
                    "name": user_name
                },
                "products": self._process_products(products, menu)
            }
        }

        self._producer.produce(out_msg)

    def _process_products(self, products: list, menu: list) -> list:
        result = []
        for prdct in products:
            product_info = {
                "id": prdct.get('id', ''),
                "price": prdct.get('price', ''),
                "quantity": prdct.get('quantity', ''),
                "name": prdct.get('name', '')
            }

            for items in menu:
                if items.get('_id', '') == prdct.get('id', ''):
                    product_info["category"] = items.get('category', '')
                    break

            result.append(product_info)

        return result

# Пример использования
# consumer = KafkaConsumer(...)
# producer = KafkaProducer(...)
# redis_client = RedisClient(...)
# stg_repository = StgRepository(...)
# logger = Logger(...)

# processor = StgMessageProcessor(consumer, producer, redis_client, stg_repository, 100, logger)
# processor.run()
