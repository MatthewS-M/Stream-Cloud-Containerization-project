from datetime import datetime
from stg_loader.repository.stg_repository import StgRepository
from lib.redis.redis_client import RedisClient
from lib.kafka_connect.kafka_connectors import KafkaConsumer
from lib.kafka_connect.kafka_connectors import KafkaProducer
from lib.pg import PgConnect
from logging import Logger
import json

class StgMessageProcessor:
    def __init__(self, consumer: KafkaConsumer, producer: KafkaProducer, redis_client: RedisClient, pg_connect: PgConnect, batch_size: int, logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis_client
        self._pg_connect = pg_connect
        self._batch_size = batch_size
        self._logger = logger
        
    def run(self) -> None:
        self._logger.debug(f"{datetime.utcnow()}: START")

        stg_repository = StgRepository(self._pg_connect)

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._process_message(msg, stg_repository)

        self._logger.debug(f"{datetime.utcnow()}: BATCH FINISHED PROCESSING")

    def _process_message(self, msg: dict, stg_repository: StgRepository) -> None:
        self._logger.info(f"\n======\n======--- MESSAGE with contents:\n{msg}\n======\n======\n")
        payload = msg['payload']
        products = payload.get('order_items', [])

        stg_repository.order_events_insert(str(msg['object_id']), str(msg['object_type']), str(datetime.now()), json.dumps(payload))
        self._logger.info(f"\n======\n======--- MESSAGE WRITTEN TO DB ---======\n======\n")

        user_name = self._redis.get(payload['user']['id']).get('name', '')
        user_login = self._redis.get(payload['user']['id']).get('login', '')
        rest_msg = self._redis.get(payload['restaurant']['id'])
        
        menu = rest_msg.get('menu', [])
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
                    "name": user_name,
                    "login": user_login
                },
                "products": self._process_products(products, menu)
            }
        }        

        self._producer.produce(out_msg)
        self._logger.info(f"\n======\n======--- DETAILED MESSAGE PRODUCED ---======\n======\n")


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
