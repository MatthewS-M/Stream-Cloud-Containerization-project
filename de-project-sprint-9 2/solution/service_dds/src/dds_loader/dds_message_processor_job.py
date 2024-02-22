from datetime import datetime
from logging import Logger


class DdsMessageProcessor:

    def run(self) -> None:
        start_message = f"{datetime.utcnow()}: START"
        self._logger.info(start_message)

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if msg is None:
                continue

            user_id = msg["payload"]["user"]["user_id"]
            self._dds_repository.h_user_insert(user_id)

            user_name = msg["payload"]["user"]["user_name"]
            user_login = msg["payload"]["user"]["user_login"]
            self._dds_repository.s_user_names_insert(user_id, user_name, user_login)

            order_id = msg["payload"]["id"]
            order_date = msg["payload"]["date"]
            self._dds_repository.h_order_insert(order_id, order_date)

            order_cost = msg["payload"]["cost"]
            order_payment = msg["payload"]["payment"]
            self._dds_repository.s_order_cost_insert(order_id, order_cost, order_payment)

            order_status = msg["payload"]["status"]
            self._dds_repository.s_order_status_insert(order_id, order_status)

            self._dds_repository.l_order_user_insert(user_id, order_id)

            products = msg["payload"]["products"]
            self._dds_repository.h_product_insert(products)

            self._dds_repository.s_product_names_insert(products)

            restaurant_info = msg["payload"]["restaurant"]
            restaurant_id = restaurant_info["restaurant_id"]

            self._dds_repository.h_restaurant_insert(restaurant_id)
    
            restaurant_name = restaurant_info["restaurant_name"]
            self._dds_repository.s_restaurant_names_insert(restaurant_id, restaurant_name)

            categories = set(p["category"] for p in products.values())
            category_mappings = self._dds_repository.h_category_insert(categories)
            for product_id, product in products.items():
                if product["category"] in category_mappings:
                    self._dds_repository.l_product_restaurant_insert(product_id, restaurant_id)
                    self._dds_repository.l_order_product_insert(order_id, product_id)
                    self._dds_repository.l_product_category_insert(product_id, category_mappings[product["category"]])

            dest_msg = self.construct_output_message(msg, category_mappings)
            self._producer.produce(dest_msg)

        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def construct_output_message(self, original_message: dict, category_mappings: dict) -> dict:
        products = {p_id: {**p, "h_category_pk": category_mappings.get(p["category"], '')} for p_id, p in original_message["payload"]["products"].items()}
        return {
            "user_id": original_message["payload"]["user"]["user_id"],
            "products": products
        }
