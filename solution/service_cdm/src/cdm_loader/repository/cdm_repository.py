import uuid
from typing import Dict, List
from lib.pg import PgConnect
from psycopg import Cursor

class CdmRepository:
    def __init__(self, db: PgConnect, logger):
        self._db = db
        self._logger = logger  

    def save_message(self, message: List[Dict]):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                try:
                    self.__create_temp_table(cur)
                    self.__load_temp_table(cur, message)
                    self._logger.info(f"\n======\n======\nTEMP TABLE LOADED\n======\n======\n")
                    self.__load_data_marts(cur)
                    self._logger.info(f"\n======\n======\nDM LOADED\n======\n======\n")
                except Exception as e:
                    self._logger.error(f"An error occurred: {e}")
                finally:
                    cur.execute(f"DROP TABLE IF EXISTS temp_table_prod;")
                    cur.execute(f"DROP TABLE IF EXISTS temp_table_cat;")

    def __create_temp_table(self, cur: Cursor):
        cur.execute(
            f"""
                CREATE TEMP TABLE temp_table_prod
                (
                    user_id         VARCHAR,
                    product_id      VARCHAR,
                    product_name    VARCHAR,
                    order_cnt       INT
                )
                    ON COMMIT DROP;
            """
        )
        cur.execute(
            f"""
                CREATE TEMP TABLE temp_table_cat
                (
                    user_id         VARCHAR,
                    category_id     VARCHAR,
                    category_name   VARCHAR,
                    order_cnt       INT
                )
                    ON COMMIT DROP;
            """
        )

    def __load_temp_table(self, cur: Cursor, stats: List[Dict]):
        for each in stats:
            if each['object_type'] == 'user_prod':
                cur.execute(
                    """
                        INSERT INTO temp_table_prod (user_id, product_id, product_name, order_cnt)
                        VALUES 
                        (
                            %(user_id)s,
                            %(product_id)s,
                            %(product_name)s,
                            %(order_cnt)s
                        );
                    """,
                    {
                        'user_id': each['payload']['user_id'],
                        'product_id': each['payload']['product_id'] if each['object_type'] == 'user_prod' else '',
                        'product_name': each['payload']['product_name'] if each['object_type'] == 'user_prod' else '',
                        'order_cnt': int(each['payload']['order_cnt'])
                    }
                )
            elif each['object_type'] == 'user_categ':
                cur.execute(
                    """
                        INSERT INTO temp_table_cat (user_id, category_id, category_name, order_cnt)
                        VALUES 
                        (
                            %(user_id)s,
                            %(category_id)s,
                            %(category_name)s,
                            %(order_cnt)s
                        );
                    """,
                    {
                        'user_id': each['payload']['user_id'],
                        'category_id': each['payload']['category_id'] if each['object_type'] == 'user_categ' else '',
                        'category_name': each['payload']['category_name'] if each['object_type'] == 'user_categ' else '',
                        'order_cnt': int(each['payload']['order_cnt'])
                    }
                )


    def __load_data_marts(self, cur: Cursor):
        cur.execute(
            f"""
                INSERT INTO cdm.user_product_counters (user_id, product_id, product_name, order_cnt)
                SELECT  tt.user_id          AS user_id, 
                        tt.product_id       AS product_id, 
                        tt.product_name     AS product_name, 
                        SUM(tt.order_cnt)   AS order_cnt
                FROM temp_table_prod AS tt
                GROUP BY user_id, product_id, product_name
                ON CONFLICT (user_id, product_id) DO UPDATE 
                SET product_name = EXCLUDED.product_name,
                    order_cnt = EXCLUDED.order_cnt;
            """
        )

        cur.execute(
            f"""
                INSERT INTO cdm.user_category_counters (user_id, category_id, category_name, order_cnt) 
                SELECT  tt.user_id          AS user_id, 
                        tt.category_id       AS category_id, 
                        tt.category_name     AS category_name, 
                        SUM(tt.order_cnt)   AS order_cnt
                FROM temp_table_cat AS tt
                GROUP BY user_id, category_id, category_name
                ON CONFLICT (user_id, category_id) DO UPDATE 
                SET category_name = EXCLUDED.category_name,
                    order_cnt = EXCLUDED.order_cnt;
            """
        )
