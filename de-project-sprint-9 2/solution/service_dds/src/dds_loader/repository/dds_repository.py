import uuid
import os
from datetime import datetime
from typing import Any, Dict, List
from lib.pg import PgConnect
from decimal import Decimal
from pydantic import BaseModel
from psycopg2.extras import execute_batch


class H_User(BaseModel):
    h_user_pk: uuid.UUID
    user_id: str
    load_dt: datetime
    load_src: str

class H_Product(BaseModel):
    h_product_pk: uuid.UUID
    product_id: str
    load_dt: datetime
    load_src: str 

class H_Category(BaseModel):
    h_category_pk : uuid.UUID
    category_name: str
    load_dt: datetime
    load_src: str

class H_Restaurant(BaseModel):
    h_restaurant_pk : uuid.UUID
    restaurant_id: str
    load_dt : datetime
    load_src: str

class H_Order(BaseModel):
    h_order_pk : uuid.UUID
    order_id : str
    order_dt : str   
    load_dt : datetime
    load_src : str

class S_Order_Status(BaseModel):
    h_order_pk : uuid.UUID
    status:str
    load_dt:datetime
    load_src:str
    hk_order_status_hashdiff:str   

class S_Order_Cost(BaseModel):
    h_order_pk : uuid.UUID
    cost:float
    payment:float
    load_dt:datetime
    load_src:str
    hk_order_status_hashdiff:str 

class S_Product_Names(BaseModel):
    h_product_pk : uuid.UUID
    name:str
    load_dt:datetime
    load_src:str
    hk_product_names_hashdiff:str

class S_Restaurant_Names(BaseModel):
    h_restaurant_pk : uuid.UUID
    name:str
    load_dt:datetime
    load_src:str
    hk_restaurant_names_hashdiff:str

class S_User_Names(BaseModel):
    h_user_pk: uuid.UUID
    username: str
    userlogin: str
    load_dt: datetime
    load_src: str
    hk_user_names_hashdiff  : str

class L_Product_Restaurant(BaseModel):
    hk_product_restaurant_pk : uuid.UUID
    h_restaurant_pk : uuid.UUID
    h_product_pk : uuid.UUID
    load_dt: datetime
    load_src: str

class L_Order_Product(BaseModel):
    hk_order_product_pk : uuid.UUID
    h_order_pk : uuid.UUID
    h_product_pk : uuid.UUID
    load_dt: datetime
    load_src: str

class L_Order_User(BaseModel):
    hk_order_user_pk : uuid.UUID
    h_order_pk : uuid.UUID
    h_user_pk : uuid.UUID
    load_dt: datetime
    load_src: str

class L_Product_Category(BaseModel):
    hk_product_category_pk : uuid.UUID
    h_product_pk : uuid.UUID
    h_category_pk : uuid.UUID
    load_dt: datetime
    load_src: str



class OrderDdsBuilder:
    def init(self, dict: Dict) -> None:
        self._dict = dict
        self.source_system = ""
        self.order_ns_uuid = uuid.UUID('')

    def _uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(namespace=self.order_ns_uuid, name=str(obj))
    
    def h_user(self) -> H_User:
        user_id = self._data['user']
        return H_User(
            h_user_pk=self._uuid(user_id),
            user_id=user_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
    
    def h_product(self) -> List[H_Product]:
        products = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            products.append(
                H_Product(
                    h_product_pk=self._uuid(prod_id),
                    product_id=prod_id,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system 
                )
            )
        return products
     
    def h_restaurant(self) -> H_Restaurant:
        restaurant_id = self._data['restaurant']['id']
        return H_Restaurant(
            h_restaurant_pk=self._uuid(restaurant_id),
            id=restaurant_id,
            name=self._data['restaurant']['name'],
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
    
    def h_order(self) -> H_Order:
        order_id = self._dict['id']
        return H_Order(
            h_order_pk=self._uuid(order_id),
            order_id=order_id,
            order_dt=self._dict['date'],
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def h_category(self) -> List[H_Category]:
        categories = []
        for prod_dict in self._dict['products']:
            category_name = prod_dict["category"]
            categories.append(
                H_Category(
                    h_category_pk=self._uuid(category_name),
                    category_name=category_name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system                    
                )
            )
    def s_order_status(self) -> S_Order_Status:
        return S_Order_Status(
            h_order_pk=self._uuid(self._dict["id"]),
            status=self._dict["status"],
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_order_status_hashdiff=self._uuid([self._dict["id"],self._dict["status"]])
        )
    
    def s_order_cost(self) -> S_Order_Cost:
        return S_Order_Cost(
            h_order_pk=self._uuid(self._dict["id"]),
            cost=self._dict["cost"],
            payment=self._dict["payment"],
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_order_status_hashdiff=self._uuid([self._dict["id"],self._dict["cost"], self._dict["payment"]])
        )
    
    def s_product_names(self) -> List[S_Product_Names]:
        pnames = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            pnames.append(
                S_Product_Names(
                    h_product_pk=self._uuid(prod_id),
                    name=prod_dict['name'],
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system,
                    hk_product_names_hashdiff=self._uuid([ prod_id, prod_dict['name'] ])
                )
            )
        return pnames

    def s_restaurant_names(self) -> S_Restaurant_Names:
        rest = self._dict["restaurant"]
        return S_Restaurant_Names(
            h_restaurant_pk=self._uuid(rest["id"]),
            name=rest["name"],
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_restaurant_names_hashdiff=self._uuid([ rest["id"], rest["name"] ])
        )

    def s_user_names(self) -> S_User_Names:
        usr = self._dict["user"]
        return S_User_Names(
            h_user_pk=self._uuid(usr["id"]),
            username=usr["name"],
            userlogin=usr["login"],
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_user_names_hashdiff=self._uuid([ usr["id"], usr["name"], usr["login"]])
        )
    
    def l_product_restaurant(self) -> List[L_Product_Restaurant]:
        h_restaurant_pk = self._uuid(self._dict["restaurant"]["id"])
        l_pr = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            h_product_pk = self._uuid(prod_id)
            l_pr.append(
                L_Product_Restaurant(
                    hk_product_restaurant_pk=self._uuid([ h_product_pk, h_restaurant_pk ]),
                    h_product_pk=h_product_pk,
                    h_restaurant_pk = h_restaurant_pk,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return l_pr

    def l_order_product(self) -> List[L_Order_Product]:
        h_order_pk = self._uuid(self._dict["id"])
        l_op = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            h_product_pk = self._uuid(prod_id)
            l_op.append(
                L_Order_Product(
                    hk_order_product_pk=self._uuid([ h_product_pk, h_order_pk ]),
                    h_product_pk=h_product_pk,
                    h_order_pk = h_order_pk,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return l_op

    def l_order_user(self) -> L_Order_User:
        h_order_pk = self._uuid(self._dict["id"])
        h_user_pk = self._uuid(self._dict["user"]["id"])
        return L_Order_User(
            hk_order_user_pk = self._uuid([h_order_pk, h_user_pk]),
            h_order_pk = h_order_pk,
            h_user_pk = h_user_pk,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def l_product_category(self) -> List[L_Product_Category]:
        l_pc = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            h_product_pk = self._uuid(prod_id)
            h_category_pk = self._uuid(prod_dict['category'])
            l_pc.append(
                L_Order_Product(
                    hk_product_category_pk=self._uuid([ h_product_pk, h_category_pk ]),
                    h_product_pk=h_product_pk,
                    h_category_pk = h_category_pk,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return l_pc
    
    def cdm_prd_msg(self) -> List[Dict]:
        msg = []
        for prod_dict in self._dict['products']:
            prd_msg = {}
            prd_msg["object_id"] = self._dict["id"]
            prd_msg["object_type"] = 'user_prod'
            pl = {}
            pl["user_id"] = self.h_user().user_id
            pl["product_id"] = prod_dict['id']
            pl["product_name"] = prod_dict["name"]
            pl["order_cnt"] = 1
            prd_msg["payload"] = pl
            msg.append(prd_msg)
        return msg
    
    def cdm_categ_msg(self) -> List[Dict]:
        msg = []
        for prod_dict in self._dict['products']:
            prd_msg = {}
            prd_msg["object_id"] = self._dict["id"]
            prd_msg["object_type"] = 'user_categ'
            pl = {}
            pl["user_id"] = self.h_user().user_id
            pl["category_id"] = self._uuid(prod_dict["category"])
            pl["category_name"] = prod_dict["category"]
            pl["order_cnt"] = 1
            prd_msg["payload"] = pl
            msg.append(prd_msg)
        return msg

class DdsRepository:
    def init(self, db: PgConnect) -> None:
        self._db = db


    def h_user_insert(self, user: H_User) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_user(
                            h_user_pk,
                            user_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_user_pk)s,
                            %(user_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_user_pk) DO NOTHING;
                    """,
                    {
                        'h_user_pk': user.h_user_pk,
                        'user_id': user.user_id,
                        'load_dt': user.load_dt,
                        'load_src': user.load_src
                    }
                )

    def s_user_names_insert(self, user_id: str, username: str, userlogin: str, user: H_User) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT h_user_pk FROM dds.h_user WHERE user_id = '{user_id}'")
                h_user_pk = cur.fetchone()[0]
                cur.execute("""
                    INSERT INTO dds.s_user_names (
                        hk_user_names_pk,
                        h_user_pk,
                        username,
                        userlogin,
                        load_dt,
                        load_src
                    )
                    VALUES (
                        %(hk_user_names_pk)s,
                        %(h_user_pk)s,
                        %(username)s,
                        %(userlogin)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (h_user_pk) DO NOTHING;
                """, {
                    "hk_user_names_pk": str(uuid.uuid4()),
                    "h_user_pk": h_user_pk,
                    "username": username,
                    "userlogin": userlogin,
                    "load_dt": user.load_dt,
                    "load_src": user.load_src
                })

        
    
    def h_order_insert(self, obj: H_Order) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_order(
                            h_order_pk,
                            order_id,
                            order_dt,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(order_id)s,
                            %(order_dt)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_order_pk) DO NOTHING;
                    """,
                    {
                        'h_order_pk': obj.h_order_pk,
                        'order_id': obj.order_id,
                        'order_dt': obj.order_dt,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                ) 

    def s_order_cost_insert(self, obj: S_Order_Cost) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_status(
                            h_order_pk,
                            cost,
                            payment,
                            hk_order_cost_hashdiff,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(cost)s,
                            %(payment)s,
                            %(hk_order_cost_hashdiff)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_order_pk) DO NOTHING;
                    """,
                    {
                        'h_order_pk': obj.h_order_pk,
                        'cost': obj.cost,
                        'payment': obj.payment,
                        'hk_order_cost_hashdiff': obj.hk_order_cost_hashdiff,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                ) 


    def s_order_status_insert(self, obj: S_Order_Status) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_status(
                            h_order_pk,
                            status,
                            hk_order_status_hashdiff,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(status)s,
                            %(hk_order_status_hashdiff)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_order_pk) DO NOTHING;
                    """,
                    {
                        'h_order_pk': obj.h_order_pk,
                        'status': obj.status,
                        'hk_order_status_hashdiff': obj.hk_order_status_hashdiff,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                ) 

    def l_order_user_insert(self, obj: L_Order_User) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_user(
                            hk_order_user_pk,
                            h_order_pk,
                            h_user_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_order_user_pk)s,
                            %(h_order_pk)s,
                            %(h_user_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_order_user_pk) DO NOTHING;
                    """,
                    {
                        'hk_order_user_pk': obj.hk_order_user_pk,
                        'h_order_pk': obj.h_order_pk,
                        'h_user_pk': obj.h_user_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )


    def h_product_insert(self, obj: H_Product) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_product(
                            h_product_pk,
                            product_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_product_pk)s,
                            %(product_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_product_pk) DO NOTHING;
                    """,
                    {
                        'h_product_pk': obj.h_product_pk,
                        'product_id': obj.product_id,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                ) 

    def s_product_names_insert(self, obj: S_Product_Names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_product_names(
                            h_product_pk,
                            name,
                            load_dt,
                            load_src,
                            hk_product_names_hashdiff
                        )
                        VALUES(
                            %(h_product_pk)s,
                            %(name)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_product_names_hashdiff)s
                        )
                        ON CONFLICT (h_product_pk) DO NOTHING;
                    """,
                    {
                        'h_product_pk': obj.h_product_pk,
                        'name': obj.name,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_product_names_hashdiff': obj.hk_product_names_hashdiff
                    }
                ) 


    def l_order_product_insert(self, obj: L_Order_Product) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_product(
                            hk_order_product_pk,
                            h_order_pk,
                            h_product_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_order_product_pk)s,
                            %(h_order_pk)s,
                            %(h_product_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_order_product_pk) DO NOTHING;
                    """,
                    {
                        'hk_order_product_pk': obj.hk_order_product_pk,
                        'h_order_pk': obj.h_order_pk,
                        'h_product_pk': obj.h_product_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def h_restaurant_insert(self, obj: H_Restaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_restaurant(
                            h_restaurant_pk,
                            restaurant_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_restaurant_pk)s,
                            %(restaurant_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_restaurant_pk) DO NOTHING;
                    """,
                    {
                        'h_restaurant_pk': obj.h_restaurant_pk,
                        'restaurant_id': obj.restaurant_id,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                ) 

    def s_restaurant_names_insert(self, obj: S_Restaurant_Names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_restaurant_names(
                            h_restaurant_pk,
                            name,
                            load_dt,
                            load_src,
                            hk_restaurant_names_hashdiff
                        )
                        VALUES(
                            %(h_restaurant_pk)s,
                            %(name)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_restaurant_names_hashdiff)s
                        )
                        ON CONFLICT (h_restaurant_pk) DO NOTHING;
                    """,
                    {
                        'h_restaurant_pk': obj.h_restaurant_pk,
                        'name': obj.name,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_restaurant_names_hashdiff': obj.hk_restaurant_names_hashdiff
                    }
                ) 



    def l_product_restaurant_insert(self, obj: L_Product_Restaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_restaurant(
                            hk_product_restaurant_pk,
                            h_restaurant_pk,
                            h_product_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_product_restaurant_pk)s,
                            %(h_restaurant_pk)s,
                            %(h_product_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_product_restaurant_pk) DO NOTHING;
                    """,
                    {
                        'hk_product_restaurant_pk': obj.hk_product_restaurant_pk,
                        'h_restaurant_pk': obj.h_restaurant_pk,
                        'h_product_pk': obj.h_product_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def h_category_insert(self, obj: H_Category) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_category(
                            h_category_pk,
                            category_name,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_category_pk)s,
                            %(category_name)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_category_pk) DO NOTHING;
                    """,
                    {
                        'h_category_pk': obj.h_category_pk,
                        'category_name': obj.category_name,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                ) 

    def l_product_category_insert(self, obj: L_Product_Category) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_category(
                            hk_product_category_pk,
                            h_category_pk,
                            h_product_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_product_category_pk)s,
                            %(h_category_pk)s,
                            %(h_product_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_product_category_pk) DO NOTHING;
                    """,
                    {
                        'hk_product_category_pk': obj.hk_product_category_pk,
                        'h_category_pk': obj.h_category_pk,
                        'h_product_pk': obj.h_product_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )