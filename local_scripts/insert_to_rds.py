import psycopg2
from datetime import datetime
from random import choice
import time
import uuid

dsn = (
    "dbname={dbname} "
    "user={user} "
    "password={password} "
    "port={port} "
    "host={host} ".format(
        dbname="orders",
        user="postgres",
        password="^_LFVyTgTQZXFlOSbp2R9z4LuG^ib1",
        port=5432,
        host="rds-production-orders-db.cmaba2mp3qm1.us-east-1.rds.amazonaws.com",
    )
)

conn = psycopg2.connect(dsn)
print("connected")
conn.set_session(autocommit=True)
cur = conn.cursor()
cur.execute(
    "create table if not exists orders_v2("
    "created_at timestamp,"
    "order_id uuid PRIMARY KEY,"
    "product_name varchar(100),"
    "value float);"
)

products = {
    "casa": 500000.00,
    "carro": 69900.00,
    "moto": 7900.00,
    "caminhao": 230000.00,
    "laranja": 0.5,
    "borracha": 0.3,
    "iphone": 1000000.00,
}


while True:
    order_id = uuid.uuid4()
    created_at = datetime.now().isoformat()
    product_name, value = choice(list(products.items()))
    cur.execute(
        f"insert into orders_v2 values ('{created_at}', '{order_id}', '{product_name}', {value})"
    )
    print(
        f"insert into orders values ('{created_at}', '{order_id}', '{product_name}', {value})"
    )
    time.sleep(0.2)
