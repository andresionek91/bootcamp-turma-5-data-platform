import psycopg2
from datetime import datetime
import os
from random import choice
import time

dsn = (
    "dbname={dbname} "
    "user={user} "
    "password={password} "
    "port={port} "
    "host={host} ".format(
        dbname="orders",
        user="postgres",
        password="TewUK^=fSBJr,-.vdgm7VIsMvnW8i=",
        port=5432,
        host="rds-develop-orders-db.cmrl9yuykkwg.us-east-1.rds.amazonaws.com",
    )
)

conn = psycopg2.connect(dsn)
print("connected")
conn.set_session(autocommit=True)
cur = conn.cursor()
cur.execute(
    "create table if not exists orders_agora_vai("
    "created_at timestamp,"
    "order_id integer,"
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
idx = 0

while True:
    print(idx)
    idx += 1
    created_at = datetime.now().isoformat()
    product_name, value = choice(list(products.items()))
    cur.execute(
        f"insert into orders_agora_vai values ('{created_at}', {idx}, '{product_name}', {value})"
    )
    time.sleep(0.2)


