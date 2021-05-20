import psycopg2

dsn = (
    "dbname={dbname} "
    "user={user} "
    "password={password} "
    "port={port} "
    "host={host} ".format(
        dbname="orders",
        user="postgres",
        password=",MHCILqBEZ_L5Ev4r1eLMr=W2Ff-5A",
        port=5432,
        host="rds-production-orders-db.cjuefeiklqc5.us-east-1.rds.amazonaws.com",
    )
)

conn = psycopg2.connect(dsn)
print("connected")
conn.set_session(autocommit=True)
cur = conn.cursor()

cur.execute(
    f"update orders_v2 set value = 500000 where order_id='86a9a8a0-682e-435d-955e-dd61d888412b'"
)
cur.execute(
    f"update orders_v2 set value = 200 where order_id='86a9a8a0-682e-435d-955e-dd61d888412b'"
)
cur.execute(
    f"delete from orders_v2 where order_id='87744bf8-05b4-4e51-807e-5a07960fb450'"
)
