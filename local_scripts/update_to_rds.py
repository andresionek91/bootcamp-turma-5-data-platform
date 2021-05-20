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

cur.execute(f"update orders_agora_vai set value = 0.5 where order_id=988")
