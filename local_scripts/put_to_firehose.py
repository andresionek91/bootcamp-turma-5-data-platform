import boto3
import json
from fake_web_events import Simulation

client = boto3.client("firehose")


def put_record(event):
    data = json.dumps(event) + "\n"
    response = client.put_record(
        DeliveryStreamName="firehose-production-raw-delivery-stream",
        Record={"Data": data},
    )
    print(event)
    return response


simulation = Simulation(user_pool_size=100, sessions_per_day=1000)
events = simulation.run(duration_seconds=10000)

for event in events:
    put_record(event)
