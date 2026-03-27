import boto3, json, random, uuid
from datetime import datetime

kinesis  = boto3.client('kinesis', region_name='eu-central-1')
STREAM   = 'ridewave-rides-stream-surajsingh'
CITIES   = ["Mumbai","Delhi","Bengaluru","Chennai","Pune","Hyderabad"]
STATUSES = ["completed","completed","confirmed","cancelled"]

sent = 0
for i in range(50):
    try:
        ride = {
            "ride_id"    : str(uuid.uuid4())[:8].upper(),
            "driver_id"  : f"DRV{random.randint(1,100):03d}",
            "customer_id": f"CUST{random.randint(1,200):03d}",
            "city"       : random.choice(CITIES),
            "fare_amount": round(random.uniform(50,800),2),
            "distance_km": round(random.uniform(1.5,35.0),2),
            "ride_status": random.choice(STATUSES),
            "event_time" : datetime.utcnow().isoformat(),
            "ingest_date": datetime.utcnow().strftime("%Y-%m-%d")
        }
        kinesis.put_record(
            StreamName=STREAM, Data=json.dumps(ride),
            PartitionKey=ride["ride_id"]
        )
        sent += 1
        print(f"Sent: {ride['ride_id']} | {ride['city']}")
    except Exception as e:
        print(f"Failed: {e}")
print(f"\nTotal sent: {sent}")
