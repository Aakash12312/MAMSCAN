# consumer_writer.py
from kafka import KafkaConsumer
import mysql.connector
import json

# 🔌 Connect to Kafka
consumer = KafkaConsumer(
    'network_metrics',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# 🔌 Connect to MySQL
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Aakash10",  # change if needed
    database="monitoring"
)
cursor = db.cursor()

# ✅ Create table (if not exists)
cursor.execute('''
CREATE TABLE IF NOT EXISTS metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    host VARCHAR(255),
    cpu_percent FLOAT,
    memory_percent FLOAT,
    net_connections TEXT,
    timestamp VARCHAR(50)
)
''')

print("📥 Waiting for messages from Kafka...")

# 🔁 Listen for Kafka messages
for msg in consumer:
    data = msg.value
    try:
        cursor.execute('''
            INSERT INTO metrics (host, cpu_percent, memory_percent, net_connections, timestamp)
            VALUES (%s, %s, %s, %s, %s)
        ''', (
            data['host'],
            data['cpu_percent'],
            data['memory_percent'],
            json.dumps(data['net_connections']),  # serialize list to string
            data['timestamp']
        ))
        db.commit()
        print("✅ Data written to DB:")
        print(json.dumps(data, indent=2))
    except Exception as e:
        print("❌ Error writing to DB:", e)
