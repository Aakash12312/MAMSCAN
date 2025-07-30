import psutil
import datetime
import subprocess
import json
import time
import socket
import os
from kafka import KafkaProducer

# 🧠 Get netstat info with process names (Windows only)
def get_netstat_connections():
    try:
        result = subprocess.check_output(['netstat', '-ano'], stderr=subprocess.STDOUT).decode(errors='ignore')
        lines = result.splitlines()
        pid_map = {}
        for p in psutil.process_iter(['pid', 'name']):
            pid_map[str(p.info['pid'])] = p.info['name']

        connections = []
        for line in lines:
            if line.strip().startswith("TCP") or line.strip().startswith("UDP"):
                parts = line.split()
                if len(parts) >= 5:
                    protocol, local, foreign, state, pid = parts[0], parts[1], parts[2], parts[3], parts[4]
                elif len(parts) == 4:  # For UDP
                    protocol, local, foreign, pid = parts[0], parts[1], parts[2], parts[3]
                    state = ""
                else:
                    continue

                connections.append({
                    "protocol": protocol,
                    "local_address": local,
                    "foreign_address": foreign,
                    "state": state,
                    "pid": pid,
                    "process": pid_map.get(pid, "Unknown")
                })
        return connections
    except Exception as e:
        return [{"error": str(e)}]

# 🧠 Collect system metrics
def collect_metrics():
    return {
        'host': socket.gethostname(),
        'timestamp': datetime.datetime.now().isoformat(),
        'cpu_percent': psutil.cpu_percent(),
        'memory_percent': psutil.virtual_memory().percent,
        'net_connections': get_netstat_connections()
    }

# 🚀 Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("📡 Starting capture agent. Press Ctrl+C to stop.\n")

try:
    while True:
        metrics = collect_metrics()
        print("📤 Sending to Kafka:")
        print(json.dumps(metrics, indent=2))
        producer.send('network_metrics', value=metrics)
        producer.flush()
        time.sleep(10)
except KeyboardInterrupt:
    print("\n🛑 Stopped by user.")
    producer.close()
