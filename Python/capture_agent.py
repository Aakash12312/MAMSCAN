import psutil
import datetime
import json
import time
import socket
from kafka import KafkaProducer

def protocol_name(proto_type):
    return {
        socket.SOCK_STREAM: "TCP",
        socket.SOCK_DGRAM: "UDP"
    }.get(proto_type, "OTHER")

def get_netstat_connections():
    data = []
    for conn in psutil.net_connections(kind='inet'):
        try:
            laddr = f"{conn.laddr.ip}:{conn.laddr.port}" if conn.laddr else ""
            raddr = f"{conn.raddr.ip}:{conn.raddr.port}" if conn.raddr else ""
            pid = conn.pid
            proc_name = psutil.Process(pid).name() if pid else "Unknown"
            data.append({
                "protocol": protocol_name(conn.type),
                "local_address": laddr,
                "foreign_address": raddr,
                "state": conn.status,
                "pid": pid,
                "process": proc_name
            })
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return data

def collect_metrics():
    return {
        'host': socket.gethostname(),
        'timestamp': datetime.datetime.now().isoformat(),  
        'cpu_percent': psutil.cpu_percent(),
        'memory_percent': psutil.virtual_memory().percent,
        'net_connections': get_netstat_connections()
    }

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
