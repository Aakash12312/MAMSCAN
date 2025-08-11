# snmp_producer.py
import asyncio
import csv
import json
from datetime import datetime
from kafka import KafkaProducer
from pysnmp.hlapi.asyncio import (
    SnmpEngine,
    CommunityData,
    UdpTransportTarget,
    ContextData,
    ObjectType,
    ObjectIdentity,
    get_cmd
)

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "snmp_metrics"
POLL_INTERVAL = 10  # seconds

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

async def poll_snmp(ip, oid, community='public', hostname=None):
    """Poll a single SNMP OID from a given device."""
    transport = await UdpTransportTarget.create((ip, 161))
    errorIndication, errorStatus, errorIndex, varBinds = await get_cmd(
        SnmpEngine(),
        CommunityData(community, mpModel=1),  # SNMP v2c
        transport,
        ContextData(),
        ObjectType(ObjectIdentity(oid))
    )

    result = {}
    if errorIndication:
        result['error'] = str(errorIndication)
    elif errorStatus:
        result['error'] = f"{errorStatus.prettyPrint()} at {errorIndex and varBinds[int(errorIndex)-1][0]}"
    else:
        for varBind in varBinds:
            result[str(varBind[0])] = str(varBind[1])

    message = {
        "host": hostname or ip,
        "ip": ip,
        "collector_hostname": "LocalCollector",
        "timestamp": datetime.now().isoformat(),
        "results": result
    }

    producer.send(KAFKA_TOPIC, message)
    print(f"✅ Sent SNMP data for {ip} OID {oid} to Kafka")

async def poll_all_devices():
    """Poll all devices from inventory.csv once."""
    with open("inventory.csv") as f:
        reader = csv.DictReader(f)
        tasks = []
        for row in reader:
            oids = row["oids"].split(";")
            for oid in oids:
                tasks.append(
                    poll_snmp(
                        row["ip"],
                        oid,
                        row.get("community", "public"),
                        row.get("hostname")
                    )
                )
        await asyncio.gather(*tasks)

    # Flush Kafka producer (blocking, not async)
    producer.flush()


async def main():
    """Run SNMP polling in a loop every POLL_INTERVAL seconds."""
    while True:
        print(f"\n📡 Starting SNMP polling at {datetime.now().isoformat()}")
        await poll_all_devices()
        print(f"⏳ Waiting {POLL_INTERVAL} seconds before next poll...\n")
        await asyncio.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
