import pandas as pd
import json
from datetime import datetime
import time
import paho.mqtt.client as mqtt

# MQTT Configuration
MQTT_BROKER = "4.213.199.181"
MQTT_PORT = 1883
MQTT_TOPIC = "ilens/monitor/live/"

# Equipment and tag setup
equipment_id = "l1_100$l2_101$l3_102$l4_103$l5_104$l6_105$l7_106$ast_107"
TAG_IDS = {
    "TotalProducedUnits": 101,
    "DefectiveUnits": 100,
    "AssetStatus": 113
}
project_id = "project_827"
site_id = equipment_id.split("$")[0]

# Excel file path
excel_file = r"C:\Users\harisankar.s\PycharmProjects\PythonProject\ppm simulation\sim_data.xlsx"

# Load Excel data
df = pd.read_excel(excel_file)
df['Published'] = False  # Track which rows are sent

# MQTT setup
client = mqtt.Client(protocol=mqtt.MQTTv311)
client.connect(MQTT_BROKER, MQTT_PORT, 60)

# Message formatting
def form_message(data, timestamp, project_id, site_id):
    return {
        "data": data,
        "site_id": site_id,
        "gw_id": "",
        "pd_id": "",
        "p_id": project_id,
        "timestamp": int(timestamp * 1000),  # milliseconds
        "msg_id": 1,
        "retain_flag": False
    }

# Margin of delay allowed (push only if scheduled_time has passed, within this margin)
post_push_margin_sec = 5

print("🔁 Starting strict-timing data publishing loop...")
try:
    while True:
        now = datetime.now()
        for idx, row in df.iterrows():
            if row['Published']:
                continue

            # Parse Excel time (assumed format HH:MM or HH:MM:SS)
            time_str = str(row['Time'])
            if len(time_str) == 5:
                time_str += ":00"  # Add seconds if missing
            try:
                scheduled_time = datetime.combine(now.date(), datetime.strptime(time_str, "%H:%M:%S").time())
            except Exception as e:
                print(f" Error parsing time in row {idx}: {e}")
                continue

            time_diff_sec = (now - scheduled_time).total_seconds()

            # Strict condition: push only if time has passed (within margin), and not before
            if 0 <= time_diff_sec <= post_push_margin_sec:
                data = {
                    f"{equipment_id}$tag_{TAG_IDS['TotalProducedUnits']}": int(row['Total Produced']),
                    f"{equipment_id}$tag_{TAG_IDS['DefectiveUnits']}": int(row['Reject Units']),
                    f"{equipment_id}$tag_{TAG_IDS['AssetStatus']}": int(row['Asset Status'])
                }

                message = form_message(data, scheduled_time.timestamp(), project_id, site_id)
                client.publish(MQTT_TOPIC, json.dumps(message))
                print(f" Published for scheduled time {row['Time']} at {now.strftime('%H:%M:%S')}")

                df.at[idx, 'Published'] = True  # Prevent re-sending

        time.sleep(1)

except KeyboardInterrupt:
    print(" Interrupted by user. Exiting...")

client.disconnect()
print(" MQTT client disconnected. All scheduled messages published.")









