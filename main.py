import pandas as pd
import json
from datetime import datetime
import time
import paho.mqtt.client as mqtt

# MQTT Configuration
MQTT_BROKER = "4.213.199.181"
MQTT_PORT = 1883
MQTT_TOPIC = "ilens/monitor/live/"

# Excel file path
excel_file = r"C:\Users\harisankar.s\PycharmProjects\PythonProject\ppm simulation\sim_data.xlsx"

# Load Excel data
df = pd.read_excel(excel_file)
df['Published'] = False  # Track which rows are sent

# Extract configuration from first row
first_row = df.iloc[0]
equipment_id = first_row['Equipment ID']
project_id = first_row['Project_id']
site_id = equipment_id.split("$")[0]

# MQTT setup
client = mqtt.Client(protocol=mqtt.MQTTv311)
client.connect(MQTT_BROKER, MQTT_PORT, 60)

def form_message(data, timestamp, project_id, site_id):
    return {
        "data": data,
        "site_id": site_id,
        "gw_id": "",
        "pd_id": "",
        "p_id": project_id,
        "timestamp": int(timestamp * 1000),
        "msg_id": 1,
        "retain_flag": False
    }

post_push_margin_sec = 5

print(" Starting data publishing...")
try:
    while True:
        now = datetime.now()
        for idx, row in df.iterrows():
            if idx == 0 or row['Published']:  # Skip header and published rows
                continue

            try:
                scheduled_time = datetime.combine(now.date(), datetime.strptime(str(row['Time']), "%H:%M:%S").time())
                time_diff_sec = (now - scheduled_time).total_seconds()

                if 0 <= time_diff_sec <= post_push_margin_sec:
                    data = {
                        # Directly use tag IDs from first row
                        f"{equipment_id}${first_row['production_value_tag_id']}": int(row['Total Produced']),
                        f"{equipment_id}${first_row['reject_tag_id']}": int(row['Reject Units']),
                        f"{equipment_id}${first_row['equipment_status_tag_id']}": int(row['Asset Status']),
                        f"{equipment_id}${first_row['planned_quantity_tag_id']}": int(row['Planned Quantity']),
                        f"{equipment_id}${first_row['ict_tag']}": float(row['Ideal Cycle Time']),
                        f"{equipment_id}${first_row['process_order_tag']}": str(row['Process order']),
                        f"{equipment_id}${first_row['product_tag']}": str(row['Product Id'])
                    }

                    client.publish(MQTT_TOPIC, json.dumps(form_message(data, scheduled_time.timestamp(), project_id, site_id)))
                    asset_value = data[f"{equipment_id}${first_row['equipment_status_tag_id']}"]
                    print(f" Published {row['Time']} | Assets: {asset_value}")
                    
                    df.at[idx, 'Published'] = True

            except Exception as e:
                print(f"⚠ Error in row {idx}: {str(e)}")

        time.sleep(1)

except KeyboardInterrupt:
    print(" Stopped by user")
finally:
    client.disconnect()
    print(" Disconnected from MQTT")









