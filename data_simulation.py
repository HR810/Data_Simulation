from config import MQTT_BROKER, MQTT_PORT, MQTT_TOPIC, EXCEL_FILE_PATH, SHEET_TAGS, DB_URL
import pandas as pd
import json
from datetime import datetime, timedelta
import time
import paho.mqtt.client as mqtt
from typing import Dict, Any
from contextlib import contextmanager
from sqlalchemy import create_engine, text

class MQTTClient:
    def __init__(self, broker: str, port: int):
        self.client = mqtt.Client(protocol=mqtt.MQTTv311)
        self.broker = broker
        self.port = port
        self._setup_client()

    def _setup_client(self):
        """Setup MQTT client with error handling and callbacks."""
        try:
            self.client.on_connect = self._on_connect
            self.client.on_disconnect = self._on_disconnect
            self.client.on_publish = self._on_publish
        except Exception as e:
            print(f"Error setting up MQTT client: {e}")
            raise

    def _on_connect(self, client, userdata, flags, rc):
        """Callback for when the client connects to the broker."""
        if rc == 0:
            print("Connected to MQTT broker")
        else:
            print(f"Failed to connect to MQTT broker with code: {rc}")

    def _on_disconnect(self, client, userdata, rc):
        """Callback for when the client disconnects from the broker."""
        if rc != 0:
            print(f"Unexpected disconnection from MQTT broker with code: {rc}")

    def _on_publish(self, client, userdata, mid):
        """Callback for when a message is published."""
        pass

    @contextmanager
    def connection(self):
        """Context manager for MQTT connection."""
        try:
            self.client.connect(self.broker, self.port, 60)
            self.client.loop_start()
            yield self.client
        finally:
            self.client.loop_stop()
            self.client.disconnect()

class DataSimulator:
    def __init__(self, excel_path: str, db_url: str):
        self.excel_path = excel_path
        self.db_url = db_url
        self.df_tag = None
        self.df_guide = None
        self.guide_map = None
        self.last_produced_push: Dict[str, datetime] = {}
        self.last_reject_push: Dict[str, datetime] = {}
        self.produced_count: Dict[str, int] = {}
        self.active_plans: Dict[str, dict] = {}  # hierarchy -> plan dict
        self.active_plan_ids: Dict[str, int] = {}  # hierarchy -> plan id
        self._load_tags()
        self.engine = create_engine(self.db_url, pool_recycle=300)
        self.conn = self.engine.connect()  # Persistent connection
        self.last_plan_refresh = None
        self.plan_refresh_interval = timedelta(minutes=1)

    def _load_tags(self):
        """Load and validate Excel data."""
        try:
            xls = pd.read_excel(self.excel_path, sheet_name=None)
            self.df_tag = xls.get(SHEET_TAGS, pd.DataFrame())
            self.df_guide = xls.get('data_guide', pd.DataFrame())
            if self.df_tag.empty or self.df_guide.empty:
                raise ValueError("Tags or data_guide sheet is empty or missing")
            # Build a mapping from hierarchy to row (as dict)
            self.guide_map = {
                (str(row['hierarchy']).strip(), str(row['name']).strip()): row
                for _, row in self.df_guide.iterrows()
            }
        except Exception as e:
            print(f"Error loading Excel data: {e}")
            raise

    def refresh_active_plans(self):
        now = datetime.now()
        try:
            result = self.conn.execute(text("""
                SELECT pp.id, pp.hierarchy, pp.product, pp.process_order, pp.start_time, pp.end_time, pp.project_id, p.name
                FROM productionplan pp
                JOIN product p ON pp.product = p.id
                WHERE pp.start_time <= :now AND pp.end_time >= :now
            """), {"now": now})
            plans = result.mappings().fetchall()
        except Exception as e:
            print(f"[ERROR] DB connection lost, retrying: {e}")
            try:
                self.conn.close()
            except Exception:
                pass
            self.engine.dispose()
            time.sleep(2)
            self.conn = self.engine.connect()
            return
        new_active_plans = {}
        new_active_plan_ids = {}
        for plan in plans:
            hierarchy = plan['hierarchy']
            product_name = plan['name']
            new_active_plans[hierarchy] = plan
            new_active_plan_ids[hierarchy] = plan['id']
            # Reset counters for hierarchies with a new plan
            if hierarchy not in self.active_plan_ids or self.active_plan_ids[hierarchy] != plan['id']:
                self.produced_count[hierarchy] = 0
                self.last_produced_push[hierarchy] = None
                self.last_reject_push[hierarchy] = None
        self.active_plans = new_active_plans
        self.active_plan_ids = new_active_plan_ids
        self.last_plan_refresh = now
        print(f"[INFO] Refreshed active production plans at {now}")

    @staticmethod
    def form_message(data: Dict[str, Any], timestamp: float, project_id: int, site_id: str) -> Dict[str, Any]:
        """Form MQTT message with proper structure."""
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

    def process_production(self, client: mqtt.Client, plan: dict, now: datetime):
        """Process and publish production data."""
        hierarchy = plan['hierarchy']
        project_id = plan['project_id']
        site_id = hierarchy.split("$")[0]
        product_name = plan.get('name')
        guide_row = self.guide_map.get((hierarchy, str(product_name).strip()))
        if guide_row is not None:
            freq_minutes = int(guide_row['frequency'])
            actual_quant = int(guide_row['actual quant'])
            reject_per_hr = int(guide_row['reject_per_hr'])
            total_units = int(guide_row['total_units'])
        else:
            print(f"[WARN] No data_guide entry for hierarchy {hierarchy} and product {product_name}, skipping.")
            return
        tag_prod = self.df_tag.iloc[0]['total_produced_units']
        tag_reject = self.df_tag.iloc[0]['reject_units']
        if hierarchy not in self.produced_count:
            self.produced_count[hierarchy] = 0
        if (self.last_produced_push.get(hierarchy) is None or
            (now - self.last_produced_push[hierarchy]) >= timedelta(minutes=freq_minutes)):
            self.produced_count[hierarchy] += total_units
            data = {
                f"{hierarchy}${tag_prod}": self.produced_count[hierarchy],
                f"{hierarchy}${tag_prod}_hierarchy": hierarchy
            }
            client.publish(MQTT_TOPIC, json.dumps(self.form_message(data, now.timestamp(), project_id, site_id)))
            print(f"📤 Produced ({self.produced_count[hierarchy]}) pushed for {hierarchy} at {now.time()}")
            self.last_produced_push[hierarchy] = now
        if (self.last_reject_push.get(hierarchy) is None or
            (now - self.last_reject_push[hierarchy]) >= timedelta(hours=1)):
            data = {
                f"{hierarchy}${tag_reject}": reject_per_hr,
                f"{hierarchy}${tag_reject}_hierarchy": hierarchy
            }
            client.publish(MQTT_TOPIC, json.dumps(self.form_message(data, now.timestamp(), project_id, site_id)))
            print(f"📤 Reject pushed for {hierarchy} at {now.time()}")
            self.last_reject_push[hierarchy] = now

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

def data_simulation():
    """Main function to run the data simulation."""
    print("🚀 Starting smart incremental simulation...")
    
    simulator = None
    try:
        mqtt_client = MQTTClient(MQTT_BROKER, MQTT_PORT)
        simulator = DataSimulator(EXCEL_FILE_PATH, DB_URL)

        with mqtt_client.connection() as client:
            while True:
                now = datetime.now()
                if (simulator.last_plan_refresh is None or
                    (now - simulator.last_plan_refresh) >= simulator.plan_refresh_interval):
                    simulator.refresh_active_plans()
                for hierarchy, plan in simulator.active_plans.items():
                    simulator.process_production(client, plan, now)
                time.sleep(5)

    except KeyboardInterrupt:
        print("🛑 User stopped.")
    except Exception as e:
        print(f"Error in data simulation: {e}")
        raise
    finally:
        if simulator is not None:
            simulator.close()
        print("🔌 MQTT disconnected.")

if __name__ == "__main__":
    data_simulation()


