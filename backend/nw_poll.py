import time
import json
import requests
import psycopg
import os

DB = os.environ.get("DATABASE_URL")
URL = "https://www.kingdomgame.net/WebService/Kingdoms.asmx/GetNetworthOverTime"
HEADERS = {"Content-Type": "application/json"}

POLL_INTERVAL = 240  # 4 minutes

def poll():
    while True:
        try:
            resp = requests.post(URL, headers=HEADERS, data="{}")
            payload = json.loads(resp.json()["d"])
            points = payload["dataPoints"]

            with psycopg.connect(DB) as conn:
                with conn.cursor() as cur:
                    for p in points:
                        cur.execute(
                            """
                            INSERT INTO nw_history (kingdom, networth, tick_time)
                            VALUES (%s, %s, %s)
                            ON CONFLICT DO NOTHING
                            """,
                            ("GLOBAL", p["networth"], p["datetime"])
                        )
                conn.commit()

            print(f"Inserted {len(points)} NW points")
        except Exception as e:
            print("Poll error:", e)

        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    print("NWOT poller running (4 min interval)")
    poll()
