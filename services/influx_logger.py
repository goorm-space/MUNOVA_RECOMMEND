import requests, yaml

with open("/app/config.yml", "r") as f:
    config = yaml.safe_load(f)

INFLUX_ENABLED = config["influxdb"].get("enabled", True)
INFLUX_HOST = config["influxdb"]["host"]
INFLUX_DB = config["influxdb"]["database"]

def send_metric(measurement, tags, fields):
    if not INFLUX_ENABLED:
        return
    tag_str = ",".join(f"{k}={v}" for k,v in tags.items())
    field_str = ",".join(f"{k}={v}" if isinstance(v,(int,float)) else f'{k}="{v}"' for k,v in fields.items())
    line = f"{measurement},{tag_str} {field_str}"
    try:
        requests.post(f"{INFLUX_HOST}/write", params={"db": INFLUX_DB}, data=line, timeout=2)
    except Exception as e:
        print(f"⚠️ Influx 전송 실패: {e}")