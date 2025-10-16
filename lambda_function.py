import json, os, boto3, time, random
from datetime import datetime, timezone

# Soil moisture bands + ET thresholds
MOISTURE_HIGH = float(os.environ.get("MOISTURE_HIGH", "40"))
MOISTURE_MIN  = float(os.environ.get("MOISTURE_MIN", "30"))
RAIN_SKIP     = float(os.environ.get("RAIN_SKIP", "0.1"))
ET_THRESHOLD  = float(os.environ.get("ET_THRESHOLD", "4.0"))
DEFAULT_WIND  = float(os.environ.get("DEFAULT_WIND", "2.0"))

DDB_TABLE  = os.environ.get("DDB_TABLE")                 # vine-events
S3_BUCKET  = os.environ.get("S3_BUCKET")                 # vine-events-bucket (data)
# === Manifest destination (CloudFront/S3 bucket that serves your dashboard) ===
MANIFEST_BUCKET = os.environ.get("MANIFEST_BUCKET", "vine-events-bucket")
MANIFEST_KEY    = os.environ.get("MANIFEST_KEY", "manifest.json")  # or e.g. "manifests/manifest.json"

# Optional: invalidate /manifest.json to bust CF cache immediately
CLOUDFRONT_DISTRIBUTION_ID = os.environ.get("CLOUDFRONT_DISTRIBUTION_ID")  # e.g. "E3ABCDEF..."

ddb = boto3.client("dynamodb")
s3  = boto3.client("s3")
iot = boto3.client("iot-data")
cf  = boto3.client("cloudfront") if CLOUDFRONT_DISTRIBUTION_ID else None

def put_to_ddb(item):
    if not DDB_TABLE:
        return
    ddb.put_item(
        TableName=DDB_TABLE,
        Item={
            "deviceId": {"S": item["deviceId"]},
            "timestamp": {"S": item["timestamp"]},
            "soilMoisture": {"N": str(item.get("soilMoisture", -1))},
            "temperature": {"N": str(item.get("temperature", -1))},
            "humidity": {"N": str(item.get("humidity", -1))},
            "rainfallLastHour": {"N": str(item.get("rainfallLastHour", 0))},
            "et0": {"N": str(item.get("et0", 0))},
            "decision": {"S": item["decision"]},
            "reason": {"S": item["reason"]},
        },
    )

def append_json_to_s3(bucket, prefix, item):
    """
    Writes one JSON record per object (not JSONL) using your current partitioning.
    Returns the S3 key that was written.
    """
    key = (
        f"{prefix}/year={item['timestamp'][:4]}"
        f"/month={item['timestamp'][5:7]}"
        f"/day={item['timestamp'][8:10]}"
        f"/deviceId={item['deviceId']}/{item['timestamp']}.json"
    )
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=(json.dumps(item, separators=(",", ":")) + "\n").encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=31536000, immutable"
    )
    return key

def publish_actuator(msg):
    iot.publish(
        topic="agri/actuators/irrigation",
        qos=0,
        payload=json.dumps(msg).encode("utf-8")
    )

def estimate_et0(temp_c: float, humidity_pct: float, wind_mps: float) -> float:
    """Simplified ET0 estimate for demo (not full FAO-56)."""
    et = 0.15 * max(temp_c, 0) + 0.6 * max(wind_mps, 0) - 0.05 * max(humidity_pct, 0)
    return max(et, 0.0)

# ---------- Manifest helpers ----------

def _load_manifest(bucket: str, key: str) -> dict:
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read()
        return json.loads(data or "{}")
    except s3.exceptions.NoSuchKey:
        return {}
    except s3.exceptions.ClientError as e:
        # If manifest doesn't exist yet
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return {}
        raise

def _save_manifest(bucket: str, key: str, manifest: dict):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(manifest, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
        CacheControl="no-cache"
    )

def update_manifest_with_object(obj_key: str, device_id: str):
    """
    Merge-write manifest with retries to reduce race overwrites.
    For very high concurrency, prefer daily manifests (e.g., key=f"manifests/{today}.json").
    """
    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        try:
            manifest = _load_manifest(MANIFEST_BUCKET, MANIFEST_KEY)

            if not isinstance(manifest, dict):
                manifest = {}
            manifest.setdefault("version", 1)
            objects = manifest.setdefault("objects", [])

            # Dedup: keep at most one entry per key
            seen = set()
            out = []
            for o in objects:
                k = o.get("key")
                if k and k not in seen:
                    seen.add(k)
                    out.append(o)

            # Add/update this object
            if obj_key not in seen:
                out.append({"key": obj_key, "deviceId": device_id})
            else:
                # ensure deviceId present for this key
                out = [{"key": o["key"], "deviceId": o.get("deviceId", device_id) if o["key"] == obj_key else o.get("deviceId")}
                       if isinstance(o, dict) else o for o in out]

            manifest["objects"] = out

            _save_manifest(MANIFEST_BUCKET, MANIFEST_KEY, manifest)
            return  # success
        except Exception as e:
            # small backoff + jitter, then retry
            if attempt == max_attempts:
                raise
            time.sleep(0.05 * attempt + random.uniform(0, 0.05))

def maybe_invalidate_cloudfront():
    if not cf:
        return
    # Invalidate only the manifest to force immediate edge refresh
    cf.create_invalidation(
        DistributionId=CLOUDFRONT_DISTRIBUTION_ID,
        InvalidationBatch={
            "Paths": {"Quantity": 1, "Items": [f"/{MANIFEST_KEY}"]},
            "CallerReference": f"manifest-{int(time.time()*1000)}"
        }
    )

# ---------- Lambda entry ----------

def lambda_handler(event, context):
    payload = event if isinstance(event, dict) else json.loads(event)

    device_id = payload.get("deviceId", "unknown")
    ts = payload.get("timestamp") or datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")

    soil = float(payload.get("soilMoisture", -1))
    rain = float(payload.get("rainfallLastHour", 0))
    temp = float(payload.get("temperature", -1))
    hum  = float(payload.get("humidity", -1))
    wind = float(payload.get("windSpeed", DEFAULT_WIND))

    et0 = estimate_et0(temp, hum, wind)

    # Decision logic
    if soil >= MOISTURE_HIGH:
        decision = "IRRIGATION_OFF"
        reason = f"High soil {soil}% >= {MOISTURE_HIGH}%"
    elif soil < MOISTURE_MIN:
        if rain <= RAIN_SKIP:
            decision = "IRRIGATION_ON"
            reason = f"Low soil {soil}% < {MOISTURE_MIN}%, rain {rain}mm <= {RAIN_SKIP}mm"
        else:
            decision = "IRRIGATION_OFF"
            reason = f"Low soil but recent rain {rain}mm > {RAIN_SKIP}mm"
    else:
        if et0 > ET_THRESHOLD and rain <= RAIN_SKIP:
            decision = "IRRIGATION_ON"
            reason = f"Mid soil {soil}%, ET0 {et0:.2f}mm/day > {ET_THRESHOLD}"
        else:
            decision = "IRRIGATION_OFF"
            reason = f"Mid soil {soil}%, ET0 {et0:.2f}mm/day ≤ {ET_THRESHOLD} or rain"

    record = {
        "deviceId": device_id,
        "timestamp": ts,
        "soilMoisture": soil,
        "temperature": temp,
        "humidity": hum,
        "rainfallLastHour": rain,
        "windSpeed": wind,
        "et0": et0,
        "decision": decision,
        "reason": reason
    }

    # Persist
    if DDB_TABLE:
        put_to_ddb(record)

    obj_key = None
    if S3_BUCKET:
        obj_key = append_json_to_s3(S3_BUCKET, "raw", record)  # returns key

    # Update manifest if we successfully wrote object
    if obj_key:
        update_manifest_with_object(obj_key, device_id)
        maybe_invalidate_cloudfront()

    # Actuator publish
    publish_actuator({
        "deviceId": device_id,
        "action": decision,
        "reason": reason,
        "timestamp": ts,
        "et0": round(et0, 2)
    })

    print(json.dumps(record))
    return record
