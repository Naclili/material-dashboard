import boto3
import gzip
import json
import os
from datetime import datetime
import urllib3
import logging

# s3 = boto3.client("s3")
# dynamodb = boto3.resource("dynamodb")

# Configuration
S3_BUCKET = "airpn10-rawdata"
S3_PROCESSED_FOLDER = "iot_processed"
METADATA_TABLE = "airpn10-metadata"
OPENWEATHER_API_KEY = ''


http = urllib3.PoolManager()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        for record in event['Records']:
            bucket_name = record['s3']['bucket']['name']
            file_key = record['s3']['object']['key']

            # Get and decode the uploaded file
            response = s3.get_object(Bucket=bucket_name, Key=file_key)
            with gzip.GzipFile(fileobj=response['Body']) as f:
                payload = json.loads(f.read().decode("utf-8"))

            device_id = payload["device_id"]
            data_points = payload.get("data_points", [])

            # Validate and standardize timestamps
            for dp in data_points:
                if "Time" in dp:
                    dp["timestamp"] = datetime.fromisoformat(dp["Time"]).isoformat()
                else:
                    logger.warning(f"Missing 'Time' key in datapoint for device {device_id}")
                    dp["timestamp"] = datetime.utcnow().isoformat()

            # Fetch latest location from metadata
            metadata_table = dynamodb.Table(METADATA_TABLE)
            device_metadata = metadata_table.get_item(Key={"device_id": device_id}).get("Item", {})
            current_location = (device_metadata.get("latitude"), device_metadata.get("longitude"))

            # Check and update location if needed
            for dp in data_points:
                lat, lon = dp.get("latitude"), dp.get("longitude")
                if lat and lon and current_location != (lat, lon):
                    if current_location != (None, None):
                        distance = haversine_distance(lat, lon, *current_location)
                        if distance > 0.025:
                            metadata_table.update_item(
                                Key={"device_id": device_id},
                                UpdateExpression="SET latitude = :lat, longitude = :lon",
                                ExpressionAttributeValues={":lat": lat, ":lon": lon}
                            )
                    current_location = (lat, lon)

            # Fetch weather & pollution data (only once per upload)
            timestamps = [dp["timestamp"] for dp in data_points]
            lat, lon = current_location
            weather_data = get_weather_data(lat, lon, timestamps)
            air_data = get_air_pollution_data(lat, lon)

            # Match nearest weather data to each point
            for dp in data_points:
                dp["weather"] = find_nearest_by_time(dp["timestamp"], weather_data)
                dp["air_pollution"] = air_data
                dp["inversion_flag"] = is_inversion(dp)

            processed_payload = {
                "device_id": device_id,
                "data_points": data_points,
            }

            # Save processed data
            filename_out = os.path.basename(file_key).replace(".json.gz", "_processed.json")
            output_key = f"{S3_PROCESSED_FOLDER}/{filename_out}"
            s3.put_object(
                Bucket=bucket_name,
                Key=output_key,
                Body=json.dumps(processed_payload),
                ContentType="application/json"
            )

            logger.info(f"✅ Processed and saved file: {output_key}")

    except Exception as e:
        logger.error(f"Lambda failed: {e}", exc_info=True)
        # Optional: Save raw file somewhere else for re-processing

def get_weather_data(lat, lon, timestamps):
    try:
        url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric"
        response = http.request("GET", url)
        data = json.loads(response.data.decode("utf-8"))
        return data.get("list", [])
    except Exception as e:
        logger.error(f"Failed to fetch weather data: {e}")
        return []

def get_air_pollution_data(lat, lon):
    try:
        url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}"
        response = http.request("GET", url)
        data = json.loads(response.data.decode("utf-8"))
        return data.get("list", [{}])[0].get("components", {})
    except Exception as e:
        logger.error(f"Failed to fetch air pollution data: {e}")
        return {}

def find_nearest_by_time(target_time, weather_list):
    try:
        target_dt = datetime.fromisoformat(target_time.replace("Z", "+00:00"))
        return min(
            (entry for entry in weather_list),
            key=lambda x: abs(datetime.fromtimestamp(x["dt"]) - target_dt)
        ).get("main", {})
    except Exception as e:
        logger.warning(f"Could not match weather timestamp: {e}")
        return {}

def is_inversion(dp):
    try:
        upper_temp = dp.get("air_pollution", {}).get("temperature", 0)
        surface_temp = dp.get("weather", {}).get("temperature", 0)
        wind_speed = dp.get("weather", {}).get("wind_speed", 0)
        if upper_temp > surface_temp and wind_speed < 3:
            return True
        return False
    except Exception:
        return False

def haversine_distance(lat1, lon1, lat2, lon2):
    from math import radians, cos, sin, sqrt, atan2
    R = 6371  # km
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))
print(get_air_pollution_data(100,20))