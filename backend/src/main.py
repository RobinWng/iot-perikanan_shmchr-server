import json
import logging
import math
import paho.mqtt.client as mqtt

#mqtt config
MQTT_BROKER = "mosquitto"
MQTT_PORT = 1883
MQTT_TOPIC = "iot-perikanan/+/+/telemetry"
CLIENT_ID = "iot-perikanan-backend"

logging.basicConfig(
    level= logging.INFO,
    format= "%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

#data validation
def validate_telemetry(data):
    required_fields = [
        "ph",
        "doCon",
        "doSat",
        "temperature"
    ]

    missing_fields = [
        field for field in required_fields
        if field not in data
    ]

    if missing_fields:
        return None, f"Missing Fields: {missing_fields}"

    validated_data = {}
    invalid_fields = {}

    for field in required_fields:
        value = data[field]
        if isinstance(value, bool) or not isinstance(value, (int,float)): #ini saringan untuk tipe data lain selain float dan int
            invalid_fields[field] = "invalid data types"
            continue
        if not math.isfinite(value): #kalau ini saringan untuk nilai NaN, infinity, dan -infinity
            invalid_fields[field] = "Value is not Finite"
            continue
        if value == -1.0: #yg ini saringan buat data yang invalid dari sensor karna di pio kt masukan -1.0f untuk pembacaan invalid
            invalid_fields[field] = "Sensor reading Invalid" 
            continue

        validated_data[field] = value

    return {
        "values": validated_data,
        "invalid_fields": invalid_fields
    }, None


#mqtt callbacks
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        logger.info("Connected to MQTT Broker")
        client.subscribe(MQTT_TOPIC)
        logger.info(f"Subscribed to: {MQTT_TOPIC}")
    else : 
        logger.error(f"Failed to Connect to MQTT Broker: {reason_code}")


def on_message(client, userdata, message):
    topic = message.topic
    try: 
        payload = message.payload.decode("utf-8")
        data = json.loads(payload)

        if not isinstance(data,dict):
            logger.warning(f"Invalid telemetry format: {topic}")
            return

        result, error = validate_telemetry(data)
        if error:
            logger.error(f"Telemetry validation failed: {error}")
            return
        

        logger.info(f"Received topic: {topic}")
        logger.info(f"Received data: {data}")

        ph = result["values"].get("ph")
        do_con = result["values"].get("doCon")
        do_sat = result["values"].get("doSat")
        temperature = result["values"].get("temperature")

        invalid_fields = result["invalid_fields"]

        if invalid_fields : 
            for field, reason in invalid_fields.items():
                logger.warning(f"{field}: {reason}")

        if ph is not None:
            logger.info(f"pH: {ph}")
        if do_con is not None:
            logger.info(f"DO Concentration: {do_con}")
        if do_sat is not None:
            logger.info(f"DO Saturation: {do_sat}")
        if temperature is not None:
            logger.info(f"Temperature: {temperature}")


    except UnicodeDecodeError: 
        logger.error(f"Payload is not valid UTF-8: {topic}")
    except json.JSONDecodeError: 
        logger.error(f"Payload is not valid JSON: {topic}")
    except KeyError as error:
        logger.error(f"Missing telemetry field: {error}")

def main():
    logger.info("Starting IoT Perikanan Backend...")

    client = mqtt.Client(
        callback_api_version= mqtt.CallbackAPIVersion.VERSION2,
        client_id= CLIENT_ID,
        protocol= mqtt.MQTTv311
    )
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect_async(
        MQTT_BROKER,
        MQTT_PORT,
        keepalive=60
    )

    client.loop_forever(retry_first_connection=True)

if __name__ == "__main__":
    main()