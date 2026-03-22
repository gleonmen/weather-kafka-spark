import os
import requests
from datetime import datetime
from configparser import ConfigParser
from services.kafka_service import KafkaService

def get_weather(city):
    """Obtiene datos del clima desde WeatherAPI para una ciudad"""
    config_path = 'config.properties'
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"No se encontró el archivo de configuración: {config_path}")

    config = ConfigParser()
    with open(config_path, 'r', encoding='utf-8') as f:
        raw = f.read().strip()

    if raw and not raw.startswith('['):
        # Archivo tipo properties sin sección [DEFAULT]
        raw = '[DEFAULT]\n' + raw

    config.read_string(raw)
    WEATHER_API_KEY = config.get('DEFAULT', 'weather_api_key', fallback=None)
    if not WEATHER_API_KEY:
        raise ValueError("WEATHER_API_KEY not found in config.properties")

    url = f"http://api.weatherapi.com/v1/current.json?key={WEATHER_API_KEY}&q={city}&aqi=no"
   
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        weather = {
            "city": data["location"]["name"],
            "region": data["location"]["region"],
            "country": data["location"]["country"],
            "temperature_c": data["current"]["temp_c"],
            "humidity": data["current"]["humidity"],
            "wind_kph": data["current"]["wind_kph"],
            "condition": data["current"]["condition"]["text"],
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M")
        }
        return weather
    except Exception as e:
        print(f"Error obteniendo clima de {city}: {e}")
        return None

def main():
    # Initialize the service
    kafkaService = KafkaService(
        bootstrap_servers="127.0.0.1:9092",
        topic="weather-topic",
        group_id="weather-group"
    )

    # Produce weather messages for cities
    cities = ["New York", "London", "Tokyo", "Bogota", "Paris", "Sydney"]
    for city in cities:
        weather = get_weather(city)
        print(f"Weather for {city}: {weather}")

        if weather:
            kafkaService.produce(key=f"weather-{city}", value=weather)


if __name__ == "__main__":
    main()