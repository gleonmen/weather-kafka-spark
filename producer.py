import requests
from datetime import datetime
from services.kafka_service import KafkaService
from services.weather_utils import WeatherUtils


def get_weather(city):
    """Obtiene datos del clima desde WeatherAPI para una ciudad"""
    api_key = WeatherUtils.get_value_by_key(key="weather_api_key")
    url = f"http://api.weatherapi.com/v1/current.json?key={api_key}&q={city}&aqi=no"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        return {
            "city": data["location"]["name"],
            "region": data["location"]["region"],
            "country": data["location"]["country"],
            "temperature_c": data["current"]["temp_c"],
            "humidity": data["current"]["humidity"],
            "wind_kph": data["current"]["wind_kph"],
            "condition": data["current"]["condition"]["text"],
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }
    except Exception as e:
        print(f"Error obteniendo clima de {city}: {e}")
        return None

def main():
    # Initialize the service
    kafkaTopic = WeatherUtils.get_value_by_key(key="topic")
    brokers = WeatherUtils.get_value_by_key(key="brokers")
    group = WeatherUtils.get_value_by_key(key="group")

    kafkaService = KafkaService(
        bootstrap_servers=brokers,
        topic=kafkaTopic,
        group_id=group
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