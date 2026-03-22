import os
import requests
from datetime import datetime
from configparser import ConfigParser


class WeatherUtils:
    @staticmethod
    def get_api_key(config_path: str = "config.properties") -> str:
        """Carga la API key de WeatherAPI desde config.properties."""
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"No se encontró el archivo de configuración: {config_path}")

        config = ConfigParser()
        with open(config_path, "r", encoding="utf-8") as f:
            raw = f.read().strip()

        if raw and not raw.startswith("["):
            raw = "[DEFAULT]\n" + raw

        config.read_string(raw)
        api_key = config.get("DEFAULT", "weather_api_key", fallback=None)

        if not api_key:
            raise ValueError("WEATHER_API_KEY not found in config.properties")

        return api_key
