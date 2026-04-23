"""Domain model for hourly weather observations (Open-Meteo)."""

from datetime import datetime

from pydantic import BaseModel


class WeatherObservation(BaseModel):
    location_id: str
    hour_utc: datetime
    temperature_c: float | None = None
    precipitation_mm: float | None = None
    rain_mm: float | None = None
    wind_speed_kmh: float | None = None
    wind_direction_deg: float | None = None
    wind_direction_name: str = ""
    cloud_cover_pct: float | None = None
    weather_code: int = 0
    weather_desc: str = ""
    source: str = "ingestion.openmeteo"
