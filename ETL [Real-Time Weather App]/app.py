import os
import time
import logging
from datetime import datetime, timezone, timedelta
import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)

WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
OUTPUT_FILE = "weather_vijayapur.csv"
INTERVAL_SECONDS = 1

PLACE = {
    "city": "Vijayapura",
    "state": "Karnataka",
    "country": "IN",
    "latitude": 16.8302,
    "longitude": 75.7100,
}

IST = timezone(timedelta(hours=5, minutes=30))

WEATHER_CODES = {
    0: "clear sky",
    1: "mainly clear",
    2: "partly cloudy",
    3: "overcast",
    45: "fog",
    48: "rime fog",
    51: "light drizzle",
    53: "moderate drizzle",
    55: "dense drizzle",
    61: "light rain",
    63: "moderate rain",
    65: "heavy rain",
    71: "light snow",
    73: "moderate snow",
    75: "heavy snow",
    80: "light showers",
    81: "moderate showers",
    82: "violent showers",
    95: "thunderstorm",
    96: "thunderstorm with hail",
    99: "severe thunderstorm with hail",
}


def extract_data(session: requests.Session) -> dict:
    response = session.get(
        WEATHER_URL,
        params={
            "latitude": PLACE["latitude"],
            "longitude": PLACE["longitude"],
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code",
            "wind_speed_unit": "ms",
            "timezone": "Asia/Kolkata",
        },
        timeout=10,
    )

    response.raise_for_status()

    return response.json()


def transform_data(data: dict) -> dict:
    try:
        current = data["current"]

        return {
            "city": PLACE["city"],
            "state": PLACE["state"],
            "country": PLACE["country"],
            "temperature_c": current["temperature_2m"],
            "humidity_pct": current["relative_humidity_2m"],
            "description": WEATHER_CODES.get(
                current["weather_code"],
                "unknown",
            ),
            "wind_speed_mps": current["wind_speed_10m"],
            "timestamp_ist": datetime.now(IST).isoformat(
                timespec="seconds"
            ),
        }

    except (KeyError, TypeError) as e:
        raise ValueError(
            f"Unexpected API response structure: {e}"
        ) from e


def load_data(data: dict, filename: str) -> None:
    df = pd.DataFrame([data])

    write_header = (
        not os.path.exists(filename)
        or os.path.getsize(filename) == 0
    )

    df.to_csv(
        filename,
        mode="a",
        header=write_header,
        index=False,
    )


def run_etl_once(session: requests.Session) -> dict:
    raw = extract_data(session)
    transformed = transform_data(raw)
    load_data(transformed, OUTPUT_FILE)

    return transformed


if __name__ == "__main__":
    logger.info(
        "Collecting weather for %s, %s every %s second(s). Press Ctrl+C to stop.",
        PLACE["city"],
        PLACE["state"],
        INTERVAL_SECONDS,
    )

    count = 0
    next_run = time.monotonic()

    session = requests.Session()

    try:
        while True:
            try:
                record = run_etl_once(session)
                count += 1

                logger.info(
                    "Record #%d | %s | %.1f°C | %s%% humidity | %s",
                    count,
                    record["city"],
                    record["temperature_c"],
                    record["humidity_pct"],
                    record["description"],
                )

            except requests.exceptions.HTTPError as e:
                if (
                    e.response is not None
                    and e.response.status_code == 429
                ):
                    logger.warning(
                        "Rate limited (429). Waiting 60 seconds..."
                    )
                    time.sleep(60)
                    next_run = time.monotonic()
                else:
                    logger.error(
                        "Cycle failed, will retry: %s",
                        e,
                    )

            except (
                requests.exceptions.RequestException,
                ValueError,
            ) as e:
                logger.error(
                    "Cycle failed, will retry: %s",
                    e,
                )

            next_run += INTERVAL_SECONDS

            time.sleep(
                max(
                    0,
                    next_run - time.monotonic(),
                )
            )

    except KeyboardInterrupt:
        logger.info(
            "Stopped. %d records saved to %s",
            count,
            OUTPUT_FILE,
        )