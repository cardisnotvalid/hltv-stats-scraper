import os
import yaml
import json

from stats_scraper.logger import logger
from stats_scraper.paths import CONFIG_PATH, OUT_DIR


def load_config() -> dict:
    with open(CONFIG_PATH) as file:
        return yaml.load(file, Loader=yaml.FullLoader)


def save_data(match_name: str, filename: str, json_data: dict) -> None:
    filename = f"{filename}.json"
    filepath = OUT_DIR / match_name / filename
    
    os.makedirs(OUT_DIR / match_name, exist_ok=True)
    
    with open(filepath, "w", encoding="utf-8") as file:
        json.dump(json_data, file, ensure_ascii=False, indent=4)
    logger.info(f"Файл сохранен {match_name}/{filename}")