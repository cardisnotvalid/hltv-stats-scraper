import yaml
from stats_scraper.paths import CONFIG_PATH


def load_config() -> dict:
    with open(CONFIG_PATH) as file:
        return yaml.load(file, Loader=yaml.FullLoader)