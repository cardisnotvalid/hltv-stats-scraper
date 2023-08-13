from stats_scraper.scraper import Scraper
from stats_scraper.logger import logger


async def main() -> None:
    async with Scraper() as scraper:
        await scraper.collect_all_match_data()