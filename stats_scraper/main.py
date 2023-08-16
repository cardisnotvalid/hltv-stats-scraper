from stats_scraper.scraper import Scraper
from stats_scraper.utils import load_config


config = load_config()
temp_url = "https://www.hltv.org/matches/2366146/zero-tenacity-vs-los-kogutos-european-pro-league-2nd-division-season-5"


async def main() -> None:
    max_concurrent_tasks = config["max_concurrent_tasks"]
    
    async with Scraper(max_concurrent_tasks) as scraper:
        # matches = await scraper.collect_all_match_data()
        
        await scraper.fetch_match_data(temp_url)