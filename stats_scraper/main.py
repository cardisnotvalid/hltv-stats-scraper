import asyncio
from bs4 import BeautifulSoup

from stats_scraper.scraper import Scraper
from stats_scraper.utils import save_data

async def main() -> None:
    async with Scraper() as scraper:
        match_urls = await scraper.get_all_match_urls()

        sem = asyncio.Semaphore(2)

        async def process_match(match_url):
            async with sem:
                match_name = match_url.rsplit("/")[-1]
                match_page_content = await scraper.get_page_content(match_url)
                
                match_data = await scraper.fetch_all_match_data(match_page_content)
                save_data(match_name, "pre-match-data", match_data)
                
                players = []
                for lineups in match_data["lineups"]:
                    players += lineups["players"]
                
                players_stats = await scraper.get_all_players_stats(players)
                save_data(match_name, "player-stats", players_stats)
        
                soup = BeautifulSoup(match_page_content, "lxml")
                analytic_url = "https://www.hltv.org" + soup.select_one(
                    ".matchpage-analytics-center-container"
                ).get("href")
        
                match_analytics = await scraper.fetch_match_analytics(analytic_url)
                save_data(match_name, "match-analytics", match_analytics)
                
                for lineup in match_data["lineups"]:
                    team_id, team_name = lineup["id"], lineup["team"]
                    team_name = team_name.replace(" ", "-").lower()
                    team_stats = await scraper.fetch_team_stats(team_id, team_name)
                    save_data(match_name, f"team-{team_name}", team_stats)

        await asyncio.gather(*(process_match(match_url) for match_url in match_urls))
