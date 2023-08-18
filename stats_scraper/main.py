from bs4 import BeautifulSoup
from datetime import datetime

from stats_scraper.scraper import Scraper
from stats_scraper.utils import save_data, save_data_to_txt


async def main() -> None:
    async with Scraper() as scraper:
        match_urls = await scraper.get_all_match_urls()

        for match_url in match_urls:
            time = datetime.now().strftime("%d%m%y%H%M%S")
            
            json_data = {}
            
            match_page_content = await scraper.get_page_content(match_url)
            
            match_name = await scraper.get_match_name(match_page_content) + f"({time})"
            match_type = await scraper.get_match_type(match_page_content)
            
            json_data["match_name"] = f"{match_name}. {match_type}"
            
            match_data = await scraper.fetch_all_match_data(match_page_content,match_name)
            
            json_data["match_pre_data"] = match_data
            save_data(match_name, "pre-match-data", match_data)

            players = []
            for lineups in match_data["lineups"]:
                players += lineups["players"]
            
            players_stats = await scraper.get_all_players_stats(players)
            
            json_data["match_player_stats"] = players_stats
            save_data(match_name, "player-stats", players_stats)
        
            soup = BeautifulSoup(match_page_content, "lxml")
            analytic_selector = soup.select_one(".matchpage-analytics-center-container")
            if analytic_selector:
                analytic_url = "https://www.hltv.org" + analytic_selector.get("href")
            
                match_analytics = await scraper.fetch_match_analytics(analytic_url)

                json_data["match_analytics"] = match_analytics
                save_data(match_name, "match-analytics", match_analytics)
            
            json_data["match_teams"] = []
            for lineup in match_data["lineups"]:
                team_id, team_name = lineup["id"], lineup["team"]
                team_name = team_name.replace(" ", "-").lower()
                team_stats = await scraper.fetch_team_stats(team_id, team_name)
                
                json_data["match_teams"].append(team_stats)
                
                save_data(match_name, f"team-{team_name}", team_stats)
                
            save_data_to_txt(json_data, match_name)