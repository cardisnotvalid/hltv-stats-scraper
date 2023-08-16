import asyncio
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from playwright.async_api import async_playwright

import time
from typing import List, Dict, Any
from stats_scraper.logger import logger


class Scraper:
    base_url = "https://www.hltv.org"
    
    def __init__(self, max_concurrent_tasks: int = 5) -> None:
        self.sessoin = ClientSession()
        self.max_concurrent_tasks = max_concurrent_tasks
        self.semaphore = asyncio.Semaphore(self.max_concurrent_tasks)
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, *args):
        await self.session.close()
    
    async def get_page_content(self, url: str) -> str:
        logger.debug(f"Получение содержимого страницы: {url}")
        
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            context = await browser.new_context(user_agent=UserAgent().random)
            page = await context.new_page()
            await page.goto(url, wait_until="load")
            return await page.content()
    
    async def collect_match_urls(self, page_content: str) -> List[str]:
        soup = BeautifulSoup(page_content, "lxml")
        return [
            self.base_url + item.select_one(".match").get("href") 
            for item in soup.select(".upcomingMatch[team1]")
        ]
    
    async def collect_all_match_data(self):
        logger.info("Процесс получение всех данных о матчах")
        
        matches_content = await self.get_page_content(f"{self.base_url}/matches")
        matches_url = await self.collect_match_urls(matches_content)
        
        logger.info(f"Кол-во предстоящих матчей: {len(matches_url)}")
        
        async def process_match_data(url: str):
            async with self.semaphore:
                await self.fetch_match_data(url)
                
        match_data_tasks = [process_match_data(url) for url in matches_url]
        
        start = time.monotonic()
        await asyncio.gather(*match_data_tasks)
        delta = round(time.monotonic() - start, 3)
        
        logger.info(f"Время выполнения: {delta} сек")
    
    async def get_player_stats(self, url: str) -> Dict[str, Any]:
        player_content = await self.get_page_content(url)
        soup = BeautifulSoup(player_content, "lxml")
        
        header_key = soup.select(".summaryStatBreakdownSubHeader")
        header_value = soup.select(".summaryStatBreakdownData")
        
        player_data = {
            "nickname": self.get_text(soup, ".summaryNickname"),
            "realname": self.get_text(soup, ".summaryRealname")
        }
        
        player_stats = {}
        for stats in soup.select(".stats-rows"):
            for stats_row in stats.select("stats-row"):
                row = stats_row.select_one("span").rsplit()
                player_stats.update({row[0]: row[1]})
        
        return player_data
    
    async def fetch_match_lineups(self, page_content: str):
        soup = BeautifulSoup(page_content, "lxml")
        lineups = soup.select(".lineup")
        
        teams = []
        for lineup in lineups:
            team_name = self.get_text(lineup, ".text-ellipsis")
            team_url = self.base_url + lineup.select_one(".text-ellipsis").get("href")
            team_world_rank = int(self.get_text(lineup, ".a-reset").rsplit("#")[-1])
            
            team_players_tasks = [
                self.get_player_stats(
                    f"{self.base_url}/stats/players/{item.get('data-player-id')}/{item.getText(strip=True)}"
                )
                for item in lineup.select(".player-compare.flagAlign")
            ]

            team_players = await asyncio.gather(*team_players_tasks)
            
            teams.append({
                "name": team_name,
                "world_rank": team_world_rank,
                "players": team_players,
                "url": team_url
            })
        print(teams)
        return teams
    
    async def fetch_match_data(self, url: str):
        match_content = await self.get_page_content(url)
        await self.fetch_match_lineups(match_content)
        
    def get_text(self, soup: BeautifulSoup, selector: str) -> str:
        text = soup.select_one(selector)
        if text:
            return text.getText(strip=True)
        else:
            return None