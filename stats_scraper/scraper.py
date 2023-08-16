import asyncio
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from playwright.async_api import async_playwright

from typing import List, Dict, Any
from stats_scraper.logger import logger


class Scraper:
    base_url = "https://www.hltv.org"
    
    def __init__(self, max_concurrent_tasks: int = 5) -> None:
        self.session = ClientSession()
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
    
    async def get_all_match_urls(self) -> List[str]:
        logger.info("Процесс получение всех ссылок на матчи")
        
        page_content = await self.get_page_content(f"{self.base_url}/matches")
        soup = BeautifulSoup(page_content, "lxml")
        return [
            self.base_url + item.select_one(".match").get("href") 
            for item in soup.select(".upcomingMatch[team1]")
        ]
    
    async def fetch_lineup(self, match_url: str) -> List[Dict[str, Any]]:
        page_content = await self.get_page_content(match_url)
        soup = BeautifulSoup(page_content, "lxml")
        return [{
            "team": item.select_one(".flex-align-center > a").getText(strip=True),
            "world_rank": int(item.select_one(".teamRanking > a").getText(strip=True).rsplit("#")[-1]),
            "players": [
                player.getText(strip=True)
                for player in item.select(".player > .flagAlign")
            ]
        } for item in soup.select(".lineup")]
        
    async def fetch_match_stats(self, match_url: str) -> List[Dict[str, Any]]:
        page_content = await self.get_page_content(match_url)
        soup = BeautifulSoup(page_content, "lxml")
        team_1 = soup.select_one(".map-stats-infobox-header > :nth-child(2)").getText(strip=True)
        team_2 = soup.select_one(".map-stats-infobox-header > :nth-child(3)").getText(strip=True)
        return [{
            "map": item.select_one(".mapname").getText(strip=True),
            "stats": {
                team_1: (
                    item.select_one(":nth-child(2) > .map-stats-infobox-winpercentage").getText(strip=True), 
                    item.select_one(":nth-child(2) > .map-stats-infobox-maps-played").getText(strip=True)
                ),
                team_2: (
                    item.select_one(":nth-child(3) > .map-stats-infobox-winpercentage").getText(strip=True),
                    item.select_one(":nth-child(3) > .map-stats-infobox-maps-played").getText(strip=True)
                )
            }
        } for item in soup.select(".map-stats-infobox-maps")]