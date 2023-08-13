import asyncio
from bs4 import BeautifulSoup
from aiohttp import ClientSession
from fake_useragent import UserAgent
from playwright.async_api import async_playwright

from typing import List, Dict, Any
from stats_scraper.logger import logger


class Scraper:
    base_url = "https://www.hltv.org"
    
    def __init__(
        self,
        max_concurrent_tasks: int = 25
    ) -> None:
        self.max_concurrent_tasks = max_concurrent_tasks
        self.semaphore = asyncio.Semaphore(self.max_concurrent_tasks)
    
    async def __aenter__(self):
        self.session = ClientSession()
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