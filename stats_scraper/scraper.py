import asyncio
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from playwright.async_api import async_playwright

from datetime import datetime
from dateutil.relativedelta import relativedelta

from typing import List, Dict, Any
from stats_scraper.logger import logger


class Scraper:
    base_url = "https://www.hltv.org"
    base_player_url = "https://www.hltv.org/stats/players"
    base_teams_url = "https://www.hltv.org/stats/teams"
    
    def __init__(self) -> None:
        self.session = ClientSession()
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, *args):
        await self.session.close()
    
    async def get_page_content(self, url: str) -> str:
        logger.debug(f"Получение содержимого страницы: {url}")
        
        async with async_playwright() as playwright:
            await asyncio.sleep(5)
            
            browser = await playwright.chromium.launch(headless=True)
            context = await browser.new_context(user_agent=UserAgent().random)
            page = await context.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            return await page.content()
    
    async def get_all_match_urls(self) -> List[str]:
        logger.info("Процесс получение всех ссылок на матчи")
        
        page_content = await self.get_page_content(f"{self.base_url}/matches")
        soup = BeautifulSoup(page_content, "lxml")
        return [
            self.base_url + item.select_one(".match").get("href") 
            for item in soup.select_one(".upcomingMatchesSection").select(".upcomingMatch[team1]")
        ]
    
    async def fetch_all_match_data(self, page_content: str, match_name: str) -> Dict[str, Any]:
        logger.info(f"Получение данных матча: {match_name}")
        
        result = await asyncio.gather(
            self.fetch_lineups(page_content),
            self.fetch_match_stats(page_content),
            self.fetch_past_3_month(page_content),
            self.fetch_head_to_head(page_content)
        )
        return {
            "lineups": result[0],
            "match_stats": result[1],
            "past_3_month": result[2],
            "head_to_head": result[3]
        }
    
    async def get_match_name(self, page_content: str) -> str:
        soup = BeautifulSoup(page_content, "lxml")
        return self.get_text(soup, ".event")
    
    async def get_match_type(self, page_content: str) -> str:
        soup = BeautifulSoup(page_content, "lxml")
        return self.get_text(soup, ".preformatted-text").split("*")[0].strip()
    
    async def fetch_lineups(self, page_content: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(page_content, "lxml")
        return [{
            "id":         int(item.select_one(".flex-align-center > a").get("href").split("/")[2]),
            "team":       self.get_text(item, ".flex-align-center > a"),
            "world_rank": int(self.get_text(item, ".teamRanking > a").rsplit("#")[-1]) if self.get_text(item, ".teamRanking > a") is not None else None,
            "players": [{
                "id":       player.get("data-player-id"),
                "nickname": player.getText(strip=True)
            } for player in item.select(".player > .flagAlign")]
        } for item in soup.select(".lineup")]
        
    async def fetch_match_stats(self, page_content: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(page_content, "lxml")
        team_1 = self.get_text(soup, ".map-stats-infobox-header > :nth-child(2)")
        team_2 = self.get_text(soup, ".map-stats-infobox-header > :nth-child(3)")
        return [{
            "map": self.get_text(item, ".mapname"),
            "stats": {
                team_1: (
                    self.get_text(item, ":nth-child(2) > .map-stats-infobox-winpercentage"),
                    self.get_text(item, ":nth-child(2) > .map-stats-infobox-maps-played")
                ),
                team_2: (
                    self.get_text(item, ":nth-child(3) > .map-stats-infobox-winpercentage"),
                    self.get_text(item, ":nth-child(3) > .map-stats-infobox-maps-played")
                )
            }
        } for item in soup.select(".map-stats-infobox-maps")]
        
    async def fetch_past_3_month(self, page_content: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(page_content, "lxml")
        return [{
            "team": self.get_text(item, ".past-matches-headline"),
            "matches": [{
                "team":  self.get_text(match, ".past-matches-teamname"),
                "cell":  self.get_text(match, ".past-matches-map"),
                "score": self.get_text(match, ".past-matches-score")
            }for match in item.select("tbody > tr")]
        } for item in soup.select(".past-matches > :nth-child(3) > .past-matches-box")]
        
    async def fetch_head_to_head(self, page_content: str) -> Dict[str, Any]:
        soup = BeautifulSoup(page_content, "lxml")
        head_to_head = soup.select_one(".head-to-head")
        return {
            "stats": {
                self.get_text(head_to_head, ".team1"): self.get_text(head_to_head, ".right-border > .bold"),
                self.get_text(head_to_head, ".team2"): self.get_text(head_to_head, ".left-border > .bold"),
                "overtimes": self.get_text(head_to_head, ".padding > :nth-child(3) > .bold")
            } if head_to_head else {
                "team1": None,
                "team2": None,
                "overtimes": None
            },
            "listing": [{
                "date":   self.get_text(row, ".date"),
                "team1":  self.get_text(row, ".team1"),
                "team2":  self.get_text(row, ".team2"), 
                "event":  self.get_text(row, ".event"),
                "map":    self.get_text(row, ".map > .dynamic-map-name-full"),
                "result": self.get_text(row, ".result")
            } for row in soup.select(".head-to-head-listing > table > tbody > tr") if row]
        }
        
    async def fetch_player_stats(self, page_content: str) -> Dict[str, Any]:
        soup = BeautifulSoup(page_content, "lxml")
        return {
            "nickname": self.get_text(soup, ".summaryNickname"),
            "realname": self.get_text(soup, ".summaryRealname"),
            "team":     self.get_text(soup, ".SummaryTeamname"),
            "age":      self.get_text(soup, ".summaryPlayerAge"),
            "short_stats": {
                self.get_text(stats, ".summaryStatTooltip > b"): self.get_text(stats, ".summaryStatBreakdownDataValue")
                for stats in soup.select(".summaryStatBreakdown")
            },
            "full_stats": {
                self.get_text(stats, ":nth-child(1)"): self.get_text(stats, ":nth-child(2)")
                for stats in soup.select(".stats-row")
            }
        }
        
    async def get_all_players_stats(self, players: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        tasks = []
        for player in players:
            id, nickname = player["id"], player["nickname"]
            page_content = await self.get_page_content(f"{self.base_player_url}/{id}/{nickname}")
            task = self.fetch_player_stats(page_content)
            tasks.append(task)
            
        return await asyncio.gather(*tasks)

    async def fetch_match_analytics(self, match_url: str) -> Dict[str, Any]:
        page_content = await self.get_page_content(match_url)
        soup = BeautifulSoup(page_content, "lxml")
        return {
            "analytics_summary": [{
                "team": self.get_text(item, ".team-name"),
                "analytic": [
                    self.get_text(analytic, ".analytics-insights-info")
                    for analytic in item.select(".analytics-insights-insight")
                ]
            } for item in soup.select(".analytics-insights-wrapper > .col-6")],
            "head_to_head": [{
                "team": self.get_text(hth, ".team-name"),
                "players": {
                    self.get_text(player, ".player-nickname"): {
                        "3 month": self.get_text(player, ".table-3-months"),
                        "event":   self.get_text(player, ".table-event")
                    }
                    for player in hth.select(".table-container > tbody > tr")
                },
                "last_matchs": [{
                    "team":  self.get_text(match, ".team-name"),
                    "score": self.get_text(match, ".recent-score"),
                    "type":  self.get_text(match, ".match-type")
                } for match in hth.select(".analytics-last-matches > a")]
            } for hth in soup.select(".analytics-head-to-head-container")],
            "past_3_month": [{
                "team":            self.get_text(p3m, ".team-name"),
                "match_map_count": self.get_text(p3m, ".match-map-count"),
                "matches": [{
                    "score":    self.get_text(match, "td:not(.best-bet):not(.handicap-data)"),
                    "handicap": self.get_text(match, ".handicap-data")
                } for match in p3m.select("tbody > tr")]
            } for p3m in soup.select(".analytics-handicap-wrapper > .col-6")],
            "map_handicap": [{
                "overall_data": {
                    "avg_rounds_lost_in_wins":  self.get_text(
                        handicap, 
                        ".analytics-handicap-map-data-overall-container > :nth-child(1) > :nth-child(1)"
                    ),
                    "avg_rounds_won_in_losses": self.get_text(
                        handicap, 
                        ".analytics-handicap-map-data-overall-container > :nth-child(2) > :nth-child(1)"
                    )
                },
                "individual_maps": [{
                    "map":                      self.get_text(item, ".mapname"),
                    "avg_rounds_lost_in_wins":  self.get_text(item, ":nth-child(2):not(.mapname)"),
                    "avg_rounds_won_in_losses": self.get_text(item, ":nth-child(3):not(.mapname)")
                } for item in handicap.select("tbody > tr")]
            } for handicap in soup.select(".analytics-handicap-map-wrapper > .col-6")],
            "map_stats": [{
                "map":        self.get_text(item, "td[rowspan]"),
                "team":       self.get_text(item, ".maps-team-name"),
                "first_pick": self.get_text(item, ".analytics-map-stats-pick-percentage"),
                "first_ban":  self.get_text(item, ".analytics-map-stats-ban-percentage"),
                "win":        self.get_text(item, ".analytics-map-stats-win-percentage"),
                "played":     self.get_text(item, ".analytics-map-stats-played"),
                "comment":    self.get_text(item, ".analytics-map-stats-comment")
            } for item in soup.select(".gtSmartphone-only > tbody > tr")]
        }
        
    async def fetch_team_stats(self, team_id: int, team_name: str) -> List[Dict[str, Any]]:
        logger.info(f"Получения статистики команды: {team_name}")
        
        today = datetime.now()
        three_months_ago = today - relativedelta(months=3)
        today_str = today.strftime('%Y-%m-%d')
        three_months_ago_str = three_months_ago.strftime('%Y-%m-%d')
        
        async def fetch_data(url_suffix: str, data_transformer: callable) -> Any:
            full_url = f"{self.base_teams_url}/{url_suffix}/{team_id}/{team_name}" \
                       f"?startDate={three_months_ago_str}&endDate={today_str}"
            page_content = await self.get_page_content(full_url)
            soup = BeautifulSoup(page_content, "lxml")
            return await data_transformer(soup)

        team_stats = {"team": team_name}
        data_fetchers = [
            ("", self._parse_overview),
            ("matches", self._parse_matches),
            ("maps", self._parse_maps),
            ("players", self._parse_players),
            ("players/flashes", self._parse_flashes),
            ("players/openingkills", self._parse_opening_kills)
        ]

        for suffix, transformer in data_fetchers:
            team_stats.update(await fetch_data(suffix, transformer))
        
        return team_stats


    async def _parse_overview(self, soup: BeautifulSoup) -> Dict[str, Any]:
        return {
            "overview": {
                self.get_text(item, ".small-label-below"): self.get_text(item, ".large-strong")
                for item in soup.select(".col.standard-box")
            }
        }

    async def _parse_matches(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        return {
            "matches": [{
                "date":     self.get_text(item, ".time"),
                "event":    self.get_text(item, ".gtSmartphone-only"),
                "opponent": self.get_text(item, ":nth-child(4)"),
                "map":      self.get_text(item, ".statsMapPlayed"),
                "result":   self.get_text(item, ".statsDetail"),
                "W/L":      self.get_text(item, ".text-center:not(.gtSmartphone-only)")
            } for item in soup.select(".stats-table > tbody > tr")]
        }

    async def _parse_maps(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        return {
            "maps": [{
                "map": self.get_text(item, ".map-pool"),
                "stats": {
                    self.get_text(row, ".strong"): self.get_text(row, ":not(.strong)")
                    for row in item.select(".stats-row")
                } 
            } for item in soup.select(".two-grid:not(.win-defeat-container) > .col")]
        }

    async def _parse_players(self, soup: BeautifulSoup) -> Dict[str, Any]:
        return {
            "overview": [{
                "nickname": self.get_text(item, ":nth-child(1)"),
                "maps":     self.get_text(item, ".statsDetail"),
                "rounds":   self.get_text(item, ":nth-child(3)"),
                "k-d diff": self.get_text(item, ":nth-child(4)"),
                "k/d":      self.get_text(item, ":nth-child(5)"),
                "rating":   self.get_text(item, ":nth-child(6)")
            } for item in soup.select(".stats-table > tbody > tr")]
        }

    async def _parse_flashes(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        return {
            "flashes": [{
                "nickname":    self.get_text(item, ":nth-child(1)"),
                "maps":        self.get_text(item, ".mapsCol"),
                "rounds":      self.get_text(item, ":nth-child(3)"),
                "thrown":      self.get_text(item, ":nth-child(4)"),
                "blinder":     self.get_text(item, ":nth-child(5)"),
                "opp_flashed": self.get_text(item, ":nth-child(6)"),
                "diff":        self.get_text(item, ":nth-child(7)"),
                "fa":          self.get_text(item, ":nth-child(8)"),
                "success":     self.get_text(item, ":nth-child(9)")
            } for item in soup.select(".stats-table > tbody > tr")]
        }

    async def _parse_opening_kills(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        return {
            "opening_kills": [{
                "nickname": self.get_text(item, ":nth-child(1)"),
                "maps":     self.get_text(item, ".mapsCol"),
                "rounds":   self.get_text(item, ":nth-child(3)"),
                "kpr":      self.get_text(item, ":nth-child(4)"),
                "dpr":      self.get_text(item, ":nth-child(5)"),
                "attempts": self.get_text(item, ":nth-child(6)"),
                "success":  self.get_text(item, ":nth-child(7)"),
                "rating":   self.get_text(item, ":nth-child(8)"),
            } for item in soup.select(".stats-table > tbody > tr")]
        }

    def get_text(self, soup: BeautifulSoup, selector: str) -> str | None:
        text = soup.select_one(selector)
        if text:
            return text.getText(strip=True)
        else:
            return None