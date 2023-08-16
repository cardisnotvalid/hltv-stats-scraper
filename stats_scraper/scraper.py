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
            for item in soup.select(".upcomingMatch[team1]")
        ]
    
    async def fetch_all_match_data(self, page_content: str) -> Dict[str, Any]:
        logger.info("Получение данных матча")
        
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
        
    async def fetch_lineups(self, page_content: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(page_content, "lxml")
        return [{
            "id": int(item.select_one(".flex-align-center > a").get("href").split("/")[2]),
            "team": item.select_one(".flex-align-center > a").getText(strip=True),
            "world_rank": int(item.select_one(".teamRanking > a").getText(strip=True).rsplit("#")[-1]),
            "players": [{
                "id": player.get("data-player-id"),
                "nickname": player.getText(strip=True)
            } for player in item.select(".player > .flagAlign")]
        } for item in soup.select(".lineup")]
        
    async def fetch_match_stats(self, page_content: str) -> List[Dict[str, Any]]:
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
        
    async def fetch_past_3_month(self, page_content: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(page_content, "lxml")
        return [{
            "team": item.select_one(".past-matches-headline").getText(strip=True),
            "matches": [{
                "team": match.select_one(".past-matches-teamname").getText(strip=True),
                "cell": match.select_one(".past-matches-map").getText(strip=True),
                "score": match.select_one(".past-matches-score").getText(strip=True)
            }for match in item.select("tbody > tr")]
        } for item in soup.select(".past-matches > :nth-child(3) > .past-matches-box")]
        
    async def fetch_head_to_head(self, page_content: str) -> Dict[str, Any]:
        soup = BeautifulSoup(page_content, "lxml")
        head_to_head = soup.select_one(".head-to-head")
        return {
            "stats": {
                head_to_head.select_one(".team1").getText(strip=True): 
                    head_to_head.select_one(".right-border > .bold").getText(strip=True),
                head_to_head.select_one(".team2").getText(strip=True):
                    head_to_head.select_one(".left-border > .bold").getText(strip=True),
                "overtimes": head_to_head.select_one(".padding > :nth-child(3) > .bold").getText(strip=True)
            },
            "listing": [{
                "date": row.select_one(".date").getText(strip=True),
                "team1": row.select_one(".team1").getText(strip=True),
                "team2": row.select_one(".team2").getText(strip=True),
                "event": row.select_one(".event").getText(strip=True),
                "map": row.select_one(".map > .dynamic-map-name-full").getText(strip=True),
                "result": row.select_one(".result").getText(strip=True)
            } for row in soup.select(".head-to-head-listing > table > tbody > tr")]
        }
        
    async def fetch_player_stats(self, page_content: str) -> Dict[str, Any]:
        soup = BeautifulSoup(page_content, "lxml")
        return {
            "nickname": soup.select_one(".summaryNickname").getText(strip=True),
            "realname": soup.select_one(".summaryRealname").getText(strip=True),
            "team": soup.select_one(".SummaryTeamname").getText(strip=True),
            "age": soup.select_one(".summaryPlayerAge").getText(strip=True),
            "short_stats": {
                stats.select_one(".summaryStatTooltip > b").getText(strip=True):
                    stats.select_one(".summaryStatBreakdownDataValue").getText(strip=True)
                for stats in soup.select(".summaryStatBreakdown")
            },
            "full_stats": {
                stats.select_one(":nth-child(1)").getText(strip=True):
                    stats.select_one(":nth-child(2)").getText(strip=True)
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
                "team": item.select_one(".team-name").getText(strip=True),
                "analytic": [
                    analytic.select_one(".analytics-insights-info").getText(strip=True)
                    for analytic in item.select(".analytics-insights-insight")
                ]
            } for item in soup.select(".analytics-insights-wrapper > .col-6")],
            "head_to_head": [{
                "team": hth.select_one(".team-name").getText(strip=True),
                "players": {
                    player.select_one(".player-nickname").getText(strip=True): {
                        "3 month": player.select_one(".table-3-months").getText(strip=True),
                        "event": player.select_one(".table-event").getText(strip=True)
                    }
                    for player in hth.select(".table-container > tbody > tr")
                },
                "last_matchs": [{
                    "team": match.select_one(".team-name").getText(strip=True),
                    "score": match.select_one(".recent-score").getText(strip=True),
                    "type": match.select_one(".match-type").getText(strip=True)
                } for match in hth.select(".analytics-last-matches > a")]
            } for hth in soup.select(".analytics-head-to-head-container")],
            "past_3_month": [{
                "team": p3m.select_one(".team-name").getText(strip=True),
                "match_map_count": p3m.select_one(".match-map-count").getText(strip=True),
                "matches": [{
                    "score": match.select_one("td:not(.best-bet):not(.handicap-data)").getText(strip=True),
                    "handicap": match.select_one(".handicap-data").getText(strip=True)
                } for match in p3m.select("tbody > tr")]
            } for p3m in soup.select(".analytics-handicap-wrapper > .col-6")],
            "map_handicap": [{
                "overall_data": {
                    "avg_rounds_lost_in_wins": 
                        handicap.select_one(
                            ".analytics-handicap-map-data-overall-container > :nth-child(1) > :nth-child(1)"
                        ).getText(strip=True),
                    "avg_rounds_won_in_losses": 
                        handicap.select_one(
                            ".analytics-handicap-map-data-overall-container > :nth-child(2) > :nth-child(1)"
                        ).getText(strip=True)
                },
                "individual_maps": [{
                    "map": item.select_one(".mapname").getText(strip=True),
                    "avg_rounds_lost_in_wins": item.select_one(
                        ":nth-child(2):not(.mapname)"
                    ).getText(strip=True),
                    "avg_rounds_won_in_losses": item.select_one(
                        ":nth-child(3):not(.mapname)"
                    ).getText(strip=True)
                } for item in handicap.select("tbody > tr")]
            } for handicap in soup.select(".analytics-handicap-map-wrapper > .col-6")],
            "map_stats": [{
                "map": item.select_one(
                    "td[rowspan]"
                ).getText(strip=True) if item.select_one("td[rowspan]") else None,
                "team": item.select_one(
                    ".maps-team-name"
                ).getText(strip=True),
                "first_pick": item.select_one(
                    ".analytics-map-stats-pick-percentage"
                ).getText(strip=True),
                "first_ban": item.select_one(
                    ".analytics-map-stats-ban-percentage"
                ).getText(strip=True),
                "win": item.select_one(
                    ".analytics-map-stats-win-percentage"
                ).getText(strip=True),
                "played": item.select_one(
                    ".analytics-map-stats-played"
                ).getText(strip=True),
                "comment": item.select_one(
                    ".analytics-map-stats-comment"
                ).getText(strip=True)
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

        team_stats = {}
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
                item.select_one(".small-label-below").getText(strip=True):
                    item.select_one(".large-strong").getText(strip=True)
                for item in soup.select(".col.standard-box")
            }
        }

    async def _parse_matches(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        return {
            "matches": [{
                "date": item.select_one(".time").getText(strip=True),
                "event": item.select_one(".gtSmartphone-only").getText(strip=True),
                "opponent": item.select_one(".flag.flag").getText(strip=True),
                "map": item.select_one(".statsMapPlayed").getText(strip=True),
                "result": item.select_one(".statsDetail").getText(strip=True),
                "W/L": item.select_one(".text-center:not(.gtSmartphone-only)").getText(strip=True)
            } for item in soup.select(".stats-table > tbody > tr")]
        }

    async def _parse_maps(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        return {
            "maps": [{
                "map": item.select_one(".map-pool").getText(strip=True) if item.select_one(".map-pool") else None,
                "stats": {
                    row.select_one(".strong").getText(strip=True):
                        row.select_one(":not(.strong)").getText(strip=True)
                    for row in item.select(".stats-row")
                } 
            } for item in soup.select(".two-grid:not(.win-defeat-container) > .col")]
        }

    async def _parse_players(self, soup: BeautifulSoup) -> Dict[str, Any]:
        return {
            "overview": [{
                "nickname": item.select_one(":nth-child(1)").getText(strip=True),
                "maps": item.select_one(".statsDetail").getText(strip=True),
                "rounds": item.select_one(":nth-child(3)").getText(strip=True),
                "k-d diff": item.select_one(":nth-child(4)").getText(strip=True),
                "k/d": item.select_one(":nth-child(5)").getText(strip=True),
                "rating": item.select_one(":nth-child(6)").getText(strip=True)
            } for item in soup.select(".stats-table > tbody > tr")]
        }

    async def _parse_flashes(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        return {
            "flashes": [{
                "nickname": item.select_one(":nth-child(1)").getText(strip=True),
                "maps": item.select_one(".mapsCol").getText(strip=True),
                "rounds": item.select_one(":nth-child(3)").getText(strip=True),
                "thrown": item.select_one(":nth-child(4)").getText(strip=True),
                "blinder": item.select_one(":nth-child(5)").getText(strip=True),
                "opp_flashed": item.select_one(":nth-child(6)").getText(strip=True),
                "diff": item.select_one(":nth-child(7)").getText(strip=True),
                "fa": item.select_one(":nth-child(8)").getText(strip=True),
                "success": item.select_one(":nth-child(9)").getText(strip=True)
            } for item in soup.select(".stats-table > tbody > tr")]
        }

    async def _parse_opening_kills(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        return {
            "opening_kills": [{
                "nickname": item.select_one(":nth-child(1)").getText(strip=True),
                "maps": item.select_one(".mapsCol").getText(strip=True),
                "rounds": item.select_one(":nth-child(3)").getText(strip=True),
                "kpr": item.select_one(":nth-child(4)").getText(strip=True),
                "dpr": item.select_one(":nth-child(5)").getText(strip=True),
                "attempts": item.select_one(":nth-child(6)").getText(strip=True),
                "success": item.select_one(":nth-child(7)").getText(strip=True),
                "rating": item.select_one(":nth-child(8)").getText(strip=True)
            } for item in soup.select(".stats-table > tbody > tr")]
        }
