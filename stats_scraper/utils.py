import os
import yaml
import json

from typing import Dict, Any
from stats_scraper.logger import logger
from stats_scraper.paths import CONFIG_PATH, OUT_DIR


def load_config() -> dict:
    with open(CONFIG_PATH) as file:
        return yaml.load(file, Loader=yaml.FullLoader)


def save_data(match_name: str, filename: str, json_data: dict) -> None:
    filename = f"{filename}.json"
    filepath = OUT_DIR / match_name / filename
    
    os.makedirs(OUT_DIR / match_name, exist_ok=True)
    
    with open(filepath, "w", encoding="utf-8") as file:
        json.dump(json_data, file, ensure_ascii=False, indent=4)
    logger.info(f"Файл сохранен {match_name}/{filename}")
    

def save_data_to_txt(data: Dict[str, Any], filename: str) -> str:
    filename = f"{filename}.txt"
    filepath = OUT_DIR / filename
    
    output_text = f"Название турнира + формат: {data['match_name'].rsplit()[0]}\n\n"
    output_text += "Lineups:\n\n"
    
    lineup = data['match_pre_data']['lineups'][0]
    output_text += f"Первая команда: {lineup['team']} #{lineup['world_rank']}\n"
    for player in lineup['players']:
        output_text += f"- {player['nickname']}\n"
    
    output_text += "\nСтатистика игроков:\n"
    player_stats = data['match_player_stats'][:5]
    for stats in player_stats:
        for key, value in stats.items():
            if isinstance(value, dict):
                for key, value in value.items():
                    output_text += f"  - {key}: {value}\n"
            else:
                output_text += f"- {key}: {value}\n"
        output_text += "\n"
    
    lineup = data['match_pre_data']['lineups'][1]
    output_text += f"Вторая команда: {lineup['team']} #{lineup['world_rank']}\n"
    for player in lineup['players']:
        output_text += f"- {player['nickname']}\n"
    
    output_text += "\nСтатистика игроков:\n"
    player_stats = data['match_player_stats'][5:]
    for stats in player_stats:
        for key, value in stats.items():
            if isinstance(value, dict):
                for key, value in value.items():
                    output_text += f"  - {key}: {value}\n"
            else:
                output_text += f"- {key}: {value}\n"
        output_text += "\n"
    
    output_text += "\nMap stats:\n"
    
    map_stats = data['match_pre_data']['match_stats']
    for map_stat in map_stats:
        for key, value in map_stat.items():
            if isinstance(value, dict):
                for key, value in value.items():
                    output_text += f"  - {key}: {' / '.join(value)}\n"
            else:
                output_text += f"- Карта: {value}\n"
        output_text += "\n"
    
    output_text += "\nPast 3 month:\n"
    output_text += f"Первая команда: {data['match_pre_data']['past_3_month'][0]['team']}\n"
    
    past_3_month = data['match_pre_data']['past_3_month'][0]['matches']
    for item in past_3_month:
        output_text += f"- {' / '.join(item.values())}\n"
        
    output_text += f"\nВторая команда: {data['match_pre_data']['past_3_month'][1]['team']}\n"
    
    past_3_month = data['match_pre_data']['past_3_month'][1]['matches']
    for item in past_3_month:
        output_text += f"- {' / '.join(item.values())}\n"
    
    output_text += "\n\nHead to head:\n"
    
    head_to_head = data['match_pre_data']['head_to_head']
    output_text += f"{' / '.join(f'{key}: {value}' for key, value in head_to_head['stats'].items())}\n"
    
    listing = head_to_head['listing']
    output_text += "\n" if not listing else ""
    
    for item in listing:
        for key, value in item.items():
            output_text += f"- {key}: {value}\n"
        output_text += "\n"
    
    output_text += "\nAnalytics center.\n"
    
    output_text += "\nAnalytics summary:\n"
    analytics = data['match_analytics']
    
    analytics_summary = analytics['analytics_summary']
    for item in analytics_summary:
        output_text += f"- Команда: {item['team']}\n"
        for analytic in item['analytic']:
            output_text += f"  - {analytic}\n"
        output_text += f"\n"
    
    output_text += "Head to head:\n"
    
    analytic_head_to_head = analytics['head_to_head']
    for item in analytic_head_to_head:
        output_text += f"- Команда: {item['team']}\n"
        for players in item['players'].items():
            output_text += f"  - {players[0]}: {' / '.join(players[1].values())}\n"
        output_text += "\n"
    
    output_text += "Past 3 months:\n"
    
    analytic_past_3_month = analytics['past_3_month']
    for item in analytic_past_3_month:
        output_text += f"- Команда: {item['team']}\n"
        output_text += f"- Кол-во матчей: {item['match_map_count']}\n"
        for matches in item['matches']:
            for key, value in matches.items():
                output_text += f"  - {key}: {value}\n"
            output_text += "\n"
    
    output_text += "Map handicap:\n"
    
    map_handicap = analytics['map_handicap']
    for item in map_handicap:
        output_text += f"- Overall data:\n"
        for key, value in item['overall_data'].items():
            output_text += f"  - {key}: {value}\n"
        
        output_text += "\n"
        
        for individual_map in item['individual_maps']:
            for key, value in individual_map.items():
                output_text += f"  - {key}: {value}\n"
            output_text += "\n"
        
    output_text += "Map stats:\n"
    
    map_stats = analytics['map_stats']
    for item in map_stats:
        for key, value in item.items():
            output_text += f"- {key}: {value}\n"
        output_text += "\n"
    
    output_text += "\nСтатистика команд:\n"
    team_stats = data['match_teams']
    
    output_text += f"\nСтатистика команды: {team_stats[0]['team']}\n"
    output_text += "Overview:\n"
    
    team1_overview = team_stats[0]['overview']
    for item in team1_overview:
        values = item.values()
        output_text += f"- {' / '.join(values)}\n"
        
    output_text += "\nMatches:\n"
    
    team1_matches = team_stats[0]['matches']
    for item in team1_matches:
        values = item.values()
        output_text += f"- {' / '.join(values)}\n"
    
    output_text += "\nMaps:\n"
    
    team1_maps = team_stats[0]['maps']
    for item in team1_maps:
        output_text += f"- Карта: {item['map']}\n"
        for key, value in item['stats'].items():
            output_text += f"  - {key}: {value}\n"
        output_text += "\n"
        
    output_text += "Flashes:\n"
    
    team1_flashes = team_stats[0]['flashes']
    for item in team1_flashes:
        values = item.values()
        output_text += f"- {' / '.join(values)}\n"
    
    output_text += "\nOpening Kills:\n"
    
    team1_opening_kills = team_stats[0]['opening_kills']
    for item in team1_opening_kills:
        values = item.values()
        output_text += f"- {' / '.join(values)}\n"

    output_text += f"\nСтатистика команды: {team_stats[1]['team']}\n"
    output_text += "Overview:\n"
    
    team1_overview = team_stats[1]['overview']
    for item in team1_overview:
        values = item.values()
        output_text += f"- {' / '.join(values)}\n"
        
    output_text += "\nMatches:\n"
    
    team1_matches = team_stats[1]['matches']
    for item in team1_matches:
        values = item.values()
        output_text += f"- {' / '.join(values)}\n"
    
    output_text += "\nMaps:\n"
    
    team1_maps = team_stats[1]['maps']
    for item in team1_maps:
        output_text += f"- Карта: {item['map']}\n"
        for key, value in item['stats'].items():
            output_text += f"  - {key}: {value}\n"
        output_text += "\n"
        
    output_text += "Flashes:\n"
    
    team1_flashes = team_stats[1]['flashes']
    for item in team1_flashes:
        values = item.values()
        output_text += f"- {' / '.join(values)}\n"
    
    output_text += "\nOpening Kills:\n"
    
    team1_opening_kills = team_stats[1]['opening_kills']
    for item in team1_opening_kills:
        values = item.values()
        output_text += f"- {' / '.join(values)}\n"
    
    with open(filepath, "w", encoding="utf-8") as file:
        file.write(output_text)
    logger.info(f"Файл сохранен: {filename}")