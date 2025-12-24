"""
Match Data Saver - FotMob verisini ilgili tüm tablolara kaydeder
fotmob_to_db.py'nin Python FastAPI versiyonu
"""
import json
from typing import Optional, Dict, Any
from datetime import datetime
from app.services.db import execute_query, execute_insert


def parse_match_round(round_value) -> int:
    """Round değerini integer'a çevir (turnuva formatları dahil)"""
    if round_value is None:
        return 0
    
    if isinstance(round_value, int):
        return round_value
    
    round_str = str(round_value).strip()
    if not round_str:
        return 0
    
    # Tournament format: "1/16", "1/4", "1/2"
    if '/' in round_str:
        try:
            parts = round_str.split('/')
            if len(parts) == 2 and parts[0] == '1':
                return int(parts[1])
        except:
            pass
        return 0
    
    # Final
    if round_str.lower() in ('final', 'finale'):
        return 1
    if 'semi' in round_str.lower():
        return 2
    if 'quarter' in round_str.lower():
        return 4
    
    try:
        return int(round_str)
    except ValueError:
        import re
        numbers = re.findall(r'\d+', round_str)
        if numbers:
            return int(numbers[0])
        return 0


def save_league(match_data: dict) -> int:
    """Ligi kaydet veya mevcut ID'yi döndür"""
    general = match_data.get('general', {})
    league_id_value = general.get('parentLeagueId') or general.get('leagueId')
    
    check_query = f"SELECT id FROM public.leagues WHERE fotmob_league_id = {league_id_value}"
    existing = execute_query(check_query)
    
    if existing:
        return existing[0]['id']
    
    insert_query = """
        INSERT INTO public.leagues (fotmob_league_id, name, country, country_code, season)
        VALUES (:fotmob_league_id, :name, :country, :country_code, :season)
        RETURNING id
    """
    result = execute_insert(insert_query, {
        "fotmob_league_id": league_id_value,
        "name": general.get('leagueName', 'Unknown'),
        "country": general.get('countryCode', ''),
        "country_code": general.get('countryCode', ''),
        "season": '2024/2025'
    })
    
    return result['id'] if result else None


def save_team(team_data: dict, league_id: int) -> int:
    """Takımı kaydet veya mevcut ID'yi döndür"""
    check_query = f"SELECT id FROM public.teams WHERE fotmob_team_id = {team_data['id']}"
    existing = execute_query(check_query)
    
    if existing:
        return existing[0]['id']
    
    insert_query = """
        INSERT INTO public.teams (fotmob_team_id, name, short_name, league_id)
        VALUES (:fotmob_team_id, :name, :short_name, :league_id)
        RETURNING id
    """
    result = execute_insert(insert_query, {
        "fotmob_team_id": team_data['id'],
        "name": team_data.get('name', 'Unknown'),
        "short_name": team_data.get('shortName', team_data.get('name', '')[:3]),
        "league_id": league_id
    })
    
    return result['id'] if result else None


def save_match(match_data: dict, home_team_id: int, away_team_id: int, league_id: int) -> int:
    """Maçı kaydet veya güncelle"""
    general = match_data.get('general', {})
    header = match_data.get('header', {})
    
    fotmob_match_id = int(general.get('matchId', 0))
    
    check_query = f"SELECT id FROM public.matches WHERE fotmob_match_id = {fotmob_match_id}"
    existing = execute_query(check_query)
    
    teams = header.get('teams', [{}, {}])
    home_score = teams[0].get('score', 0) if len(teams) > 0 else 0
    away_score = teams[1].get('score', 0) if len(teams) > 1 else 0
    
    raw_json = json.dumps(match_data)
    
    if existing:
        match_id = existing[0]['id']
        update_query = """
            UPDATE public.matches SET
                home_score = :home_score,
                away_score = :away_score,
                finished = :finished,
                raw_match_details = :raw_match_details,
                updated_at = NOW()
            WHERE id = :id
        """
        execute_insert(update_query, {
            "home_score": home_score,
            "away_score": away_score,
            "finished": general.get('finished', False),
            "raw_match_details": raw_json,
            "id": match_id
        })
        return match_id
    
    # Match date parsing
    match_date_str = general.get('matchTimeUTCDate', '')
    match_date = None
    if match_date_str:
        try:
            match_date = datetime.fromisoformat(match_date_str.replace('Z', '+00:00'))
        except:
            pass
    
    insert_query = """
        INSERT INTO public.matches (
            fotmob_match_id, home_team_id, away_team_id, league_id,
            round, match_date, home_score, away_score, finished,
            raw_match_details
        )
        VALUES (:fotmob_match_id, :home_team_id, :away_team_id, :league_id,
                :round, :match_date, :home_score, :away_score, :finished,
                :raw_match_details)
        RETURNING id
    """
    result = execute_insert(insert_query, {
        "fotmob_match_id": fotmob_match_id,
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "league_id": league_id,
        "round": parse_match_round(general.get('matchRound', 0)),
        "match_date": match_date,
        "home_score": home_score,
        "away_score": away_score,
        "finished": general.get('finished', False),
        "raw_match_details": raw_json
    })
    
    return result['id'] if result else None


def save_match_stats(match_id: int, match_data: dict):
    """Maç istatistiklerini kaydet"""
    content = match_data.get('content', {})
    stats_data = content.get('stats')
    
    if not stats_data:
        return
    
    # Default values
    home_xg = away_xg = 0.0
    home_shots = away_shots = 0
    home_shots_on_target = away_shots_on_target = 0
    home_possession = away_possession = 50.0
    home_corners = away_corners = 0
    home_fouls = away_fouls = 0
    home_yellow = away_yellow = 0
    home_red = away_red = 0
    
    periods = stats_data.get('Periods', {})
    all_stats = periods.get('All', {}).get('stats', [])
    
    for stat_group in all_stats:
        for stat in stat_group.get('stats', []):
            key = stat.get('key', '')
            stats_arr = stat.get('stats', [None, None])
            
            home_val = stats_arr[0] if len(stats_arr) > 0 else None
            away_val = stats_arr[1] if len(stats_arr) > 1 else None
            
            if key == 'expected_goals':
                home_xg = float(home_val) if home_val else 0.0
                away_xg = float(away_val) if away_val else 0.0
            elif key == 'total_shots':
                home_shots = int(home_val) if home_val else 0
                away_shots = int(away_val) if away_val else 0
            elif key in ('ShotsOnTarget', 'shots_on_target'):
                home_shots_on_target = int(home_val) if home_val else 0
                away_shots_on_target = int(away_val) if away_val else 0
            elif key in ('BallPossesion', 'BallPossession', 'Ball possession', 'possession_percentage'):
                home_possession = float(str(home_val).replace('%', '')) if home_val else 50.0
                away_possession = float(str(away_val).replace('%', '')) if away_val else 50.0
            elif key == 'corners':
                home_corners = int(home_val) if home_val else 0
                away_corners = int(away_val) if away_val else 0
            elif key == 'fouls':
                home_fouls = int(home_val) if home_val else 0
                away_fouls = int(away_val) if away_val else 0
            elif key == 'yellow_cards':
                home_yellow = int(home_val) if home_val else 0
                away_yellow = int(away_val) if away_val else 0
            elif key == 'red_cards':
                home_red = int(home_val) if home_val else 0
                away_red = int(away_val) if away_val else 0
    
    query = """
        INSERT INTO public.match_stats (
            match_id, home_xg, away_xg, home_shots, away_shots,
            home_shots_on_target, away_shots_on_target,
            home_possession, away_possession,
            home_corners, away_corners, home_fouls, away_fouls,
            home_yellow_cards, away_yellow_cards,
            home_red_cards, away_red_cards
        )
        VALUES (:match_id, :home_xg, :away_xg, :home_shots, :away_shots,
                :home_shots_on_target, :away_shots_on_target,
                :home_possession, :away_possession,
                :home_corners, :away_corners, :home_fouls, :away_fouls,
                :home_yellow, :away_yellow, :home_red, :away_red)
        ON CONFLICT (match_id) DO UPDATE SET
            home_xg = EXCLUDED.home_xg,
            away_xg = EXCLUDED.away_xg,
            home_shots = EXCLUDED.home_shots,
            away_shots = EXCLUDED.away_shots,
            home_shots_on_target = EXCLUDED.home_shots_on_target,
            away_shots_on_target = EXCLUDED.away_shots_on_target,
            home_possession = EXCLUDED.home_possession,
            away_possession = EXCLUDED.away_possession
    """
    execute_insert(query, {
        "match_id": match_id,
        "home_xg": home_xg, "away_xg": away_xg,
        "home_shots": home_shots, "away_shots": away_shots,
        "home_shots_on_target": home_shots_on_target, "away_shots_on_target": away_shots_on_target,
        "home_possession": home_possession, "away_possession": away_possession,
        "home_corners": home_corners, "away_corners": away_corners,
        "home_fouls": home_fouls, "away_fouls": away_fouls,
        "home_yellow": home_yellow, "away_yellow": away_yellow,
        "home_red": home_red, "away_red": away_red
    })


def save_match_context(match_id: int, match_data: dict):
    """Stadyum, hakem, hava durumu bilgilerini kaydet"""
    check_query = f"SELECT match_id FROM public.match_context WHERE match_id = {match_id}"
    if execute_query(check_query):
        return
    
    content = match_data.get('content', {})
    match_facts = content.get('matchFacts', {})
    
    if not match_facts:
        return
    
    info_box = match_facts.get('infoBox', {}) or {}
    
    # Stadium
    stadium_info = info_box.get('Stadium') or {}
    stadium_name = stadium_info.get('name', '') if isinstance(stadium_info, dict) else ''
    stadium_lat = stadium_info.get('lat') if isinstance(stadium_info, dict) else None
    stadium_lon = stadium_info.get('long') if isinstance(stadium_info, dict) else None
    stadium_capacity = stadium_info.get('capacity') if isinstance(stadium_info, dict) else None
    
    # Referee
    referee_info = info_box.get('Referee') or {}
    referee_name = referee_info.get('text', '') if isinstance(referee_info, dict) else ''
    referee_country = referee_info.get('country', '') if isinstance(referee_info, dict) else ''
    
    # Attendance
    attendance = info_box.get('Attendance')
    if attendance:
        try:
            attendance = int(str(attendance).replace(',', '').replace('.', ''))
        except:
            attendance = None
    
    # Weather
    weather = content.get('weather', {})
    weather_condition = weather.get('condition', '')
    weather_temp = weather.get('temp')
    
    query = """
        INSERT INTO public.match_context (
            match_id, stadium, stadium_lat, stadium_lon, stadium_capacity,
            referee, referee_country, attendance,
            weather_condition, weather_temp
        )
        VALUES (:match_id, :stadium, :stadium_lat, :stadium_lon, :stadium_capacity,
                :referee, :referee_country, :attendance,
                :weather_condition, :weather_temp)
    """
    execute_insert(query, {
        "match_id": match_id,
        "stadium": stadium_name, "stadium_lat": stadium_lat,
        "stadium_lon": stadium_lon, "stadium_capacity": stadium_capacity,
        "referee": referee_name, "referee_country": referee_country,
        "attendance": attendance,
        "weather_condition": weather_condition, "weather_temp": weather_temp
    })


def save_match_formations(match_id: int, match_data: dict):
    """Diziliş bilgilerini kaydet"""
    check_query = f"SELECT match_id FROM public.match_formations WHERE match_id = {match_id}"
    if execute_query(check_query):
        return
    
    content = match_data.get('content', {})
    lineup = content.get('lineup', {})
    
    if not lineup:
        return
    
    home_team = lineup.get('homeTeam', {})
    away_team = lineup.get('awayTeam', {})
    
    home_formation = home_team.get('formation', '') if home_team else ''
    away_formation = away_team.get('formation', '') if away_team else ''
    
    if home_formation or away_formation:
        query = """
            INSERT INTO public.match_formations (match_id, home_formation, away_formation)
            VALUES (:match_id, :home_formation, :away_formation)
        """
        execute_insert(query, {
            "match_id": match_id,
            "home_formation": home_formation,
            "away_formation": away_formation
        })


def save_match_lineups(match_id: int, match_data: dict, home_team_id: int, away_team_id: int):
    """Kadro bilgilerini kaydet"""
    check_query = f"SELECT COUNT(*) as cnt FROM public.match_lineups WHERE match_id = {match_id}"
    existing = execute_query(check_query)
    if existing and existing[0]['cnt'] > 0:
        return
    
    content = match_data.get('content', {})
    lineup = content.get('lineup', {})
    
    if not lineup:
        return
    
    for team_key, team_id in [('homeTeam', home_team_id), ('awayTeam', away_team_id)]:
        team_data = lineup.get(team_key, {})
        
        if not team_data:
            continue
        
        # Starters
        for player in team_data.get('starters', []):
            if not player:
                continue
            
            player_name = player.get('name', '')
            market_value = player.get('marketValue')
            if market_value:
                market_value = market_value / 1000000.0
            
            performance = player.get('performance', {})
            rating = performance.get('seasonRating') if isinstance(performance, dict) else None
            
            query = """
                INSERT INTO public.match_lineups (
                    match_id, team_id, player_name, shirt_number, position,
                    is_starter, market_value, age, rating
                )
                VALUES (:match_id, :team_id, :player_name, :shirt_number, :position,
                        :is_starter, :market_value, :age, :rating)
            """
            execute_insert(query, {
                "match_id": match_id,
                "team_id": team_id,
                "player_name": player_name,
                "shirt_number": player.get('shirtNumber'),
                "position": str(player.get('positionId', '')),
                "is_starter": True,
                "market_value": market_value,
                "age": player.get('age'),
                "rating": rating
            })
        
        # Substitutes
        for player in team_data.get('subs', []):
            if not player:
                continue
            
            player_name = player.get('name', '')
            market_value = player.get('marketValue')
            if market_value:
                market_value = market_value / 1000000.0
            
            performance = player.get('performance', {})
            rating = performance.get('seasonRating') if isinstance(performance, dict) else None
            
            query = """
                INSERT INTO public.match_lineups (
                    match_id, team_id, player_name, shirt_number, position,
                    is_starter, market_value, age, rating
                )
                VALUES (:match_id, :team_id, :player_name, :shirt_number, :position,
                        :is_starter, :market_value, :age, :rating)
            """
            execute_insert(query, {
                "match_id": match_id,
                "team_id": team_id,
                "player_name": player_name,
                "shirt_number": player.get('shirtNumber'),
                "position": str(player.get('positionId', '')),
                "is_starter": False,
                "market_value": market_value,
                "age": player.get('age'),
                "rating": rating
            })


def save_match_events(match_id: int, match_data: dict, home_team_id: int, away_team_id: int):
    """Maç olaylarını kaydet (gol, kart, değişiklik)"""
    check_query = f"SELECT COUNT(*) as cnt FROM public.match_events WHERE match_id = {match_id}"
    existing = execute_query(check_query)
    if existing and existing[0]['cnt'] > 0:
        return
    
    content = match_data.get('content', {})
    match_facts = content.get('matchFacts', {})
    
    if not match_facts:
        return
    
    events_data = match_facts.get('events', {})
    events_list = events_data.get('events', [])
    
    if not events_list:
        return
    
    for event in events_list:
        if not event:
            continue
        
        event_type_raw = event.get('type', '')
        
        if event_type_raw == 'Goal':
            event_type = 'GOAL'
        elif event_type_raw == 'Card':
            card_type = event.get('card', 'Yellow')
            event_type = 'RED_CARD' if card_type == 'Red' else 'YELLOW_CARD'
        elif event_type_raw == 'Substitution':
            event_type = 'SUBSTITUTION'
        elif event_type_raw == 'AddedTime':
            continue
        else:
            event_type = event_type_raw.upper()
        
        is_home = event.get('isHome', True)
        team_id = home_team_id if is_home else away_team_id
        
        player_name = event.get('fullName', '') or event.get('nameStr', '')
        minute = event.get('time')
        added_time = event.get('overloadTime')
        
        assisted_by = None
        if event_type == 'GOAL':
            assisted_by = event.get('assistStr')
        
        player_in = None
        player_out = None
        if event_type == 'SUBSTITUTION':
            swap = event.get('swap', [])
            if len(swap) >= 2:
                player_out = swap[0].get('name', '') if isinstance(swap[0], dict) else ''
                player_in = swap[1].get('name', '') if isinstance(swap[1], dict) else ''
                player_name = player_out
        
        is_own_goal = bool(event.get('ownGoal'))
        is_penalty = 'Penalty' in str(event.get('goalDescription', '')) or 'penalty' in str(event.get('suffixKey', ''))
        
        query = """
            INSERT INTO public.match_events (
                match_id, team_id, event_type, minute, added_time,
                player_name, assisted_by, player_in, player_out,
                is_own_goal, is_penalty, event_data
            )
            VALUES (:match_id, :team_id, :event_type, :minute, :added_time,
                    :player_name, :assisted_by, :player_in, :player_out,
                    :is_own_goal, :is_penalty, :event_data)
        """
        execute_insert(query, {
            "match_id": match_id,
            "team_id": team_id,
            "event_type": event_type,
            "minute": minute,
            "added_time": added_time,
            "player_name": player_name,
            "assisted_by": assisted_by,
            "player_in": player_in,
            "player_out": player_out,
            "is_own_goal": is_own_goal,
            "is_penalty": is_penalty,
            "event_data": json.dumps(event)
        })


def save_player_availability(match_id: int, match_data: dict, home_team_id: int, away_team_id: int):
    """Sakatlık/ceza bilgilerini kaydet"""
    check_query = f"SELECT COUNT(*) as cnt FROM public.player_availability WHERE match_id = {match_id}"
    existing = execute_query(check_query)
    if existing and existing[0]['cnt'] > 0:
        return
    
    content = match_data.get('content', {})
    lineup = content.get('lineup', {})
    
    if not lineup:
        return
    
    for team_key, team_id in [('homeTeam', home_team_id), ('awayTeam', away_team_id)]:
        team_data = lineup.get(team_key, {})
        
        if not team_data:
            continue
        
        unavailable = team_data.get('unavailable', [])
        for player in unavailable:
            if not player:
                continue
            
            player_name = player.get('name', '')
            reason = player.get('injuryStatus', 'Unknown')
            if not reason or reason == 'Unknown':
                reason = player.get('reason', 'Unknown')
            
            query = """
                INSERT INTO public.player_availability (
                    match_id, team_id, player_name, status, reason
                )
                VALUES (:match_id, :team_id, :player_name, :status, :reason)
            """
            execute_insert(query, {
                "match_id": match_id,
                "team_id": team_id,
                "player_name": player_name,
                "status": 'UNAVAILABLE',
                "reason": reason
            })


def save_h2h_stats(match_data: dict, home_team_id: int, away_team_id: int):
    """H2H istatistiklerini kaydet"""
    check_query = f"""
        SELECT id FROM public.h2h_stats 
        WHERE (team1_id = {home_team_id} AND team2_id = {away_team_id}) 
           OR (team1_id = {away_team_id} AND team2_id = {home_team_id})
    """
    if execute_query(check_query):
        return
    
    content = match_data.get('content', {})
    h2h = content.get('h2h', {})
    
    if not h2h:
        return
    
    summary = h2h.get('summary', [])
    
    home_wins = summary[0] if len(summary) > 0 else 0
    draws = summary[1] if len(summary) > 1 else 0
    away_wins = summary[2] if len(summary) > 2 else 0
    total_matches = home_wins + draws + away_wins
    
    if total_matches == 0:
        return
    
    matches = h2h.get('matches', [])
    home_goals_total = 0
    away_goals_total = 0
    
    for match in matches[:10]:
        home_score = match.get('homeScore', 0)
        away_score = match.get('awayScore', 0)
        home_goals_total += home_score
        away_goals_total += away_score
    
    match_count = min(len(matches), 10)
    avg_home_goals = home_goals_total / match_count if match_count > 0 else 0
    avg_away_goals = away_goals_total / match_count if match_count > 0 else 0
    
    query = """
        INSERT INTO public.h2h_stats (
            team1_id, team2_id, total_matches, team1_wins, team2_wins, draws,
            avg_goals_team1, avg_goals_team2
        )
        VALUES (:team1_id, :team2_id, :total_matches, :team1_wins, :team2_wins, :draws,
                :avg_goals_team1, :avg_goals_team2)
    """
    execute_insert(query, {
        "team1_id": home_team_id,
        "team2_id": away_team_id,
        "total_matches": total_matches,
        "team1_wins": home_wins,
        "team2_wins": away_wins,
        "draws": draws,
        "avg_goals_team1": round(avg_home_goals, 2),
        "avg_goals_team2": round(avg_away_goals, 2)
    })


def save_match_advanced_stats(match_id: int, match_data: dict):
    """Detaylı maç istatistiklerini kaydet"""
    check_query = f"SELECT match_id FROM public.match_advanced_stats WHERE match_id = {match_id}"
    if execute_query(check_query):
        return
    
    content = match_data.get('content', {})
    stats_data = content.get('stats')
    
    if not stats_data:
        return
    
    periods = stats_data.get('Periods', {})
    all_stats = periods.get('All', {}).get('stats', [])
    
    stats_dict = {}
    
    def parse_value(val):
        if val is None:
            return None
        if isinstance(val, (int, float)):
            return val
        val_str = str(val)
        if '(' in val_str:
            num_part = val_str.split('(')[0].strip()
            try:
                return int(num_part)
            except:
                return None
        if '%' in val_str:
            try:
                return float(val_str.replace('%', ''))
            except:
                return None
        try:
            return float(val_str) if '.' in val_str else int(val_str)
        except:
            return None
    
    def parse_pct(val):
        if val is None:
            return None
        val_str = str(val)
        if '(' in val_str and '%' in val_str:
            try:
                pct_part = val_str.split('(')[1].replace('%', '').replace(')', '')
                return float(pct_part)
            except:
                return None
        if '%' in val_str:
            try:
                return float(val_str.replace('%', ''))
            except:
                return None
        return None
    
    for stat_group in all_stats:
        for stat in stat_group.get('stats', []):
            key = stat.get('key', '')
            stats_arr = stat.get('stats', [None, None])
            
            home_val = stats_arr[0] if len(stats_arr) > 0 else None
            away_val = stats_arr[1] if len(stats_arr) > 1 else None
            
            # Map keys to columns
            if key == 'expected_goals_open_play':
                stats_dict['home_open_play_xg'] = parse_value(home_val)
                stats_dict['away_open_play_xg'] = parse_value(away_val)
            elif key == 'expected_goals_set_play':
                stats_dict['home_set_piece_xg'] = parse_value(home_val)
                stats_dict['away_set_piece_xg'] = parse_value(away_val)
            elif key == 'expected_goals_on_target':
                stats_dict['home_xgot'] = parse_value(home_val)
                stats_dict['away_xgot'] = parse_value(away_val)
            elif key == 'blocked_shots':
                stats_dict['home_shots_blocked'] = parse_value(home_val)
                stats_dict['away_shots_blocked'] = parse_value(away_val)
            elif key == 'ShotsOffTarget':
                stats_dict['home_shots_off_target'] = parse_value(home_val)
                stats_dict['away_shots_off_target'] = parse_value(away_val)
            elif key == 'shots_inside_box':
                stats_dict['home_shots_inside_box'] = parse_value(home_val)
                stats_dict['away_shots_inside_box'] = parse_value(away_val)
            elif key == 'shots_outside_box':
                stats_dict['home_shots_outside_box'] = parse_value(home_val)
                stats_dict['away_shots_outside_box'] = parse_value(away_val)
            elif key == 'passes':
                stats_dict['home_total_passes'] = parse_value(home_val)
                stats_dict['away_total_passes'] = parse_value(away_val)
            elif key == 'accurate_passes':
                stats_dict['home_pass_accuracy'] = parse_pct(home_val)
                stats_dict['away_pass_accuracy'] = parse_pct(away_val)
            elif key == 'long_balls_accurate':
                stats_dict['home_long_passes'] = parse_value(home_val)
                stats_dict['away_long_passes'] = parse_value(away_val)
                stats_dict['home_long_pass_accuracy'] = parse_pct(home_val)
                stats_dict['away_long_pass_accuracy'] = parse_pct(away_val)
            elif key == 'accurate_crosses':
                stats_dict['home_crosses'] = parse_value(home_val)
                stats_dict['away_crosses'] = parse_value(away_val)
                stats_dict['home_cross_accuracy'] = parse_pct(home_val)
                stats_dict['away_cross_accuracy'] = parse_pct(away_val)
            elif key == 'own_half_passes':
                stats_dict['home_passes_own_half'] = parse_value(home_val)
                stats_dict['away_passes_own_half'] = parse_value(away_val)
            elif key == 'opposition_half_passes':
                stats_dict['home_passes_opp_half'] = parse_value(home_val)
                stats_dict['away_passes_opp_half'] = parse_value(away_val)
            elif key == 'touches_opp_box':
                stats_dict['home_touches_in_box'] = parse_value(home_val)
                stats_dict['away_touches_in_box'] = parse_value(away_val)
            elif key == 'matchstats.headers.tackles':
                stats_dict['home_tackles'] = parse_value(home_val)
                stats_dict['away_tackles'] = parse_value(away_val)
            elif key == 'interceptions':
                stats_dict['home_interceptions'] = parse_value(home_val)
                stats_dict['away_interceptions'] = parse_value(away_val)
            elif key == 'shot_blocks':
                stats_dict['home_blocks'] = parse_value(home_val)
                stats_dict['away_blocks'] = parse_value(away_val)
            elif key == 'clearances':
                stats_dict['home_clearances'] = parse_value(home_val)
                stats_dict['away_clearances'] = parse_value(away_val)
            elif key == 'keeper_saves':
                stats_dict['home_goalkeeper_saves'] = parse_value(home_val)
                stats_dict['away_goalkeeper_saves'] = parse_value(away_val)
            elif key == 'duel_won':
                stats_dict['home_duels_won'] = parse_value(home_val)
                stats_dict['away_duels_won'] = parse_value(away_val)
            elif key == 'ground_duels_won':
                stats_dict['home_duels_won_pct'] = parse_pct(home_val)
                stats_dict['away_duels_won_pct'] = parse_pct(away_val)
            elif key == 'aerials_won':
                stats_dict['home_aerial_duels_won'] = parse_value(home_val)
                stats_dict['away_aerial_duels_won'] = parse_value(away_val)
                stats_dict['home_aerial_duels_pct'] = parse_pct(home_val)
                stats_dict['away_aerial_duels_pct'] = parse_pct(away_val)
            elif key == 'dribbles_succeeded':
                stats_dict['home_dribbles_successful'] = parse_value(home_val)
                stats_dict['away_dribbles_successful'] = parse_value(away_val)
                stats_dict['home_dribbles_pct'] = parse_pct(home_val)
                stats_dict['away_dribbles_pct'] = parse_pct(away_val)
            elif key == 'Offsides':
                stats_dict['home_offsides'] = parse_value(home_val)
                stats_dict['away_offsides'] = parse_value(away_val)
    
    query = """
        INSERT INTO public.match_advanced_stats (
            match_id,
            home_open_play_xg, away_open_play_xg, home_set_piece_xg, away_set_piece_xg,
            home_xgot, away_xgot,
            home_shots_blocked, away_shots_blocked, home_shots_off_target, away_shots_off_target,
            home_shots_inside_box, away_shots_inside_box, home_shots_outside_box, away_shots_outside_box,
            home_total_passes, away_total_passes, home_pass_accuracy, away_pass_accuracy,
            home_long_passes, away_long_passes, home_long_pass_accuracy, away_long_pass_accuracy,
            home_crosses, away_crosses, home_cross_accuracy, away_cross_accuracy,
            home_passes_own_half, away_passes_own_half, home_passes_opp_half, away_passes_opp_half,
            home_touches_in_box, away_touches_in_box,
            home_tackles, away_tackles, home_interceptions, away_interceptions,
            home_blocks, away_blocks, home_clearances, away_clearances,
            home_goalkeeper_saves, away_goalkeeper_saves,
            home_duels_won, away_duels_won, home_duels_won_pct, away_duels_won_pct,
            home_aerial_duels_won, away_aerial_duels_won, home_aerial_duels_pct, away_aerial_duels_pct,
            home_dribbles_successful, away_dribbles_successful, home_dribbles_pct, away_dribbles_pct,
            home_offsides, away_offsides
        )
        VALUES (
            :match_id,
            :home_open_play_xg, :away_open_play_xg, :home_set_piece_xg, :away_set_piece_xg,
            :home_xgot, :away_xgot,
            :home_shots_blocked, :away_shots_blocked, :home_shots_off_target, :away_shots_off_target,
            :home_shots_inside_box, :away_shots_inside_box, :home_shots_outside_box, :away_shots_outside_box,
            :home_total_passes, :away_total_passes, :home_pass_accuracy, :away_pass_accuracy,
            :home_long_passes, :away_long_passes, :home_long_pass_accuracy, :away_long_pass_accuracy,
            :home_crosses, :away_crosses, :home_cross_accuracy, :away_cross_accuracy,
            :home_passes_own_half, :away_passes_own_half, :home_passes_opp_half, :away_passes_opp_half,
            :home_touches_in_box, :away_touches_in_box,
            :home_tackles, :away_tackles, :home_interceptions, :away_interceptions,
            :home_blocks, :away_blocks, :home_clearances, :away_clearances,
            :home_goalkeeper_saves, :away_goalkeeper_saves,
            :home_duels_won, :away_duels_won, :home_duels_won_pct, :away_duels_won_pct,
            :home_aerial_duels_won, :away_aerial_duels_won, :home_aerial_duels_pct, :away_aerial_duels_pct,
            :home_dribbles_successful, :away_dribbles_successful, :home_dribbles_pct, :away_dribbles_pct,
            :home_offsides, :away_offsides
        )
    """
    execute_insert(query, {
        "match_id": match_id,
        "home_open_play_xg": stats_dict.get('home_open_play_xg'),
        "away_open_play_xg": stats_dict.get('away_open_play_xg'),
        "home_set_piece_xg": stats_dict.get('home_set_piece_xg'),
        "away_set_piece_xg": stats_dict.get('away_set_piece_xg'),
        "home_xgot": stats_dict.get('home_xgot'),
        "away_xgot": stats_dict.get('away_xgot'),
        "home_shots_blocked": stats_dict.get('home_shots_blocked'),
        "away_shots_blocked": stats_dict.get('away_shots_blocked'),
        "home_shots_off_target": stats_dict.get('home_shots_off_target'),
        "away_shots_off_target": stats_dict.get('away_shots_off_target'),
        "home_shots_inside_box": stats_dict.get('home_shots_inside_box'),
        "away_shots_inside_box": stats_dict.get('away_shots_inside_box'),
        "home_shots_outside_box": stats_dict.get('home_shots_outside_box'),
        "away_shots_outside_box": stats_dict.get('away_shots_outside_box'),
        "home_total_passes": stats_dict.get('home_total_passes'),
        "away_total_passes": stats_dict.get('away_total_passes'),
        "home_pass_accuracy": stats_dict.get('home_pass_accuracy'),
        "away_pass_accuracy": stats_dict.get('away_pass_accuracy'),
        "home_long_passes": stats_dict.get('home_long_passes'),
        "away_long_passes": stats_dict.get('away_long_passes'),
        "home_long_pass_accuracy": stats_dict.get('home_long_pass_accuracy'),
        "away_long_pass_accuracy": stats_dict.get('away_long_pass_accuracy'),
        "home_crosses": stats_dict.get('home_crosses'),
        "away_crosses": stats_dict.get('away_crosses'),
        "home_cross_accuracy": stats_dict.get('home_cross_accuracy'),
        "away_cross_accuracy": stats_dict.get('away_cross_accuracy'),
        "home_passes_own_half": stats_dict.get('home_passes_own_half'),
        "away_passes_own_half": stats_dict.get('away_passes_own_half'),
        "home_passes_opp_half": stats_dict.get('home_passes_opp_half'),
        "away_passes_opp_half": stats_dict.get('away_passes_opp_half'),
        "home_touches_in_box": stats_dict.get('home_touches_in_box'),
        "away_touches_in_box": stats_dict.get('away_touches_in_box'),
        "home_tackles": stats_dict.get('home_tackles'),
        "away_tackles": stats_dict.get('away_tackles'),
        "home_interceptions": stats_dict.get('home_interceptions'),
        "away_interceptions": stats_dict.get('away_interceptions'),
        "home_blocks": stats_dict.get('home_blocks'),
        "away_blocks": stats_dict.get('away_blocks'),
        "home_clearances": stats_dict.get('home_clearances'),
        "away_clearances": stats_dict.get('away_clearances'),
        "home_goalkeeper_saves": stats_dict.get('home_goalkeeper_saves'),
        "away_goalkeeper_saves": stats_dict.get('away_goalkeeper_saves'),
        "home_duels_won": stats_dict.get('home_duels_won'),
        "away_duels_won": stats_dict.get('away_duels_won'),
        "home_duels_won_pct": stats_dict.get('home_duels_won_pct'),
        "away_duels_won_pct": stats_dict.get('away_duels_won_pct'),
        "home_aerial_duels_won": stats_dict.get('home_aerial_duels_won'),
        "away_aerial_duels_won": stats_dict.get('away_aerial_duels_won'),
        "home_aerial_duels_pct": stats_dict.get('home_aerial_duels_pct'),
        "away_aerial_duels_pct": stats_dict.get('away_aerial_duels_pct'),
        "home_dribbles_successful": stats_dict.get('home_dribbles_successful'),
        "away_dribbles_successful": stats_dict.get('away_dribbles_successful'),
        "home_dribbles_pct": stats_dict.get('home_dribbles_pct'),
        "away_dribbles_pct": stats_dict.get('away_dribbles_pct'),
        "home_offsides": stats_dict.get('home_offsides'),
        "away_offsides": stats_dict.get('away_offsides')
    })


def save_match_player_stats(match_id: int, match_data: dict, home_team_id: int, away_team_id: int):
    """Oyuncu istatistiklerini kaydet"""
    check_query = f"SELECT COUNT(*) as cnt FROM public.match_player_stats WHERE match_id = {match_id}"
    existing = execute_query(check_query)
    if existing and existing[0]['cnt'] > 0:
        return
    
    content = match_data.get('content', {})
    player_stats_data = content.get('playerStats', {})
    lineup = content.get('lineup', {})
    
    if not player_stats_data:
        return
    
    home_team = lineup.get('homeTeam', {})
    away_team = lineup.get('awayTeam', {})
    home_team_fotmob_id = home_team.get('id')
    away_team_fotmob_id = away_team.get('id')
    
    def get_stat(stats_dict, key):
        if not stats_dict:
            return None
        stat_obj = stats_dict.get(key, {})
        if not stat_obj:
            return None
        stat_data = stat_obj.get('stat', {})
        if not stat_data:
            return None
        return stat_data.get('value')
    
    def get_stat_total(stats_dict, key):
        if not stats_dict:
            return None
        stat_obj = stats_dict.get(key, {})
        if not stat_obj:
            return None
        stat_data = stat_obj.get('stat', {})
        if not stat_data:
            return None
        return stat_data.get('total')
    
    for player_id, player_data in player_stats_data.items():
        player_name = player_data.get('name', '')
        player_team_id = player_data.get('teamId')
        is_goalkeeper = player_data.get('isGoalkeeper', False)
        
        if player_team_id == home_team_fotmob_id:
            team_id = home_team_id
        elif player_team_id == away_team_fotmob_id:
            team_id = away_team_id
        else:
            continue
        
        all_stats = {}
        for stat_group in player_data.get('stats', []):
            group_stats = stat_group.get('stats', {})
            all_stats.update(group_stats)
        
        rating = get_stat(all_stats, 'FotMob rating') or get_stat(all_stats, 'rating_title')
        minutes_played = get_stat(all_stats, 'Minutes played') or get_stat(all_stats, 'minutes_played')
        
        goals = get_stat(all_stats, 'Goals') or get_stat(all_stats, 'goals') or 0
        assists = get_stat(all_stats, 'Assists') or get_stat(all_stats, 'assists') or 0
        xg = get_stat(all_stats, 'Expected goals (xG)') or get_stat(all_stats, 'expected_goals')
        xa = get_stat(all_stats, 'Expected assists (xA)') or get_stat(all_stats, 'expected_assists')
        total_shots = get_stat(all_stats, 'Total shots') or get_stat(all_stats, 'total_shots') or 0
        shots_on_target = get_stat(all_stats, 'Shots on target') or get_stat(all_stats, 'ShotsOnTarget') or 0
        touches = get_stat(all_stats, 'Touches') or get_stat(all_stats, 'touches') or 0
        total_passes = get_stat_total(all_stats, 'Accurate passes') or get_stat_total(all_stats, 'accurate_passes') or 0
        accurate_passes = get_stat(all_stats, 'Accurate passes') or get_stat(all_stats, 'accurate_passes') or 0
        key_passes = get_stat(all_stats, 'Key passes') or get_stat(all_stats, 'key_passes') or 0
        
        tackles = get_stat(all_stats, 'Tackles') or get_stat(all_stats, 'tackles') or 0
        interceptions = get_stat(all_stats, 'Interceptions') or get_stat(all_stats, 'interceptions') or 0
        clearances = get_stat(all_stats, 'Clearances') or get_stat(all_stats, 'clearances') or 0
        duels_won = get_stat(all_stats, 'Duels won') or get_stat(all_stats, 'duels_won') or 0
        duels_lost = get_stat(all_stats, 'Duels lost') or get_stat(all_stats, 'duels_lost') or 0
        fouls_committed = get_stat(all_stats, 'Fouls') or get_stat(all_stats, 'fouls') or 0
        fouls_won = get_stat(all_stats, 'Was fouled') or get_stat(all_stats, 'was_fouled') or 0
        
        # GK stats
        saves = None
        goals_conceded = None
        if is_goalkeeper:
            saves = get_stat(all_stats, 'Saves') or get_stat(all_stats, 'saves') or 0
            goals_conceded = get_stat(all_stats, 'Goals conceded') or get_stat(all_stats, 'goals_conceded')
        
        query = """
            INSERT INTO public.match_player_stats (
                match_id, team_id, player_name, rating, minutes_played, position,
                goals, assists, xg, xa, total_shots, shots_on_target,
                touches, total_passes, accurate_passes, key_passes,
                tackles, interceptions, clearances,
                duels_won, duels_lost, fouls_committed, fouls_won,
                saves, goals_conceded
            )
            VALUES (
                :match_id, :team_id, :player_name, :rating, :minutes_played, :position,
                :goals, :assists, :xg, :xa, :total_shots, :shots_on_target,
                :touches, :total_passes, :accurate_passes, :key_passes,
                :tackles, :interceptions, :clearances,
                :duels_won, :duels_lost, :fouls_committed, :fouls_won,
                :saves, :goals_conceded
            )
        """
        execute_insert(query, {
            "match_id": match_id,
            "team_id": team_id,
            "player_name": player_name,
            "rating": rating,
            "minutes_played": minutes_played,
            "position": None,
            "goals": goals,
            "assists": assists,
            "xg": xg,
            "xa": xa,
            "total_shots": total_shots,
            "shots_on_target": shots_on_target,
            "touches": touches,
            "total_passes": total_passes,
            "accurate_passes": accurate_passes,
            "key_passes": key_passes,
            "tackles": tackles,
            "interceptions": interceptions,
            "clearances": clearances,
            "duels_won": duels_won,
            "duels_lost": duels_lost,
            "fouls_committed": fouls_committed,
            "fouls_won": fouls_won,
            "saves": saves,
            "goals_conceded": goals_conceded
        })


def save_full_match_data(match_data: dict) -> int:
    """
    Maç verisini TÜM ilgili tablolara kaydet
    Ana fonksiyon - fotmob_to_db.process_match() gibi çalışır
    
    Returns:
        match_id: Kaydedilen maçın ID'si
    """
    general = match_data.get('general', {})
    
    # 1. Ligi kaydet/al
    league_id = save_league(match_data)
    
    # 2. Takımları kaydet/al
    home_team_data = general.get('homeTeam', {})
    away_team_data = general.get('awayTeam', {})
    
    home_team_id = save_team(home_team_data, league_id)
    away_team_id = save_team(away_team_data, league_id)
    
    # 3. Maçı kaydet
    match_id = save_match(match_data, home_team_id, away_team_id, league_id)
    
    # 4. İlgili tüm verileri kaydet
    save_match_stats(match_id, match_data)
    save_match_advanced_stats(match_id, match_data)
    save_match_context(match_id, match_data)
    save_match_formations(match_id, match_data)
    save_match_lineups(match_id, match_data, home_team_id, away_team_id)
    save_match_player_stats(match_id, match_data, home_team_id, away_team_id)
    save_match_events(match_id, match_data, home_team_id, away_team_id)
    save_player_availability(match_id, match_data, home_team_id, away_team_id)
    save_h2h_stats(match_data, home_team_id, away_team_id)
    
    return match_id

