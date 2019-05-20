import server
from server import r
from datetime import timedelta
from dynaconf import settings
from dataclasses import dataclass
from enum import Enum
from threading import Lock
import tbapy


@dataclass
class CachedTeam:
    name: str
    num_quals_matches: int


def team_number_to_key(number):
    return "frc%i" % number


def team_key_to_number(key):
    return int(key.strip("frc"))


event_cache = {}
tba_settings = settings["module_tba"]
scheduler, db = server.setup()
update_lock = Lock()
tba = tbapy.TBA(tba_settings.api_key)
user_team_key = team_number_to_key(settings["team_number"])
event_key = tba_settings.event_key


def populate_event_cache():
    """Populates the in memory cache of team information that doesn't change throughout the event.
    This is used to avoid making an excessive number of requests to TBA"""
    event_cache.clear()
    event_teams = tba.event_teams(event_key, simple=True)
    for team in event_teams:
        matches = tba.team_matches(team.key, event_key, simple=True)
        quals_matches = list(filter(lambda m: m.comp_level == "qm", matches))
        num_quals_matches = len(quals_matches)
        team_name = team.nickname
        event_cache[team.key] = CachedTeam(team_name, num_quals_matches)


def init_tables():
    # Create the database if it doesn't exist
    server.init_table("eventData", db)

    # Populate the table with the rows
    r.table("eventData").insert({"id": "rankings", "value": []}, conflict="replace").run(db)
    r.table("eventData").insert({"id": "schedule", "value": []}, conflict="replace").run(db)


def update_rankings():
    rankings = tba.event_rankings(event_key)
    rank_list = []
    for ranking in rankings.rankings:
        team_key = ranking["team_key"]
        team_number = team_key_to_number(team_key)
        team_rank = ranking["rank"]
        team_matches_played = ranking["matches_played"]
        team_avg_rp = ranking["sort_orders"][0]
        team_record = ranking["record"]
        team_record_string = " - ".join([str(team_record[field]) for field in ["wins", "losses", "ties"]])
        team_cached = event_cache[team_key]
        team_num_matches = team_cached.num_quals_matches
        team_matches_left = team_num_matches - team_matches_played
        team_name = team_cached.name
        team_full_name = str(team_number) + ' - ' + team_name
        data = {
            "teamNumber": team_number,
            "teamName": team_full_name,
            "teamRecord": team_record_string,
            "rank": team_rank,
            "matchesLeft": team_matches_left,
            "rpAverage": team_avg_rp,
            "record": team_record
        }
        rank_list.append(data)
    r.table("eventData").get("rankings").update({"value": rank_list}).run(db)


def update_schedule():
    event_matches = tba.team_matches(team=user_team_key, event=event_key, simple=True)
    schedule = []
    for match in event_matches:
        match_number = match["match_number"]
        match_type = match["comp_level"].upper()
        match_string = match_type + " " + str(match_number)
        match_scheduled_time = match["time"]
        match_predicted_time = match["predicted_time"]
        red_score = match["alliances"]["red"]["score"]
        blue_score = match["alliances"]["blue"]["score"]
        user_alliance = "red" if user_team_key in match["alliances"]["red"]["team_keys"] else "blue"
        opponent_alliance = "red" if user_alliance == "blue" else "blue"
        match_outcome = "Win" if user_alliance == match["winning_alliance"] else "Loss" if opponent_alliance == match["winning_alliance"] else "Tie"
        allies = list(match["alliances"][user_alliance]["team_keys"])
        allies.remove(user_team_key)
        opponents = list(match["alliances"][opponent_alliance]["team_keys"])
        ally_1 = team_key_to_number(allies[0])
        ally_2 = team_key_to_number(allies[1])
        opponent_1 = team_key_to_number(opponents[0])
        opponent_2 = team_key_to_number(opponents[1])
        opponent_3 = team_key_to_number(opponents[2])
        data = {
            "matchNumber": match_number,
            "matchType": match_type,
            "matchString": match_string,
            "matchOutcome": match_outcome,
            "ally1": ally_1,
            "ally2": ally_2,
            "oppo1": opponent_1,
            "oppo2": opponent_2,
            "oppo3": opponent_3,
            "scheduledTime": match_scheduled_time,
            "predictedTime": match_predicted_time,
            "bumperColor": user_alliance,
            "redScore": red_score,
            "blueScore": blue_score
        }
        schedule.append(data)
    schedule = sorted(schedule, key = lambda k: k["scheduledTime"])
    r.table("eventData").get("schedule").update({"value": schedule}).run(db)


@scheduler.job(interval=timedelta(seconds=tba_settings.matches_update_rate_seconds))
def update_matches():
    update_lock.acquire()
    update_rankings()  # Update the rankings list
    update_schedule()
    update_lock.release()


@scheduler.job(interval=timedelta(seconds=tba_settings.event_update_rate_seconds))
def update_event():
    update_lock.acquire()
    populate_event_cache()
    update_lock.release()


if __name__ == '__main__':
    init_tables()  # Pull in initial data
    update_event()
    update_matches()
    scheduler.start(block=True)  # Start the update loop
