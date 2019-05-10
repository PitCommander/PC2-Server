import server
from server import r
from datetime import timedelta
from dynaconf import settings
from dataclasses import dataclass
from threading import Lock
import tbapy


@dataclass
class CachedTeam:
    name: str
    num_quals_matches: int


event_cache = {}
tba_settings = settings["module_tba"]
scheduler, db = server.setup()
update_lock = Lock()
tba = tbapy.TBA(tba_settings.api_key)
event_key = tba_settings.event_key


def team_number_to_key(number):
    return "frc%i" % number


def team_key_to_number(key):
    return int(key.strip("frc"))


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
    # Clean out the database
    server.init_table("event_data", db)

    # Populate the table with the rows
    r.table("event_data").insert({"id": "rankings", "value": []}, conflict="replace").run(db)


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
        team_cached = event_cache[team_key]
        team_num_matches = team_cached.num_quals_matches
        team_matches_left = team_num_matches - team_matches_played
        team_name = team_cached.name
        data = {
            "teamNumber": team_number,
            "teamName": team_name,
            "rank": team_rank,
            "matchesLeft": team_matches_left,
            "rpAverage": team_avg_rp,
            "record": team_record
        }
        rank_list.append(data)
    r.table("event_data").get("rankings").update({"value": rank_list}).run(db)


@scheduler.job(interval=timedelta(seconds=tba_settings.matches_update_rate_seconds))
def update_matches():
    update_lock.acquire()
    update_rankings()  # Update the rankings list
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
