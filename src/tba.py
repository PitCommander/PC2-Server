import server
from server import r
from datetime import timedelta
from dynaconf import settings
import tbapy

tba_settings = settings["module_tba"]
scheduler, db = server.setup()

tba = tbapy.TBA(tba_settings.api_key)

event_key = tba_settings.event_key


def team_key(number):
    return "frc%i" % number


def init_tables():
    # Clean out the database
    server.init_table("event_rankings", db)
    server.init_table("event_schedule", db)

    # Populate rankings table with initial data


@scheduler.job(interval=timedelta(seconds=tba_settings.update_rate_seconds))
def update():
    print("hello")


if __name__ == '__main__':
    event_teams = tba.event_teams(event_key, simple=True)
    teams_num_quals_matches = {}
    for team in event_teams:
        matches = tba.team_matches(team.key, event_key, simple=True)
        quals_matches = list(filter(lambda match: match.comp_level == "qm", matches))
        num_quals_matches = len(quals_matches)
        teams_num_quals_matches[team.key] = num_quals_matches
    event_rankings = tba.event_rankings(event_key)
