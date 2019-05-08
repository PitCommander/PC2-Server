import server
from server import r
from datetime import timedelta
from dynaconf import settings
import tbapy

tba_settings = settings["module_tba"]
scheduler, db = server.setup()

tba = tbapy.TBA(tba_settings.api_key)

event_key = tba_settings.event_key


def team_number_to_key(number):
    return "frc%i" % number


def team_key_to_number(key):
    return int(key.strip("frc"))


def init_tables():
    # Clean out the database
    server.init_table("event_rankings", db)
    server.init_table("event_schedule", db)
    r.table("event_rankings").index_create("teamNumber").run(db)

    # Populate rankings table with initial data
    event_teams = tba.event_teams(event_key, simple=True)
    for team in event_teams:
        matches = tba.team_matches(team.key, event_key, simple=True)
        quals_matches = list(filter(lambda m: m.comp_level == "qm", matches))
        num_quals_matches = len(quals_matches)
        row = {
            "rank": 0,
            "teamNumber": team.team_number,
            "teamName": team.nickname,
            "numQualsMatches": num_quals_matches,
            "rpAverage": 0.0,
            "qualsMatchesLeft": 0,
            "record": {
                "wins": 0,
                "losses": 0,
                "ties": 0
            }
        }
        r.table("event_rankings").insert(row).run(db)


def update_rankings():
    rankings = tba.event_rankings(event_key)
    for ranking in rankings.rankings:
        team_number = team_key_to_number(ranking["team_key"])
        team_rank = ranking["rank"]
        team_matches_played = ranking["matches_played"]
        team_avg_rp = ranking["sort_orders"][0]
        team_row = list(r.table("event_rankings").get_all(team_number, index="teamNumber").run(db))[0]
        team_num_matches = team_row["numQualsMatches"]
        team_record = ranking["record"]
        data = {
            "rank": team_rank,
            "qualsMatchesLeft": team_num_matches - team_matches_played,
            "rpAverage": team_avg_rp,
            "record": team_record
        }
        r.table("event_rankings").get_all(team_number, index="teamNumber").update(data).run(db)


@scheduler.job(interval=timedelta(seconds=tba_settings.update_rate_seconds))
def update():
    update_rankings()  # Update the rankings list


if __name__ == '__main__':
    init_tables()  # Pull in initial data
    update()  # Run update once to populate everything initially
    scheduler.start(block=True)  # Start the update loop
