from datetime import datetime, timedelta, date
from google.cloud import storage
from time import sleep
import requests
import sqlite3
import re


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def update_emote_stats(
    start_date: datetime = datetime.today(), end_date: datetime = None
):
    if not end_date:
        end_date = start_date
    emote_json = requests.get("https://cdn.destiny.gg/emotes/emotes.json").json()
    emotes = [e["prefix"] for e in emote_json]

    con = sqlite3.connect("emote_stats.db")
    # Will create the .db file if it doesn't exist
    cur = con.cursor()
    cmd = (
        "CREATE TABLE IF NOT EXISTS EmoteStats ("
        "UserName STRING NOT NULL, "
        "Date DATE NOT NULL)"
    )
    cur.execute(cmd)

    columns = [i[1] for i in cur.execute("PRAGMA table_info(EmoteStats)")]
    for emote in emotes:
        if emote not in columns:
            cur.execute(f"ALTER TABLE EmoteStats ADD {emote} INT")

    next_day = end_date + timedelta(days=1)
    for day in daterange(start_date, next_day):
        user_emotes = {}
        year_num, month_num, month_name, day_num = day.strftime("%Y %m %B %d").split()
        rustle_url = (
            "https://dgg.overrustlelogs.net/Destinygg%20chatlog/"
            f"{month_name}%20{year_num}/{year_num}-{month_num}-{day_num}.txt"
        )
        print(f"Connecting to {rustle_url}")
        logs = requests.get(rustle_url).text.split("\n")
        for log in logs:
            if len(log) > 26:
                user = log[26 : log.find(":", 26)]
                if user not in user_emotes.keys():
                    user_emotes[user] = {emote: 0 for emote in emotes}
                for emote in emotes:
                    user_emotes[user][emote] += len(re.findall(rf"\b{emote}\b", log))
        db_date = f"{year_num}-{month_num}-{day_num}"
        db_keys = (
            f"`UserName`,`Date`{''.join([f',`{emote_name}`' for emote_name in emotes])}"
        )
        for username, emote_dict in user_emotes.items():
            db_values = f"'{username}','{db_date}'" + "".join(
                [f",'{emote_count}'" for emote_count in emote_dict.values()]
            )
            cur.execute(f"INSERT INTO EmoteStats ({db_keys}) VALUES ({db_values})")
        con.commit()

    con.close()

    storage_client = storage.Client()
    bucket = storage_client.bucket("tenadev")
    blob = bucket.blob("emote_stats.db")
    blob.upload_from_filename("emote_stats.db")

    print(f"Database updated successfully at {datetime.now()}")


if __name__ == "__main__":
    update_emote_stats()
