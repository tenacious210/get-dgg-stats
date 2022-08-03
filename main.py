from datetime import datetime, timedelta
import requests
import sqlite3
import json
import re

from google.cloud import storage


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def update_emote_stats(
    start_date: datetime = datetime.today() - timedelta(days=1),
    end_date: datetime = None,
):
    if not end_date:
        end_date = start_date
    emote_json = requests.get("https://cdn.destiny.gg/emotes/emotes.json").json()
    emotes = [e["prefix"] for e in emote_json]

    con = sqlite3.connect("dgg_stats.db")
    cur = con.cursor()
    cmd = (
        "CREATE TABLE IF NOT EXISTS EmoteStats ("
        "UserName STRING NOT NULL, "
        "Date DATE NOT NULL, "
        "UNIQUE (UserName, Date))"
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
        print(f"Getting emotes from {rustle_url}")
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
            cur.execute(
                f"INSERT OR IGNORE INTO EmoteStats ({db_keys}) VALUES ({db_values})"
            )
        con.commit()

    print("Getting top posters")
    cmd = (
        "CREATE TABLE IF NOT EXISTS TopPosters ("
        "Emote STRING NOT NULL UNIQUE, "
        "Posters STRING NOT NULL)"
    )
    cur.execute(cmd)
    cur.execute("DELETE FROM TopPosters")
    target_day = datetime.today() - timedelta(days=30)
    sql_date = target_day.strftime("%Y-%m-%d")
    for emote in emotes:
        cmd = (
            f"SELECT UserName,SUM({emote}) FROM EmoteStats "
            f"WHERE Date >= DATE('{sql_date}') AND {emote} > 0 "
            f"GROUP BY UserName ORDER BY SUM({emote}) DESC "
            f"LIMIT 5"
        )
        top_posters_raw = cur.execute(cmd).fetchall()
        top_posters = json.dumps({u: a for u, a in top_posters_raw})
        cmd = "INSERT INTO TopPosters VALUES (:emote, :posters)"
        params = {"emote": emote, "posters": top_posters}
        cur.execute(cmd, params)
    con.commit()

    con.close()

    print(f"Emotes updated successfully at {datetime.now()}")


def update_lines(
    start_date: datetime = datetime.today() - timedelta(days=1),
    end_date: datetime = None,
):
    if not end_date:
        end_date = start_date
    con = sqlite3.connect("dgg_stats.db")
    cur = con.cursor()
    cmd = (
        "CREATE TABLE IF NOT EXISTS Lines ("
        "UserName STRING NOT NULL UNIQUE, "
        "Amount INT NOT NULL)"
    )
    cur.execute(cmd)

    next_day = end_date + timedelta(days=1)
    for day in daterange(start_date, next_day):
        lines = {}
        year_num, month_num, month_name, day_num = day.strftime("%Y %m %B %d").split()
        rustle_url = (
            "https://dgg.overrustlelogs.net/Destinygg%20chatlog/"
            f"{month_name}%20{year_num}/{year_num}-{month_num}-{day_num}.txt"
        )
        print(f"Getting lines from {rustle_url}")
        logs = requests.get(rustle_url).text.split("\n")
        for log in logs:
            if len(log) > 26:
                user = log[26 : log.find(":", 26)]
                if user not in lines.keys():
                    lines[user] = 0
                lines[user] += 1
        for user, amount in lines.items():
            params = {"user": user, "amount": amount}
            cur.execute(
                f"INSERT OR IGNORE INTO Lines (UserName, Amount) VALUES (:user, 0)",
                params,
            )
            cur.execute(
                f"UPDATE Lines SET Amount = Amount + :amount WHERE UserName = :user",
                params,
            )
        con.commit()
    con.close()
    print(f"Lines updated successfully at {datetime.now()}")


if __name__ == "__main__":
    storage_client = storage.Client()
    bucket = storage_client.bucket("tenadev")
    blob = bucket.blob("dgg_stats.db")
    blob.download_to_filename("dgg_stats.db")
    update_emote_stats()
    update_lines()
    blob.upload_from_filename("dgg_stats.db")
