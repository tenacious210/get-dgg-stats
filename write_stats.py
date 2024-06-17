from datetime import datetime, timedelta
from os import getenv
import sqlite3
import json
import logging
import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
db_name = getenv("DGG_STATS_DB")


def define_tables(return_users=False):
    dt_con = sqlite3.connect(db_name, timeout=60.0)
    dt_cur = dt_con.cursor()
    commands = (
        "CREATE TABLE IF NOT EXISTS Lines ("
        "UserName STRING NOT NULL UNIQUE, "
        "Amount INT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS UserMentions ("
        "UserName STRING NOT NULL UNIQUE, "
        "Mentions STRING NOT NULL)",
        "CREATE TABLE IF NOT EXISTS EmoteStats ("
        "UserName STRING NOT NULL, "
        "Date DATE NOT NULL, "
        "UNIQUE (UserName, Date))",
        "CREATE TABLE IF NOT EXISTS TopPosters ("
        "Emote STRING NOT NULL UNIQUE, "
        "Posters STRING NOT NULL)",
        "CREATE TABLE IF NOT EXISTS TngScore ("
        "UserName STRING NOT NULL UNIQUE, "
        "Score INT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS UserBans ("
        "UserName STRING NOT NULL UNIQUE, "
        "Bans STRING NOT NULL)",
    )
    for command in commands:
        dt_cur.execute(command)
    dt_con.commit()
    user_index = None
    if return_users:
        cmd = "SELECT UserName FROM Lines ORDER BY Amount DESC LIMIT 5000"
        user_index_raw = dt_cur.execute(cmd).fetchall()
        user_index = [str(i[0]) for i in user_index_raw]
    dt_con.close()
    return user_index


def check_latest_emotes(cur: sqlite3.Cursor):
    emote_json = requests.get("https://cdn.destiny.gg/emotes/emotes.json").json()
    emotes = [e["prefix"] for e in emote_json]
    columns = [i[1].lower() for i in cur.execute("PRAGMA table_info(EmoteStats)")]
    for emote in emotes:
        if emote.lower() not in columns:
            cur.execute(f"ALTER TABLE EmoteStats ADD `{emote}` INT")
    return emotes


def add_lines(username, amount, cur: sqlite3.Cursor):
    params = {"username": username, "amount": amount}
    commands = (
        "INSERT OR IGNORE INTO Lines (UserName, Amount) VALUES (:username, 0)",
        "UPDATE Lines SET Amount = Amount + :amount WHERE UserName = :username",
    )
    for cmd in commands:
        cur.execute(cmd, params)


def update_mentions(username, mentions: dict, cur: sqlite3.Cursor):
    params = {"username": username}
    cmd = "INSERT OR IGNORE INTO UserMentions (UserName, Mentions) VALUES (:username, '{}')"
    cur.execute(cmd, params)
    cmd = "SELECT Mentions FROM UserMentions WHERE UserName = :username"
    db_mentions = json.loads(cur.execute(cmd, params).fetchall()[0][0])
    for user, amount in mentions.items():
        if user not in db_mentions.keys():
            db_mentions[user] = 0
        db_mentions[user] += amount
    params["mentions"] = json.dumps(db_mentions)
    cmd = "UPDATE UserMentions SET Mentions = :mentions WHERE UserName = :username"
    cur.execute(cmd, params)


def update_emotes(username, date: datetime, emote_dict: dict, cur: sqlite3.Cursor):
    params = {"username": username}
    cmd = (
        f"INSERT OR IGNORE INTO EmoteStats "
        f"(UserName, Date, `{'`, `'.join(emote_dict.keys())}`) "
        f"VALUES (:username, '{date.strftime('%Y-%m-%d')}', "
        f"{', '.join([str(v) for v in emote_dict.values()])})"
    )
    cur.execute(cmd, params)


def trim_old_emote_stats(cur: sqlite3.Cursor):
    target_day = datetime.today() - timedelta(days=30)
    params = {"date": target_day.strftime("%Y-%m-%d")}
    cmd = f"DELETE FROM EmoteStats WHERE Date < DATE(:date)"
    cur.execute(cmd, params)


def update_tng_score(username, change, cur: sqlite3.Cursor):
    mode = "+" if change > 0 else "-"
    params = {"username": username, "change": abs(change), "mode": mode}
    commands = (
        "INSERT OR IGNORE INTO TngScore (UserName, Score) VALUES (:username, 0)",
        f"UPDATE TngScore SET Score = Score {mode} :change WHERE UserName = :username",
    )
    for cmd in commands:
        cur.execute(cmd, params)


def update_bans(username, new_banlist: list, cur: sqlite3.Cursor):
    params = {"username": username}
    cmd = "INSERT OR IGNORE INTO UserBans (UserName, Bans) VALUES (:username, '[]')"
    cur.execute(cmd, params)
    cmd = "SELECT Bans FROM UserBans WHERE UserName = :username"
    old_banlist = json.loads(cur.execute(cmd, params).fetchall()[0][0])
    params["banlist"] = json.dumps(old_banlist + new_banlist)
    cmd = f"UPDATE UserBans SET Bans = :banlist WHERE UserName = :username"
    cur.execute(cmd, params)


def update_top_posters(cur: sqlite3.Cursor):
    cur.execute("DELETE FROM TopPosters")
    emotes = check_latest_emotes(cur)
    target_day = datetime.today() - timedelta(days=30)
    sql_date = target_day.strftime("%Y-%m-%d")
    for emote in emotes:
        cmd = (
            f"SELECT UserName,SUM(`{emote}`) FROM EmoteStats "
            f"WHERE Date >= DATE('{sql_date}') AND `{emote}` > 0 "
            f"GROUP BY UserName ORDER BY SUM(`{emote}`) DESC "
            f"LIMIT 5"
        )
        top_posters_raw = cur.execute(cmd).fetchall()
        top_posters = json.dumps({u: a for u, a in top_posters_raw})
        params = {"emote": emote, "posters": top_posters}
        cmd = "INSERT INTO TopPosters VALUES (:emote, :posters)"
        cur.execute(cmd, params)


def write_dgg_stats(stats, date: datetime):
    con = sqlite3.connect(db_name, timeout=60.0)
    cur = con.cursor()
    logger.debug("Writing stats...")
    for username, amount in stats["lines"].items():
        add_lines(username, amount, cur)
    for username, mentions in stats["mentions"].items():
        update_mentions(username, mentions, cur)
    for username, banlist in stats["bans"].items():
        update_bans(username, banlist, cur)
    for username, change in stats["tng_score"].items():
        update_tng_score(username, change, cur)
    check_latest_emotes(cur)
    for username, emote_stats in stats["emotes"].items():
        update_emotes(username, date, emote_stats, cur)
    trim_old_emote_stats(cur)
    logger.debug("Writing top posters...")
    update_top_posters(cur)
    con.commit()
    logger.debug("Cleaning up...")
    cur.execute("VACUUM")
    con.commit()
    con.close()
