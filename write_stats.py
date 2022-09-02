from datetime import datetime, timedelta
import requests
import sqlite3
import json


def define_tables(return_users=False):
    con = sqlite3.connect("dgg_stats.db")
    cur = con.cursor()
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
        cur.execute(command)
    user_index = None
    if return_users:
        cmd = "SELECT UserName FROM Lines ORDER BY Amount DESC LIMIT 5000"
        user_index = [str(i[0]) for i in cur.execute(cmd).fetchall()]
    con.commit()
    con.close()
    return user_index


def check_latest_emotes():
    emote_json = requests.get("https://cdn.destiny.gg/emotes/emotes.json").json()
    emotes = [e["prefix"] for e in emote_json]
    con = sqlite3.connect("dgg_stats.db")
    cur = con.cursor()
    columns = [i[1] for i in cur.execute("PRAGMA table_info(EmoteStats)")]
    for emote in emotes:
        if emote not in columns:
            cur.execute(f"ALTER TABLE EmoteStats ADD {emote} INT")
    con.commit()
    con.close()
    return emotes


def add_lines(username, amount):
    con = sqlite3.connect("dgg_stats.db")
    cur = con.cursor()
    params = {"username": username, "amount": amount}
    commands = (
        "INSERT OR IGNORE INTO Lines (UserName, Amount) VALUES (:username, 0)",
        "UPDATE Lines SET Amount = Amount + :amount WHERE UserName = :username",
    )
    for cmd in commands:
        cur.execute(cmd, params)
    con.commit()
    con.close()


def update_mentions(username, mentions: dict):
    con = sqlite3.connect("dgg_stats.db")
    cur = con.cursor()
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
    con.commit()
    con.close()


def update_emotes(username, date: datetime, emote_dict: dict):
    con = sqlite3.connect("dgg_stats.db")
    cur = con.cursor()
    params = {"username": username}
    cmd = (
        f"INSERT OR IGNORE INTO EmoteStats "
        f"(UserName, Date, {', '.join(emote_dict.keys())}) "
        f"VALUES (:username, '{date.strftime('%Y-%m-%d')}', "
        f"{', '.join([str(v) for v in emote_dict.values()])})"
    )
    cur.execute(cmd, params)
    con.commit()
    con.close()


def update_top_posters():
    con = sqlite3.connect("dgg_stats.db")
    cur = con.cursor()
    cur.execute("DELETE FROM TopPosters")
    emotes = check_latest_emotes()
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
        params = {"emote": emote, "posters": top_posters}
        cmd = "INSERT INTO TopPosters VALUES (:emote, :posters)"
        cur.execute(cmd, params)
    con.commit()
    con.close()


def update_tng_score(username, change):
    con = sqlite3.connect("dgg_stats.db")
    cur = con.cursor()
    mode = "+" if change > 0 else "-"
    params = {"username": username, "change": abs(change), "mode": mode}
    commands = (
        "INSERT OR IGNORE INTO TngScore (UserName, Score) VALUES (:username, 0)",
        f"UPDATE TngScore SET Score = Score {mode} :change WHERE UserName = :username",
    )
    for cmd in commands:
        cur.execute(cmd, params)
    con.commit()
    con.close()


def update_bans(username, new_banlist: list):
    con = sqlite3.connect("dgg_stats.db")
    cur = con.cursor()
    params = {"username": username}
    cmd = "INSERT OR IGNORE INTO UserBans (UserName, Bans) VALUES (:username, '[]')"
    cur.execute(cmd, params)
    cmd = "SELECT Bans FROM UserBans WHERE UserName = :username"
    old_banlist = json.loads(cur.execute(cmd, params).fetchall()[0][0])
    params["banlist"] = json.dumps(old_banlist + new_banlist)
    cmd = f"UPDATE UserBans SET Bans = :banlist WHERE UserName = :username"
    cur.execute(cmd, params)
    con.commit()
    con.close()


def write_dgg_stats(stats, date: datetime):
    print("Writing lines...")
    for username, amount in stats["lines"].items():
        add_lines(username, amount)
    print("Writing emotes...")
    print("Writing mentions...")
    for username, mentions in stats["mentions"].items():
        update_mentions(username, mentions)
    print("Writing bans...")
    for username, banlist in stats["bans"].items():
        update_bans(username, banlist)
    print("Writing tng scores...")
    for username, change in stats["tng_score"].items():
        update_tng_score(username, change)
    check_latest_emotes()
    for username, emote_stats in stats["emotes"].items():
        update_emotes(username, date, emote_stats)
    print("Writing top posters...")
    update_top_posters()
