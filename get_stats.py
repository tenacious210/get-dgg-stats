from write_stats import define_tables
import requests
import re

emote_json = requests.get("https://cdn.destiny.gg/emotes/emotes.json").json()
emote_names = [e["prefix"] for e in emote_json]
user_index = define_tables(return_users=True)

ban_pattern = re.compile(
    r"(?i)^\[(?P<timestamp>\d+-\d+-\d+ \d+:\d+:\d+ UTC)\] "
    r"(?P<mod>\w+): !(?P<type>ban|mute|ipban|ip) ?"
    r"(?P<duration>\d+\w+)? (?P<user>\w+) ?(?P<reason>.*)?"
)
admins = (
    "Destiny",
    "RightToBearArmsLOL",
    "Lemmiwinks",
    "Cake",
    "Ninou",
    "Linusred",
    "MrMouton",
)


def get_mentions(log):
    mentions = []
    if len(log) > 26:
        message = log[log.find(":", 26) :]
        for username in user_index:
            if username in message:
                mentions.append(username)
    return mentions


def get_emotes(log):
    emotes = {}
    if len(log) > 26:
        for emote in emote_names:
            if count := len(re.findall(rf"\b{emote}\b", log)):
                if emote not in emotes.keys():
                    emotes[emote] = 0
                emotes[emote] += count
    return emotes


def get_tng_score(log):
    social_credits = {}
    if len(log) > 26:
        if log[26 : log.find(":", 26)] == "tng69":
            for credit_change in re.findall(r"(\w+) (\+|-)(\d+)", log):
                user, change, amount = credit_change
                if user not in social_credits.keys():
                    social_credits[user] = 0
                if change == "+":
                    social_credits[user] += int(amount)
                else:
                    social_credits[user] -= int(amount)
    return social_credits


def get_bans(log):
    ban = {}
    if len(log) > 26:
        if log[26 : log.find(":", 26)] in admins:
            if ban := re.search(ban_pattern, log):
                ban = ban.groupdict()
                ban["type"] = ban["type"].lower()
                ban["user"] = ban["user"].lower()
                ban["duration"] = ban["duration"].lower() if ban["duration"] else None
                if not ban["reason"]:
                    ban["reason"] = None
    return ban


def get_dgg_stats(log):
    if len(log) > 26:
        stats = {"username": log[26 : log.find(":", 26)]}
        stats["mentions"] = get_mentions(log)
        stats["emotes"] = get_emotes(log)
        stats["tng_score"] = get_tng_score(log)
        stats["bans"] = get_bans(log)
        return stats


def process_dgg_stats(stats: list):
    stat_types = ("lines", "mentions", "emotes", "bans", "tng_score")
    processed = {k: {} for k in stat_types}
    for stat in [s for s in stats if s]:
        if stat["username"] not in processed["lines"].keys():
            processed["lines"][stat["username"]] = 0
        processed["lines"][stat["username"]] += 1

        for user_mentioned in stat["mentions"]:
            if user_mentioned not in processed["mentions"].keys():
                processed["mentions"][user_mentioned] = {}
            if stat["username"] not in processed["mentions"][user_mentioned].keys():
                processed["mentions"][user_mentioned][stat["username"]] = 0
            processed["mentions"][user_mentioned][stat["username"]] += 1

        for emote, amount in stat["emotes"].items():
            if stat["username"] not in processed["emotes"].keys():
                processed["emotes"][stat["username"]] = {}
            if emote not in processed["emotes"][stat["username"]].keys():
                processed["emotes"][stat["username"]][emote] = 0
            processed["emotes"][stat["username"]][emote] += amount

        if stat["bans"]:
            user = stat["bans"].pop("user")
            if user not in processed["bans"].keys():
                processed["bans"][user] = []
            processed["bans"][user].append(stat["bans"])

        for user, credit_change in stat["tng_score"].items():
            if user not in processed["tng_score"].keys():
                processed["tng_score"][user] = 0
            processed["tng_score"][user] += credit_change

    return processed
