import os
import random
from datetime import timedelta, timezone

from faker import Faker
from pymongo import MongoClient


MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "streaming_final")


def _conn():
    return MongoClient(MONGO_URI)[MONGO_DB]


def _uid():
    return f"user_{random.randint(1, 200)}"


def _watch_sessions(fake: Faker, n: int = 900):
    out = []
    for i in range(n):
        start = fake.date_time_between(start_date="-30d", end_date="now", tzinfo=timezone.utc)
        end = start + timedelta(minutes=random.randint(5, 240))
        pages = ["/home", "/feed", "/watch/intro", "/watch/episode_1", "/search", "/profile"]
        actions = ["play", "pause", "seek", "like", "dislike", "add_to_watchlist"]
        dev = random.choice([
            {"type": "tv", "os": "Tizen", "app": "SmartTV"},
            {"type": "mobile", "os": "iOS", "app": "StreamingApp"},
            {"type": "desktop", "os": "Windows", "app": "Browser"},
        ])
        out.append({
            "session_id": f"watch_{i+1:05d}",
            "user_id": _uid(),
            "start_time": start.isoformat(),
            "end_time": end.isoformat(),
            "pages_visited": random.sample(pages, k=random.randint(2, min(6, len(pages)))),
            "device": dev,
            "actions": random.sample(actions, k=random.randint(2, min(6, len(actions)))),
        })
    return out


def _playback_events(fake: Faker, n: int = 2200):
    out = []
    for i in range(n):
        ts = fake.date_time_between(start_date="-30d", end_date="now", tzinfo=timezone.utc)
        out.append({
            "event_id": f"play_{i+1:05d}",
            "timestamp": ts.isoformat(),
            "event_type": random.choice(["play", "pause", "seek", "buffer_start", "buffer_end", "error"]),
            "details": {
                "video_id": f"vid_{random.randint(1, 500)}",
                "position_seconds": random.randint(0, 10800),
                "quality": random.choice(["144p", "360p", "720p", "1080p"]),
            },
        })
    return out


def _support_cases(fake: Faker, n: int = 400):
    out = []
    for i in range(n):
        created = fake.date_time_between(start_date="-30d", end_date="-1d", tzinfo=timezone.utc)
        status = random.choice(["open", "in_progress", "resolved", "closed"])
        updated = created + timedelta(hours=random.randint(1, 48)) if status in ("resolved", "closed") else fake.date_time_between(start_date=created, end_date="now", tzinfo=timezone.utc)
        msgs = []
        t = created
        for j in range(random.randint(1, 4)):
            t += timedelta(hours=random.randint(1, 8))
            msgs.append({"sender": "user" if j % 2 == 0 else "support", "message": fake.sentence(nb_words=10), "timestamp": t.isoformat()})
        out.append({
            "ticket_id": f"case_{i+1:05d}",
            "user_id": _uid(),
            "status": status,
            "issue_type": random.choice(["billing", "quality", "access", "other"]),
            "messages": msgs,
            "created_at": created.isoformat(),
            "updated_at": updated.isoformat(),
        })
    return out


def _content_recommendations(fake: Faker, n: int = 180):
    out = []
    for i in range(1, n + 1):
        out.append({
            "user_id": f"user_{i}",
            "recommended_products": [f"vid_{random.randint(1, 500)}" for _ in range(random.randint(3, 12))],
            "last_updated": fake.date_time_between(start_date="-7d", end_date="now", tzinfo=timezone.utc).isoformat(),
        })
    return out


def _abuse_reports(fake: Faker, n: int = 450):
    out = []
    for i in range(n):
        out.append({
            "review_id": f"abuse_{i+1:05d}",
            "user_id": _uid(),
            "product_id": f"vid_{random.randint(1, 500)}",
            "review_text": fake.text(max_nb_chars=140),
            "rating": random.randint(1, 5),
            "moderation_status": random.choice(["pending", "approved", "rejected"]),
            "flags": random.sample(["spam_suspected", "hate_speech", "nudity", "other"], k=random.choice([0, 1, 2])),
            "submitted_at": fake.date_time_between(start_date="-30d", end_date="now", tzinfo=timezone.utc).isoformat(),
        })
    return out


def run_seed():
    db = _conn()
    fake = Faker()
    print(f"Streaming seed: {MONGO_URI} db={MONGO_DB}")

    for c in ["WatchSessions", "PlaybackEvents", "SupportCases", "ContentRecommendations", "AbuseReports"]:
        db[c].delete_many({})

    db.WatchSessions.insert_many(_watch_sessions(fake))
    db.PlaybackEvents.insert_many(_playback_events(fake))
    db.SupportCases.insert_many(_support_cases(fake))
    db.ContentRecommendations.insert_many(_content_recommendations(fake))
    db.AbuseReports.insert_many(_abuse_reports(fake))

    print("Done: WatchSessions, PlaybackEvents, SupportCases, ContentRecommendations, AbuseReports")


if __name__ == "__main__":
    run_seed()
