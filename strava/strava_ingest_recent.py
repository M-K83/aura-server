import os
import time
import json
import requests
from datetime import datetime, timedelta, timezone

import psycopg
from dotenv import load_dotenv


# ----------------------------
# Env / Config
# ----------------------------
ENV_PATH = os.getenv("AURA_ENV_PATH", "/srv/aura/strava/.env")
load_dotenv(ENV_PATH)


def must_get(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required env var: {name} (checked {ENV_PATH})")
    return val


# Postgres
DB_HOST = must_get("AURA_DB_HOST")
DB_PORT = int(must_get("AURA_DB_PORT"))
DB_NAME = must_get("AURA_DB_NAME")
DB_USER = must_get("AURA_DB_USER")
DB_PASSWORD = must_get("AURA_DB_PASSWORD")
AURA_DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Strava
STRAVA_CLIENT_ID = must_get("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = must_get("STRAVA_CLIENT_SECRET")
STRAVA_REFRESH_TOKEN = must_get("STRAVA_REFRESH_TOKEN")

TOKEN_URL = "https://www.strava.com/oauth/token"
ACTIVITIES_URL = "https://www.strava.com/api/v3/athlete/activities"

STREAM_KEYS = "time,heartrate,cadence,watts,velocity_smooth,altitude,grade_smooth,latlng,temp,moving"
LOOKBACK_BUFFER_HOURS = 6
DEFAULT_DAYS_IF_EMPTY = 30
PER_PAGE = 200
SLEEP_BETWEEN_CALLS_S = 0.2
COMMIT_EVERY_N_ACTIVITIES = 25
MAX_ACTIVITIES_PER_RUN = None

REQUIRED_STREAMS_RUN = {
    "time", "heartrate", "velocity_smooth", "altitude",
    "grade_smooth", "latlng", "distance", "moving"
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_epoch(dt: datetime) -> int:
    return int(dt.timestamp())


def update_env_key(env_path: str, key: str, value: str) -> None:
    """
    Update (or append) KEY=VALUE in a .env file safely.
    """
    lines = []
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()

    out = []
    found = False
    for line in lines:
        if line.startswith(f"{key}="):
            out.append(f"{key}={value}")
            found = True
        else:
            out.append(line)

    if not found:
        out.append(f"{key}={value}")

    tmp = env_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write("\n".join(out) + "\n")
    os.replace(tmp, env_path)


def refresh_access_token() -> str:
    global STRAVA_REFRESH_TOKEN

    r = requests.post(
        TOKEN_URL,
        data={
            "client_id": STRAVA_CLIENT_ID,
            "client_secret": STRAVA_CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": STRAVA_REFRESH_TOKEN,
        },
        timeout=30,
    )
    print("[Token refresh] HTTP", r.status_code)
    if r.status_code != 200:
        raise RuntimeError(f"Token refresh failed: {r.text}")

    data = r.json()

    token = data.get("access_token")
    if not token:
        raise RuntimeError("No access_token in Strava response")

    new_refresh = data.get("refresh_token")
    if new_refresh and new_refresh != STRAVA_REFRESH_TOKEN:
        print("⚠️ Strava rotated refresh token — saving to .env")
        update_env_key(ENV_PATH, "STRAVA_REFRESH_TOKEN", new_refresh)
        STRAVA_REFRESH_TOKEN = new_refresh  # keep this process consistent too

    return token


def fetch_activities(access_token: str, after_epoch: int):
    headers = {"Authorization": f"Bearer {access_token}"}
    page = 1
    fetched = 0

    while True:
        r = requests.get(
            ACTIVITIES_URL,
            headers=headers,
            params={"after": after_epoch, "page": page, "per_page": PER_PAGE},
            timeout=30,
        )

        if r.status_code == 429:
            print("⏳ Rate limited (429). Sleeping 60s…")
            time.sleep(60)
            continue

        if r.status_code != 200:
            raise RuntimeError(f"Activities fetch failed (HTTP {r.status_code}): {r.text}")

        items = r.json()
        if not items:
            break

        for a in items:
            yield a
            fetched += 1
            if MAX_ACTIVITIES_PER_RUN and fetched >= MAX_ACTIVITIES_PER_RUN:
                return

        page += 1
        time.sleep(SLEEP_BETWEEN_CALLS_S)


def fetch_streams(access_token: str, activity_id: int) -> dict:
    url = f"https://www.strava.com/api/v3/activities/{activity_id}/streams"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {
        "keys": STREAM_KEYS,
        "key_by_type": "true",
        "resolution": "high",
        "series_type": "time",
    }

    r = requests.get(url, headers=headers, params=params, timeout=30)

    if r.status_code in (403, 404):
        return {}

    if r.status_code == 429:
        print("⏳ Rate limited (429) on streams. Sleeping 60s…")
        time.sleep(60)
        return fetch_streams(access_token, activity_id)

    if r.status_code != 200:
        raise RuntimeError(f"Streams fetch failed (HTTP {r.status_code}): {r.text}")

    return r.json()


def db_latest_activity_start(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(start_date_utc) FROM aura_strava.activities;")
        return cur.fetchone()[0]


def activity_has_required_streams(conn, activity_id: int, required: set[str]) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT stream_type
            FROM aura_strava.activity_streams
            WHERE activity_id=%s;
            """,
            (activity_id,),
        )
        present = {row[0] for row in cur.fetchall()}

    return required.issubset(present)



def upsert_activity(conn, a: dict) -> None:
    start_latlng = a.get("start_latlng") or [None, None]
    end_latlng = a.get("end_latlng") or [None, None]

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO aura_strava.activities (
              activity_id, athlete_id, name, sport_type,
              start_date_utc, start_date_local, timezone,
              distance_m, moving_time_s, elapsed_time_s,
              total_elevation_gain_m,
              start_lat, start_lng, end_lat, end_lng,
              raw, ingested_at_utc
            )
            VALUES (
              %(id)s, %(athlete_id)s, %(name)s, %(sport_type)s,
              %(start_date)s, %(start_date_local)s, %(timezone)s,
              %(distance)s, %(moving_time)s, %(elapsed_time)s,
              %(total_elevation_gain)s,
              %(slat)s, %(slng)s, %(elat)s, %(elng)s,
              %(raw)s::jsonb, NOW()
            )
            ON CONFLICT (activity_id) DO UPDATE
              SET raw=EXCLUDED.raw,
                  ingested_at_utc=NOW();
            """,
            {
                "id": a["id"],
                "athlete_id": a.get("athlete", {}).get("id"),
                "name": a.get("name"),
                "sport_type": a.get("sport_type") or a.get("type"),
                "start_date": a.get("start_date"),
                "start_date_local": a.get("start_date_local"),
                "timezone": a.get("timezone"),
                "distance": a.get("distance"),
                "moving_time": a.get("moving_time"),
                "elapsed_time": a.get("elapsed_time"),
                "total_elevation_gain": a.get("total_elevation_gain"),
                "slat": start_latlng[0],
                "slng": start_latlng[1],
                "elat": end_latlng[0],
                "elng": end_latlng[1],
                "raw": json.dumps(a),
            },
        )


def upsert_stream(conn, activity_id: int, stype: str, sobj: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO aura_strava.activity_streams (
              activity_id, stream_type, original_size, data, raw, ingested_at_utc
            )
            VALUES (%s,%s,%s,%s::jsonb,%s::jsonb,NOW())
            ON CONFLICT (activity_id, stream_type) DO UPDATE
              SET data=EXCLUDED.data,
                  raw=EXCLUDED.raw,
                  ingested_at_utc=NOW();
            """,
            (
                activity_id,
                stype,
                sobj.get("original_size"),
                json.dumps(sobj.get("data")),
                json.dumps(sobj),
            ),
        )


def main():
    print("=== Strava Incremental Ingester ===")

    token = refresh_access_token()
    print("✅ Token refreshed")

    activities = 0
    streams = 0
    skipped = 0

    with psycopg.connect(AURA_DATABASE_URL) as conn:
        latest = db_latest_activity_start(conn)
        if latest:
            after_dt = latest - timedelta(hours=LOOKBACK_BUFFER_HOURS)
        else:
            after_dt = utc_now() - timedelta(days=DEFAULT_DAYS_IF_EMPTY)

        print(f"Fetching activities after {after_dt.isoformat()}")
        after_epoch = to_epoch(after_dt)

        for a in fetch_activities(token, after_epoch):
            upsert_activity(conn, a)
            activities += 1

            sport = (a.get("sport_type") or a.get("type") or "").lower()

        if sport == "run":
            required = REQUIRED_STREAMS_RUN
        else:
            # Keep it cheap for non-runs; you can expand later if you want
            required = {"time"}

        if activity_has_required_streams(conn, a["id"], required):
            skipped += 1
        else:
            streams_by_type = fetch_streams(token, a["id"])
            for stype, sobj in streams_by_type.items():
                upsert_stream(conn, a["id"], stype, sobj)
                streams += 1
            time.sleep(SLEEP_BETWEEN_CALLS_S)


            if activities % COMMIT_EVERY_N_ACTIVITIES == 0:
                conn.commit()
                print(f"... {activities} activities | {streams} streams | {skipped} skipped")

        conn.commit()

    print("✅ Incremental ingest complete")
    print(f"Activities processed: {activities}")
    print(f"Streams upserted:     {streams}")
    print(f"Streams skipped:      {skipped}")


if __name__ == "__main__":
    main()
