import psycopg2
import hashlib
import random
import os
from datetime import datetime, timedelta

GRUPPEN = {
    "Alpha": {
        "trend": "stabil_gut",
        "pseudonyme": ["Roter Falke", "Blauer Wolf", "Goldener Adler", "Stiller Luchs", "Flinker Otter"],
    },
    "Beta": {
        "trend": "abwaerts",
        "pseudonyme": ["Wilder Panther", "Kluger Rabe", "Tapferer Bär", "Schneller Fuchs", "Mutiger Hirsch"],
    },
    "Gamma": {
        "trend": "mittel",
        "pseudonyme": ["Bunter Papagei", "Leiser Gecko", "Cooler Delfin", "Sanfter Kolibri", "Frecher Tiger"],
    },
}

WORKLOAD_OPTIONS = ["zu_wenig", "passt", "zu_viel"]


def hash_pseudo(pseudo):
    return hashlib.sha256(pseudo.strip().encode()).hexdigest()


def stimmung_for(trend, week_idx):
    if trend == "stabil_gut":
        base = random.uniform(3.5, 4.5)
    elif trend == "abwaerts":
        base = 4.2 - (week_idx * 0.35) + random.uniform(-0.3, 0.3)
    else:
        base = random.uniform(2.8, 3.5)
    return max(1, min(5, round(base)))


def kommunikation_for(trend, week_idx):
    if trend == "stabil_gut":
        base = random.uniform(3.5, 4.5)
    elif trend == "abwaerts":
        base = 4.0 - (week_idx * 0.3) + random.uniform(-0.3, 0.3)
    else:
        base = random.uniform(2.5, 3.5)
    return max(1, min(5, round(base)))


def workload_for(trend, week_idx):
    if trend == "stabil_gut":
        return random.choice(["passt", "passt", "passt", "zu_viel"])
    elif trend == "abwaerts":
        weights = ["passt"] * max(1, 4 - week_idx) + ["zu_viel"] * min(4, 1 + week_idx)
        return random.choice(weights)
    else:
        return random.choice(WORKLOAD_OPTIONS)


def seed():
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS pulse_checks (
            id SERIAL PRIMARY KEY,
            submitted_at TIMESTAMP DEFAULT NOW(),
            anon_token VARCHAR,
            gruppe VARCHAR,
            stimmung INTEGER,
            workload VARCHAR,
            kommunikation INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS teilnehmer (
            pseudo VARCHAR,
            gruppe VARCHAR,
            email VARCHAR,
            active BOOLEAN DEFAULT true,
            PRIMARY KEY (pseudo, gruppe)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS reminder_log (
            sent_at TIMESTAMP DEFAULT NOW(),
            gruppe VARCHAR,
            count INTEGER
        )
    """)

    cur.execute("DELETE FROM pulse_checks")
    cur.execute("DELETE FROM teilnehmer")

    today = datetime.now()
    last_monday = today - timedelta(days=today.weekday())
    mondays = [last_monday - timedelta(weeks=w) for w in range(5, -1, -1)]

    row_count = 0
    for gruppe_name, config in GRUPPEN.items():
        trend = config["trend"]
        for pseudo in config["pseudonyme"]:
            email = f"{pseudo.lower().replace(' ', '.')}@example.com"
            cur.execute(
                "INSERT INTO teilnehmer (pseudo, gruppe, email) VALUES (%s, %s, %s)",
                [pseudo, gruppe_name, email]
            )

        for week_idx, monday in enumerate(mondays):
            for pseudo in config["pseudonyme"]:
                if random.random() > 0.85:
                    continue
                token = hash_pseudo(pseudo)
                stimmung = stimmung_for(trend, week_idx)
                komm = kommunikation_for(trend, week_idx)
                wl = workload_for(trend, week_idx)
                ts = monday.replace(
                    hour=random.randint(8, 17),
                    minute=random.randint(0, 59),
                    second=0, microsecond=0,
                )
                cur.execute(
                    """INSERT INTO pulse_checks
                       (submitted_at, anon_token, gruppe, stimmung, workload, kommunikation)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    [ts, token, gruppe_name, stimmung, wl, komm]
                )
                row_count += 1

    cur.close()
    conn.close()
    print(f"Seed: {row_count} Pulse-Checks, {sum(len(c['pseudonyme']) for c in GRUPPEN.values())} Teilnehmer.")


if __name__ == "__main__":
    seed()
