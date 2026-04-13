import duckdb
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

FREITEXT_POSITIV = [
    "Teamarbeit läuft super, alle ziehen an einem Strang.",
    "Gute Woche, Projekte sind im Zeitplan.",
    "Kommunikation im Team hat sich deutlich verbessert.",
    "Macht gerade richtig Spaß hier zu arbeiten!",
    "Sehr produktive Woche, gutes Teamgefühl.",
]

FREITEXT_NEUTRAL = [
    "Alles okay, nichts Besonderes zu berichten.",
    "Laufende Projekte gehen voran, ein paar Kleinigkeiten könnten besser sein.",
    "Ganz in Ordnung, manchmal etwas unklare Zuständigkeiten.",
    "Solide Woche, weder besonders gut noch schlecht.",
]

FREITEXT_NEGATIV = [
    "Zu viel Druck von oben, Deadlines sind unrealistisch.",
    "Kommunikation ist schlecht, wichtige Infos kommen zu spät.",
    "Frustrierend — Entscheidungen werden ohne uns getroffen.",
    "Motivation sinkt, Workload ist nicht tragbar.",
    "Fühle mich nicht gehört, Feedback wird ignoriert.",
]

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


def freitext_for(stimmung):
    if random.random() > 0.7:
        return None
    if stimmung >= 4:
        return random.choice(FREITEXT_POSITIV)
    elif stimmung >= 3:
        return random.choice(FREITEXT_NEUTRAL)
    else:
        return random.choice(FREITEXT_NEGATIV)


def seed():
    token = os.environ.get("MOTHERDUCK_TOKEN", "")
    if token:
        db = duckdb.connect(f"md:stimmung?motherduck_token={token}")
    else:
        db = duckdb.connect("stimmung_local.duckdb")

    db.execute("CREATE SEQUENCE IF NOT EXISTS seq_pulse START 1")
    db.execute("""
        CREATE TABLE IF NOT EXISTS pulse_checks (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_pulse'),
            submitted_at TIMESTAMP DEFAULT current_timestamp,
            anon_token VARCHAR,
            gruppe VARCHAR,
            stimmung INTEGER,
            workload VARCHAR,
            kommunikation INTEGER,
            freitext VARCHAR
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS teilnehmer (
            pseudo VARCHAR,
            gruppe VARCHAR,
            email VARCHAR,
            active BOOLEAN DEFAULT true,
            PRIMARY KEY (pseudo, gruppe)
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS reminder_log (
            sent_at TIMESTAMP DEFAULT current_timestamp,
            gruppe VARCHAR,
            count INTEGER
        )
    """)

    db.execute("DELETE FROM pulse_checks")
    db.execute("DELETE FROM teilnehmer")

    today = datetime.now()
    last_monday = today - timedelta(days=today.weekday())
    mondays = [last_monday - timedelta(weeks=w) for w in range(5, -1, -1)]

    row_count = 0
    for gruppe_name, config in GRUPPEN.items():
        trend = config["trend"]
        for pseudo in config["pseudonyme"]:
            email = f"{pseudo.lower().replace(' ', '.')}@example.com"
            db.execute(
                "INSERT INTO teilnehmer (pseudo, gruppe, email) VALUES (?, ?, ?)",
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
                text = freitext_for(stimmung)
                ts = monday.replace(
                    hour=random.randint(8, 17),
                    minute=random.randint(0, 59),
                    second=0, microsecond=0,
                )
                db.execute(
                    """INSERT INTO pulse_checks
                       (submitted_at, anon_token, gruppe, stimmung, workload, kommunikation, freitext)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    [ts, token, gruppe_name, stimmung, wl, komm, text]
                )
                row_count += 1

    db.close()
    print(f"Seed: {row_count} Pulse-Checks, {sum(len(c['pseudonyme']) for c in GRUPPEN.values())} Teilnehmer.")


if __name__ == "__main__":
    seed()
