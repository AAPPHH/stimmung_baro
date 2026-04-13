"""
Seed-Script: Füllt die DuckDB mit realistischen Testdaten.
3 Gruppen, je 5 Pseudonyme, 6 Wochen historische Daten.
"""

import duckdb
import json
import hashlib
import random
from datetime import datetime, timedelta
from pathlib import Path

DB_PATH = Path(__file__).parent / "stimmung_baro.duckdb"

SALT = "stimmungsbarometer_2025"

GRUPPEN = {
    "Gruppe1": {
        "trend": "stabil_gut",  # 3.5-4.5
        "pseudonyme": ["Roter Falke", "Blauer Wolf", "Goldener Adler", "Stiller Luchs", "Flinker Otter"],
    },
    "Gruppe2": {
        "trend": "abwärtstrend",  # 4.0 → 2.5
        "pseudonyme": ["Wilder Panther", "Kluger Rabe", "Tapferer Bär", "Schneller Fuchs", "Mutiger Hirsch"],
    },
    "Gruppe3": {
        "trend": "mittel",  # 2.8-3.5
        "pseudonyme": ["Bunter Papagei", "Leiser Gecko", "Cooler Delfin", "Sanfter Kolibri", "Frecher Tiger"],
    },
}

FREITEXT_POSITIV = [
    "Teamarbeit läuft super, alle ziehen an einem Strang.",
    "Gute Woche, Projekte sind im Zeitplan und die Stimmung ist top.",
    "Kommunikation im Team hat sich deutlich verbessert.",
]

FREITEXT_NEUTRAL = [
    "Alles okay, nichts Besonderes zu berichten.",
    "Laufende Projekte gehen voran, ein paar Kleinigkeiten könnten besser sein.",
    "Ganz in Ordnung, manchmal etwas unklare Zuständigkeiten.",
]

FREITEXT_NEGATIV = [
    "Zu viel Druck von oben, Deadlines sind unrealistisch.",
    "Kommunikation ist schlecht, wichtige Infos kommen zu spät.",
    "Frustrierend — Entscheidungen werden ohne uns getroffen, Motivation sinkt.",
]

WORKLOAD_OPTIONS = ["zu_wenig", "passt", "zu_viel"]


def make_anon_token(name: str) -> str:
    return hashlib.sha256(f"{SALT}:{name}".encode()).hexdigest()[:12]


def stimmung_for(trend: str, week_index: int) -> int:
    """Generiert Stimmungswert (1-5) je nach Gruppentrend und Woche."""
    if trend == "stabil_gut":
        base = random.uniform(3.5, 4.5)
    elif trend == "abwärtstrend":
        # Woche 0 = vor 5 Wochen (gut), Woche 5 = aktuell (schlecht)
        base = 4.2 - (week_index * 0.35) + random.uniform(-0.3, 0.3)
    else:  # mittel
        base = random.uniform(2.8, 3.5)
    return max(1, min(5, round(base)))


def kommunikation_for(trend: str, week_index: int) -> int:
    if trend == "stabil_gut":
        base = random.uniform(3.5, 4.5)
    elif trend == "abwärtstrend":
        base = 4.0 - (week_index * 0.3) + random.uniform(-0.3, 0.3)
    else:
        base = random.uniform(2.5, 3.5)
    return max(1, min(5, round(base)))


def workload_for(trend: str, week_index: int) -> str:
    if trend == "stabil_gut":
        return random.choice(["passt", "passt", "passt", "zu_viel"])
    elif trend == "abwärtstrend":
        weights = ["passt"] * max(1, 4 - week_index) + ["zu_viel"] * min(4, 1 + week_index)
        return random.choice(weights)
    else:
        return random.choice(WORKLOAD_OPTIONS)


def freitext_for(stimmung: int) -> str:
    if stimmung >= 4:
        return random.choice(FREITEXT_POSITIV)
    elif stimmung >= 3:
        return random.choice(FREITEXT_NEUTRAL)
    else:
        return random.choice(FREITEXT_NEGATIV)


def sentiment_for(stimmung: int) -> tuple[float, str]:
    if stimmung >= 4:
        score = round(random.uniform(0.4, 0.9), 2)
        return score, "positiv"
    elif stimmung >= 3:
        score = round(random.uniform(-0.2, 0.3), 2)
        return score, "neutral"
    else:
        score = round(random.uniform(-0.8, -0.2), 2)
        return score, "negativ"


THEMEN_MAP = {
    "positiv": [["Teamwork", "Motivation"], ["Projektfortschritt"], ["Kommunikation", "Zusammenhalt"]],
    "neutral": [["Prozesse"], ["Zuständigkeiten", "Organisation"], ["Alltag"]],
    "negativ": [["Deadlines", "Druck"], ["Kommunikation", "Frustration"], ["Führung", "Motivation"]],
}


def themen_for(sentiment_label: str) -> list[str]:
    return random.choice(THEMEN_MAP[sentiment_label])


def zusammenfassung_for(stimmung: int) -> str:
    if stimmung >= 4:
        return random.choice([
            "Positive Stimmung, Team läuft gut.",
            "Zufriedenheit mit Teamarbeit und Fortschritt.",
        ])
    elif stimmung >= 3:
        return random.choice([
            "Neutrale Lage, kleinere Verbesserungspotenziale.",
            "Alles im Rahmen, keine größeren Probleme.",
        ])
    else:
        return random.choice([
            "Unzufriedenheit mit Arbeitsbelastung und Kommunikation.",
            "Frustration über mangelnde Einbindung in Entscheidungen.",
        ])


def seed():
    db = duckdb.connect(str(DB_PATH))

    # Schema sicherstellen
    db.execute("CREATE SEQUENCE IF NOT EXISTS seq_pulse START 1;")
    db.execute("""
        CREATE TABLE IF NOT EXISTS pulse_checks (
            id              INTEGER PRIMARY KEY DEFAULT nextval('seq_pulse'),
            submitted_at    TIMESTAMP DEFAULT current_timestamp,
            anon_token      VARCHAR,
            gruppe          VARCHAR,
            stimmung        INTEGER,
            workload        VARCHAR,
            kommunikation   INTEGER,
            freitext        VARCHAR,
            sentiment_score FLOAT,
            sentiment_label VARCHAR,
            themen          VARCHAR,
            zusammenfassung VARCHAR
        );
    """)

    # Bestehende Testdaten löschen
    db.execute("DELETE FROM pulse_checks")

    # 6 Wochen, immer Montag
    today = datetime.now()
    # Letzten Montag finden
    last_monday = today - timedelta(days=today.weekday())
    mondays = [last_monday - timedelta(weeks=w) for w in range(5, -1, -1)]  # älteste zuerst

    row_count = 0
    for gruppe_name, config in GRUPPEN.items():
        trend = config["trend"]
        for week_idx, monday in enumerate(mondays):
            for pseudo in config["pseudonyme"]:
                # Nicht jeder füllt jede Woche aus (80% Chance)
                if random.random() > 0.85:
                    continue

                anon_token = make_anon_token(pseudo)
                stimmung = stimmung_for(trend, week_idx)
                komm = kommunikation_for(trend, week_idx)
                wl = workload_for(trend, week_idx)
                text = freitext_for(stimmung)
                sent_score, sent_label = sentiment_for(stimmung)
                themen = themen_for(sent_label)
                zusammenfassung = zusammenfassung_for(stimmung)

                # Timestamp: Montag + zufällige Uhrzeit
                ts = monday.replace(
                    hour=random.randint(8, 17),
                    minute=random.randint(0, 59),
                    second=0,
                    microsecond=0,
                )

                db.execute(
                    """INSERT INTO pulse_checks
                       (submitted_at, anon_token, gruppe, stimmung, workload, kommunikation,
                        freitext, sentiment_score, sentiment_label, themen, zusammenfassung)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [
                        ts, anon_token, gruppe_name, stimmung, wl, komm,
                        text, sent_score, sent_label,
                        json.dumps(themen, ensure_ascii=False),
                        zusammenfassung,
                    ],
                )
                row_count += 1

    db.close()
    print(f"Seed abgeschlossen: {row_count} Einträge über 6 Wochen für 3 Gruppen.")


if __name__ == "__main__":
    seed()
