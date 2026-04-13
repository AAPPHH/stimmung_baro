# Stimmungsbarometer

Wöchentliches Pulse-Check Tool für Teams. Streamlit + DuckDB/MotherDuck.

## Setup

### 1. Dependencies

```bash
pip install -r requirements.txt
```

### 2. Secrets konfigurieren

```bash
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
```

Werte ausfüllen:

| Key | Beschreibung |
|-----|-------------|
| `MOTHERDUCK_TOKEN` | MotherDuck API Token ([motherduck.com](https://motherduck.com)) — leer lassen für lokale DuckDB |
| `ADMIN_PASS` | Passwort für Admin-Seiten |
| `GMAIL_USER` | Gmail-Adresse für Reminder |
| `GMAIL_APP_PASS` | Gmail App-Passwort ([Anleitung](https://support.google.com/accounts/answer/185833)) |
| `APP_URL` | Öffentliche URL der App |

### 3. Testdaten laden

```bash
python seed.py
```

Erzeugt 3 Gruppen (Alpha, Beta, Gamma) mit je 5 Teilnehmern und 6 Wochen historische Daten.

Für MotherDuck:
```bash
MOTHERDUCK_TOKEN=duckdb_... python seed.py
```

### 4. App starten

```bash
streamlit run app.py
```

## Streamlit Community Cloud

1. Repo auf GitHub pushen
2. [share.streamlit.io](https://share.streamlit.io) → New App → Repo auswählen
3. Main file: `app.py`
4. Secrets in den App-Settings eintragen (gleicher Inhalt wie `secrets.toml`)

## Seiten

- **Check-In** — Teilnehmer geben ihr wöchentliches Feedback ab
- **Gruppen-Dashboard** — Verlauf und KPIs pro Gruppe
- **Gesamt-Dashboard** — Gruppenvergleich, Heatmap, Trends (Admin)
- **Verwaltung** — Teilnehmer verwalten, Reminder senden (Admin)

## Anonymisierung

Pseudonyme werden SHA256-gehasht als `anon_token` in `pulse_checks` gespeichert. Kein Klarname in den Antwortdaten.
