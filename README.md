# Stimmungsbarometer

Wöchentliches Pulse-Check Tool für Teams. Streamlit + Supabase PostgreSQL.

## Setup

### 1. Dependencies

```bash
pip install -r requirements.txt
```

### 2. Supabase Projekt erstellen

1. [supabase.com](https://supabase.com) — neues Projekt anlegen
2. Connection String kopieren: Project Settings → Database → Connection string (URI)

### 3. Secrets konfigurieren

```bash
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
```

Werte ausfüllen:

| Key | Beschreibung |
|-----|-------------|
| `DATABASE_URL` | Supabase PostgreSQL Connection String |
| `ADMIN_PASS` | Passwort für Admin-Seiten |
| `GMAIL_USER` | Gmail-Adresse für Reminder |
| `GMAIL_APP_PASS` | Gmail App-Passwort ([Anleitung](https://support.google.com/accounts/answer/185833)) |
| `APP_URL` | Öffentliche URL der App |

### 4. Testdaten laden

```bash
DATABASE_URL="postgresql://..." python seed.py
```

Erzeugt 3 Gruppen (Alpha, Beta, Gamma) mit je 5 Teilnehmern und 6 Wochen historische Daten.

### 5. App starten

```bash
streamlit run app.py
```

## Streamlit Community Cloud

1. Repo auf GitHub pushen
2. [share.streamlit.io](https://share.streamlit.io) — New App — Repo auswählen
3. Main file: `app.py`
4. Secrets in den App-Settings eintragen (gleicher Inhalt wie `secrets.toml`)

## Seiten

- **Check-In** — Teilnehmer geben ihr wöchentliches Feedback ab
- **Gruppen-Dashboard** — Verlauf und KPIs pro Gruppe
- **Gesamt-Dashboard** — Gruppenvergleich, Heatmap, Trends (Admin)
- **Verwaltung** — Teilnehmer verwalten, Reminder senden (Admin)

## Anonymisierung

Pseudonyme werden SHA256-gehasht als `anon_token` in `pulse_checks` gespeichert. Kein Klarname in den Antwortdaten.
