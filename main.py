"""
Stimmungsbarometer — KI-gestütztes Team-Pulse-Check Tool
Monolith: Streamlit + DuckDB + Claude API
"""

import streamlit as st
import duckdb
import json
import hashlib
import os
import random
from datetime import datetime, timedelta
from pathlib import Path
from cryptography.fernet import Fernet

try:
    import plotly.express as px
    import plotly.graph_objects as go
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False


DB_PATH = Path(__file__).parent / "stimmung_baro.duckdb"
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
FERNET_KEY = os.environ.get("FERNET_KEY", "")  # base64-encoded 32-byte key
ADMIN_PASS = os.environ.get("ADMIN_PASS", "")
CHECK_INTERVAL_DAYS = 7  # wöchentlich
MIN_GROUP_SIZE = 3  # Gruppen mit weniger Teilnehmern werden anonymisiert

# ---------------------------------------------------------------------------
# Zufälliges Pseudonym
# ---------------------------------------------------------------------------
ADJEKTIVE = [
    "Roter", "Blauer", "Grüner", "Goldener", "Silberner",
    "Schneller", "Stiller", "Wilder", "Kluger", "Tapferer",
    "Bunter", "Heller", "Dunkler", "Flinker", "Sanfter",
    "Mutiger", "Cooler", "Starker", "Leiser", "Frecher",
]
TIERE = [
    "Falke", "Fuchs", "Wolf", "Bär", "Adler",
    "Luchs", "Otter", "Dachs", "Rabe", "Hirsch",
    "Delfin", "Panther", "Tiger", "Kolibri", "Pinguin",
    "Chamäleon", "Gecko", "Papagei", "Flamingo", "Eisvogel",
]


def zufalls_pseudonym() -> str:
    return f"{random.choice(ADJEKTIVE)} {random.choice(TIERE)}"


def get_fernet():
    """Returns Fernet instance or None if no key configured."""
    if not FERNET_KEY:
        return None
    return Fernet(FERNET_KEY.encode())


def get_db() -> duckdb.DuckDBPyConnection:
    """Singleton-ish DB connection per Streamlit session."""
    if "db" not in st.session_state:
        st.session_state.db = duckdb.connect(str(DB_PATH))
        _init_schema(st.session_state.db)
    return st.session_state.db


def _init_schema(db: duckdb.DuckDBPyConnection):
    db.execute("CREATE SEQUENCE IF NOT EXISTS seq_pulse START 1;")
    db.execute("""
        CREATE TABLE IF NOT EXISTS pulse_checks (
            id              INTEGER PRIMARY KEY DEFAULT nextval('seq_pulse'),
            submitted_at    TIMESTAMP DEFAULT current_timestamp,
            anon_token      VARCHAR,
            gruppe          VARCHAR,       -- Gruppenname (aus URL-Param oder Eingabe)
            -- Rohdaten
            stimmung        INTEGER,       -- 1-5
            workload        VARCHAR,       -- 'zu_wenig' | 'passt' | 'zu_viel'
            kommunikation   INTEGER,       -- 1-5
            freitext        VARCHAR,
            -- KI-generiert
            sentiment_score FLOAT,         -- -1.0 bis 1.0
            sentiment_label VARCHAR,       -- 'positiv' | 'neutral' | 'negativ'
            themen          VARCHAR,       -- JSON array of topic strings
            zusammenfassung VARCHAR
        );
    """)
    # Spalte gruppe nachträglich hinzufügen falls Tabelle schon existiert
    try:
        db.execute("ALTER TABLE pulse_checks ADD COLUMN gruppe VARCHAR")
    except duckdb.CatalogException:
        pass  # Spalte existiert bereits

    # Reminder-Subscriber Tabelle
    db.execute("""
        CREATE TABLE IF NOT EXISTS reminder_subscribers (
            id              INTEGER PRIMARY KEY DEFAULT nextval('seq_pulse'),
            anon_token      VARCHAR,
            pseudonym       VARCHAR,       -- Klartext-Pseudonym für personalisierten Link
            gruppe          VARCHAR,
            email_encrypted VARCHAR,       -- Fernet-verschlüsselt
            email_hash      VARCHAR,       -- SHA256 zum Deduplizieren
            active          BOOLEAN DEFAULT true,
            created_at      TIMESTAMP DEFAULT current_timestamp
        );
    """)
    # Spalte pseudonym nachträglich hinzufügen falls Tabelle schon existiert
    try:
        db.execute("ALTER TABLE reminder_subscribers ADD COLUMN pseudonym VARCHAR")
    except duckdb.CatalogException:
        pass

# ---------------------------------------------------------------------------
# Anonymisierung
# ---------------------------------------------------------------------------
def make_anon_token(name: str) -> str:
    """
    Einweg-Hash aus einem selbstgewählten Pseudonym.
    Ermöglicht Wiedererkennung (Trend pro Person) ohne Klarnamen.
    """
    salt = "stimmungsbarometer_2025"
    return hashlib.sha256(f"{salt}:{name}".encode()).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Claude API — Sentiment & Themen
# ---------------------------------------------------------------------------
def analyze_freitext(text: str) -> dict:
    """
    Schickt Freitext an Claude, bekommt strukturierte Analyse zurück.
    Fallback auf Dummy-Werte wenn kein API Key oder Fehler.
    """
    if not text or not text.strip():
        return {
            "sentiment_score": 0.0,
            "sentiment_label": "neutral",
            "themen": [],
            "zusammenfassung": "",
        }

    if not ANTHROPIC_API_KEY:
        st.warning("Kein ANTHROPIC_API_KEY gesetzt — Sentiment-Analyse übersprungen.")
        return {
            "sentiment_score": 0.0,
            "sentiment_label": "neutral",
            "themen": [],
            "zusammenfassung": text[:100],
        }

    import httpx

    prompt = f"""Analysiere den folgenden Team-Feedback-Text auf Deutsch.
Antworte NUR mit validem JSON, keine Erklärung, kein Markdown.

Format:
{{
    "sentiment_score": <float von -1.0 (sehr negativ) bis 1.0 (sehr positiv)>,
    "sentiment_label": "<positiv|neutral|negativ>",
    "themen": ["<Thema1>", "<Thema2>"],
    "zusammenfassung": "<1 Satz Zusammenfassung>"
}}

Text: \"{text}\""""

    try:
        resp = httpx.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 300,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=15.0,
        )
        resp.raise_for_status()
        content = resp.json()["content"][0]["text"]
        # Sicherheitshalber JSON-Fences strippen
        content = content.strip().removeprefix("```json").removesuffix("```").strip()
        return json.loads(content)
    except Exception as e:
        st.error(f"Sentiment-Analyse fehlgeschlagen: {e}")
        return {
            "sentiment_score": 0.0,
            "sentiment_label": "neutral",
            "themen": [],
            "zusammenfassung": text[:100],
        }


# ---------------------------------------------------------------------------
# Daten schreiben / lesen
# ---------------------------------------------------------------------------
def submit_pulse(
    anon_token: str,
    gruppe: str,
    stimmung: int,
    workload: str,
    kommunikation: int,
    freitext: str,
):
    db = get_db()
    analysis = analyze_freitext(freitext)

    db.execute(
        """
        INSERT INTO pulse_checks
            (anon_token, gruppe, stimmung, workload, kommunikation, freitext,
             sentiment_score, sentiment_label, themen, zusammenfassung)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            anon_token,
            gruppe or None,
            stimmung,
            workload,
            kommunikation,
            freitext,
            analysis["sentiment_score"],
            analysis["sentiment_label"],
            json.dumps(analysis.get("themen", []), ensure_ascii=False),
            analysis.get("zusammenfassung", ""),
        ],
    )


def subscribe_reminder(anon_token: str, pseudonym: str, gruppe: str, email: str):
    """Speichert verschlüsselte Email für wöchentliche Reminder."""
    fernet = get_fernet()
    if not fernet:
        st.warning("Reminder nicht verfügbar — FERNET_KEY nicht konfiguriert.")
        return

    db = get_db()
    email_hash = hashlib.sha256(email.lower().strip().encode()).hexdigest()

    # Deduplizieren: gleiche Email + Gruppe = Update statt Insert
    existing = db.execute(
        "SELECT id FROM reminder_subscribers WHERE email_hash = ? AND gruppe IS NOT DISTINCT FROM ?",
        [email_hash, gruppe or None],
    ).fetchone()

    email_encrypted = fernet.encrypt(email.strip().encode()).decode()

    if existing:
        db.execute(
            "UPDATE reminder_subscribers SET active = true, email_encrypted = ?, pseudonym = ? WHERE id = ?",
            [email_encrypted, pseudonym, existing[0]],
        )
    else:
        db.execute(
            """INSERT INTO reminder_subscribers (anon_token, pseudonym, gruppe, email_encrypted, email_hash)
               VALUES (?, ?, ?, ?, ?)""",
            [anon_token, pseudonym, gruppe or None, email_encrypted, email_hash],
        )


def _gruppe_filter(gruppe: str | None) -> tuple[str, list]:
    """Returns (WHERE clause, params) for group filtering."""
    if gruppe:
        return "WHERE gruppe = ?", [gruppe]
    return "", []


def get_weekly_avg(gruppe: str | None = None):
    db = get_db()
    where, params = _gruppe_filter(gruppe)
    return db.execute(f"""
        SELECT
            date_trunc('week', submitted_at) AS woche,
            round(avg(stimmung), 2)          AS avg_stimmung,
            round(avg(kommunikation), 2)     AS avg_kommunikation,
            round(avg(sentiment_score), 2)   AS avg_sentiment,
            count(*)                         AS anzahl
        FROM pulse_checks
        {where}
        GROUP BY date_trunc('week', submitted_at)
        ORDER BY woche
    """, params).fetchdf()


def get_workload_distribution(gruppe: str | None = None):
    db = get_db()
    where, params = _gruppe_filter(gruppe)
    return db.execute(f"""
        SELECT
            workload,
            count(*) AS anzahl
        FROM pulse_checks
        {where}
        GROUP BY workload
    """, params).fetchdf()


def get_themen_ranking(gruppe: str | None = None):
    db = get_db()
    if gruppe:
        rows = db.execute(
            "SELECT themen FROM pulse_checks WHERE gruppe = ? AND themen IS NOT NULL",
            [gruppe],
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT themen FROM pulse_checks WHERE themen IS NOT NULL"
        ).fetchall()
    counts: dict[str, int] = {}
    for (raw,) in rows:
        try:
            for t in json.loads(raw):
                counts[t] = counts.get(t, 0) + 1
        except (json.JSONDecodeError, TypeError):
            pass
    return sorted(counts.items(), key=lambda x: -x[1])


def get_recent_entries(n: int = 20, gruppe: str | None = None):
    db = get_db()
    where, params = _gruppe_filter(gruppe)
    return db.execute(f"""
        SELECT
            submitted_at,
            gruppe,
            stimmung,
            workload,
            kommunikation,
            sentiment_label,
            zusammenfassung
        FROM pulse_checks
        {where}
        ORDER BY submitted_at DESC
        LIMIT {n}
    """, params).fetchdf()


def get_total_count(gruppe: str | None = None) -> int:
    db = get_db()
    where, params = _gruppe_filter(gruppe)
    return db.execute(f"SELECT count(*) FROM pulse_checks {where}", params).fetchone()[0]


def get_all_gruppen() -> list[str]:
    db = get_db()
    rows = db.execute(
        "SELECT DISTINCT gruppe FROM pulse_checks WHERE gruppe IS NOT NULL ORDER BY gruppe"
    ).fetchall()
    return [r[0] for r in rows]


def get_reminder_count(gruppe: str | None = None) -> int:
    db = get_db()
    if gruppe:
        return db.execute(
            "SELECT count(*) FROM reminder_subscribers WHERE active = true AND gruppe = ?",
            [gruppe],
        ).fetchone()[0]
    return db.execute(
        "SELECT count(*) FROM reminder_subscribers WHERE active = true"
    ).fetchone()[0]


def get_kpis(gruppe: str | None = None):
    """Returns (avg_stimmung, avg_komm, avg_sentiment, unique_users) for given group filter."""
    db = get_db()
    where, params = _gruppe_filter(gruppe)
    return db.execute(f"""
        SELECT
            round(avg(stimmung), 1)        AS avg_stimmung,
            round(avg(kommunikation), 1)   AS avg_komm,
            round(avg(sentiment_score), 2) AS avg_sentiment,
            count(DISTINCT anon_token)      AS unique_users
        FROM pulse_checks
        {where}
    """, params).fetchone()


def get_previous_week_kpis(gruppe: str | None = None):
    """Returns KPIs for the week before the most recent week, for delta calculation."""
    db = get_db()
    if gruppe:
        return db.execute("""
            WITH current_week AS (
                SELECT date_trunc('week', max(submitted_at)) AS latest_week
                FROM pulse_checks WHERE gruppe = ?
            )
            SELECT
                round(avg(stimmung), 1)        AS avg_stimmung,
                round(avg(kommunikation), 1)   AS avg_komm,
                round(avg(sentiment_score), 2) AS avg_sentiment,
                count(DISTINCT anon_token)      AS unique_users
            FROM pulse_checks, current_week
            WHERE gruppe = ?
              AND date_trunc('week', submitted_at) = current_week.latest_week - INTERVAL 7 DAY
        """, [gruppe, gruppe]).fetchone()
    else:
        return db.execute("""
            WITH current_week AS (
                SELECT date_trunc('week', max(submitted_at)) AS latest_week
                FROM pulse_checks
            )
            SELECT
                round(avg(stimmung), 1)        AS avg_stimmung,
                round(avg(kommunikation), 1)   AS avg_komm,
                round(avg(sentiment_score), 2) AS avg_sentiment,
                count(DISTINCT anon_token)      AS unique_users
            FROM pulse_checks, current_week
            WHERE date_trunc('week', submitted_at) = current_week.latest_week - INTERVAL 7 DAY
        """).fetchone()


def get_group_stats():
    """Returns per-group aggregated stats for admin dashboard."""
    db = get_db()
    return db.execute("""
        SELECT
            gruppe,
            round(avg(stimmung), 2)          AS avg_stimmung,
            round(avg(kommunikation), 2)     AS avg_kommunikation,
            round(avg(sentiment_score), 2)   AS avg_sentiment,
            count(DISTINCT anon_token)        AS teilnehmer,
            count(*)                          AS anzahl_checkins
        FROM pulse_checks
        WHERE gruppe IS NOT NULL
        GROUP BY gruppe
        ORDER BY gruppe
    """).fetchdf()


def get_group_weekly_heatmap():
    """Returns gruppe × woche × avg_stimmung for heatmap."""
    db = get_db()
    return db.execute("""
        SELECT
            gruppe,
            date_trunc('week', submitted_at) AS woche,
            round(avg(stimmung), 2)          AS avg_stimmung,
            count(DISTINCT anon_token)        AS teilnehmer
        FROM pulse_checks
        WHERE gruppe IS NOT NULL
        GROUP BY gruppe, date_trunc('week', submitted_at)
        ORDER BY woche, gruppe
    """).fetchdf()


def get_group_trend():
    """Returns the two most recent weeks per group for trend detection."""
    db = get_db()
    return db.execute("""
        WITH ranked AS (
            SELECT
                gruppe,
                date_trunc('week', submitted_at) AS woche,
                round(avg(stimmung), 2)          AS avg_stimmung,
                count(DISTINCT anon_token)        AS teilnehmer,
                ROW_NUMBER() OVER (PARTITION BY gruppe ORDER BY date_trunc('week', submitted_at) DESC) AS rn
            FROM pulse_checks
            WHERE gruppe IS NOT NULL
            GROUP BY gruppe, date_trunc('week', submitted_at)
        )
        SELECT gruppe, woche, avg_stimmung, teilnehmer, rn
        FROM ranked
        WHERE rn <= 2
        ORDER BY gruppe, rn
    """).fetchdf()


def _stimmung_badge(value: float) -> tuple[str, str]:
    """Returns (label, color) badge based on stimmung value."""
    if value >= 4.0:
        return "Sehr gut", "green"
    elif value >= 3.0:
        return "Stabil", "blue"
    elif value >= 2.0:
        return "Achtung", "orange"
    else:
        return "Kritisch", "red"


def _generate_export_parquet():
    """Generates parquet export on demand."""
    db = get_db()
    export_path = Path(__file__).parent / "export_pulse_checks.parquet"
    db.execute(f"COPY pulse_checks TO '{export_path}' (FORMAT PARQUET)")
    with open(export_path, "rb") as f:
        return f.read()


# ---------------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------------
def main():
    st.set_page_config(
        page_title="Stimmungsbarometer",
        page_icon="🌡️",
        layout="wide",
    )

    st.title("🌡️ Team-Stimmungsbarometer")

    # Sidebar Navigation
    page = st.sidebar.radio(
        "Navigation",
        ["📝 Check-In", "📊 Gruppen-Dashboard", "🏢 Gesamt-Dashboard", "📋 Einträge"],
    )

    if page == "📝 Check-In":
        page_checkin()
    elif page == "📊 Gruppen-Dashboard":
        page_gruppen_dashboard()
    elif page == "🏢 Gesamt-Dashboard":
        page_gesamt_dashboard()
    elif page == "📋 Einträge":
        page_entries()


# ----- Check-In Seite -----
def page_checkin():
    st.header("Wöchentlicher Pulse-Check")
    st.caption("Alle Angaben sind anonym. Dein Pseudonym wird gehasht gespeichert.")

    # URL-Parameter lesen
    params = st.query_params
    gruppe_from_url = params.get("gruppe", "")
    pseudo_from_url = params.get("pseudo", "")

    # Pseudonym: aus URL > Session-Zufall > neu generieren
    if pseudo_from_url:
        default_pseudonym = pseudo_from_url
    else:
        if "random_pseudonym" not in st.session_state:
            st.session_state.random_pseudonym = zufalls_pseudonym()
        default_pseudonym = st.session_state.random_pseudonym

    with st.form("pulse_form"):
        gruppe = st.text_input(
            "Gruppe",
            value=gruppe_from_url,
            placeholder="z.B. Team-Backend",
            help="Wird automatisch aus dem URL-Parameter ?gruppe=X befüllt",
        )

        pseudonym = st.text_input(
            "Dein Pseudonym (frei wählbar, jede Woche dasselbe nutzen)",
            value=default_pseudonym,
            help="Aus deinem persönlichen Link oder zufällig vorgeschlagen",
            disabled=bool(pseudo_from_url),  # Wenn aus URL, nicht änderbar
        )

        st.divider()

        stimmung = st.slider(
            "🎯 Allgemeine Stimmung",
            min_value=1, max_value=5, value=3,
            help="1 = sehr schlecht, 5 = super",
        )

        workload = st.radio(
            "⚖️ Workload-Empfinden",
            options=["zu_wenig", "passt", "zu_viel"],
            format_func=lambda x: {
                "zu_wenig": "📉 Zu wenig",
                "passt": "✅ Passt",
                "zu_viel": "🔥 Zu viel",
            }[x],
            horizontal=True,
        )

        kommunikation = st.slider(
            "💬 Teamkommunikation",
            min_value=1, max_value=5, value=3,
            help="1 = sehr schlecht, 5 = exzellent",
        )

        freitext = st.text_area(
            "📝 Was beschäftigt dich? (optional)",
            placeholder="Hier kannst du frei schreiben...",
            height=100,
        )

        submitted = st.form_submit_button("✅ Absenden", use_container_width=True)

    # Reminder-Sektion (außerhalb des Formulars)
    st.divider()
    st.subheader("Wöchentliche Erinnerung")
    want_reminder = st.checkbox("Willst du wöchentlich an den Check-In erinnert werden?")
    reminder_email = ""
    if want_reminder:
        reminder_email = st.text_input(
            "Deine Email-Adresse",
            placeholder="wird verschlüsselt gespeichert",
            help="Deine Email wird mit AES verschlüsselt gespeichert und ist im Dashboard nicht sichtbar.",
        )

    if submitted:
        if not pseudonym.strip():
            st.error("Bitte ein Pseudonym eingeben.")
            return

        anon_token = make_anon_token(pseudonym.strip())

        with st.spinner("Analysiere..."):
            submit_pulse(anon_token, gruppe.strip(), stimmung, workload, kommunikation, freitext)

        # Reminder speichern falls gewünscht
        if want_reminder and reminder_email.strip():
            subscribe_reminder(anon_token, pseudonym.strip(), gruppe.strip(), reminder_email.strip())

        # Persönlichen Link anzeigen
        personal_params = f"?pseudo={pseudonym.strip()}"
        if gruppe.strip():
            personal_params += f"&gruppe={gruppe.strip()}"
        st.success("Danke! Dein Check-In wurde gespeichert. 🎉")
        st.info(f"Dein persönlicher Link für nächste Woche: `{personal_params}`")

        # Neues Pseudonym für nächsten Check-In generieren (nur wenn keins aus URL)
        if not pseudo_from_url:
            st.session_state.random_pseudonym = zufalls_pseudonym()


# ----- Gruppen-Dashboard -----
def page_gruppen_dashboard():
    st.header("📊 Gruppen-Dashboard")

    # Gruppe aus URL-Parameter oder Dropdown
    params = st.query_params
    gruppe_from_url = params.get("gruppe", "")

    gruppen = get_all_gruppen()
    if not gruppen:
        st.info("Noch keine Gruppen vorhanden. Starte mit dem ersten Check-In!")
        return

    # Wenn Gruppe aus URL, vorauswählen
    default_index = 0
    if gruppe_from_url and gruppe_from_url in gruppen:
        default_index = gruppen.index(gruppe_from_url)

    aktive_gruppe = st.selectbox(
        "Gruppe auswählen",
        gruppen,
        index=default_index,
        key="gruppen_dashboard_select",
    )

    total = get_total_count(aktive_gruppe)
    if total == 0:
        st.info(f"Noch keine Daten für Gruppe '{aktive_gruppe}'.")
        return

    # KPIs mit Delta zur Vorwoche
    kpis = get_kpis(aktive_gruppe)
    prev_kpis = get_previous_week_kpis(aktive_gruppe)

    stimmung_delta = None
    komm_delta = None
    if prev_kpis and prev_kpis[0] is not None and kpis[0] is not None:
        stimmung_delta = round(float(kpis[0]) - float(prev_kpis[0]), 1)
    if prev_kpis and prev_kpis[1] is not None and kpis[1] is not None:
        komm_delta = round(float(kpis[1]) - float(prev_kpis[1]), 1)

    reminder_count = get_reminder_count(aktive_gruppe)
    label, color = _stimmung_badge(float(kpis[0]) if kpis[0] else 0)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric(
        "Ø Stimmung",
        f"{kpis[0]}/5" if kpis[0] else "—",
        delta=f"{stimmung_delta:+.1f} vs. Vorwoche" if stimmung_delta is not None else None,
    )
    col2.metric(
        "Ø Kommunikation",
        f"{kpis[1]}/5" if kpis[1] else "—",
        delta=f"{komm_delta:+.1f} vs. Vorwoche" if komm_delta is not None else None,
    )
    col3.metric("Teilnehmer", kpis[3] if kpis[3] else 0)
    col4.metric("Reminder aktiv", reminder_count)

    # Status-Badge
    st.badge(label, icon=":material/trending_up:" if color == "green" else ":material/warning:" if color in ("orange", "red") else ":material/check_circle:", color=color)

    st.divider()

    # Tabs für Charts
    tab_verlauf, tab_workload, tab_themen = st.tabs(["📈 Stimmungsverlauf", "⚖️ Workload", "💡 Themen"])

    with tab_verlauf:
        weekly = get_weekly_avg(aktive_gruppe)
        if not weekly.empty:
            if HAS_PLOTLY:
                fig = px.line(
                    weekly,
                    x="woche",
                    y=["avg_stimmung", "avg_kommunikation"],
                    labels={"value": "Durchschnitt", "woche": "Woche", "variable": "Metrik"},
                    title=f"Stimmungsverlauf — {aktive_gruppe}",
                    markers=True,
                )
                fig.update_layout(yaxis_range=[1, 5], hovermode="x unified")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.line_chart(
                    weekly.set_index("woche")[["avg_stimmung", "avg_kommunikation"]],
                    use_container_width=True,
                )
        else:
            st.caption("Noch nicht genug Daten für einen Verlauf.")

    with tab_workload:
        wl = get_workload_distribution(aktive_gruppe)
        if not wl.empty:
            if HAS_PLOTLY:
                color_map = {"zu_wenig": "#3b82f6", "passt": "#22c55e", "zu_viel": "#ef4444"}
                fig = px.bar(
                    wl,
                    x="workload",
                    y="anzahl",
                    color="workload",
                    color_discrete_map=color_map,
                    labels={"workload": "Workload", "anzahl": "Anzahl"},
                    title="Workload-Verteilung",
                )
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.bar_chart(wl.set_index("workload"))
        else:
            st.caption("Noch keine Workload-Daten.")

    with tab_themen:
        themen = get_themen_ranking(aktive_gruppe)
        if themen:
            for topic, count in themen[:10]:
                st.write(f"**{topic}** — {count}x genannt")
        else:
            st.caption("Noch keine Themen erkannt.")


# ----- Gesamt-Dashboard (Admin) -----
def page_gesamt_dashboard():
    st.header("🏢 Gesamt-Dashboard")

    # Admin-Authentifizierung
    if not ADMIN_PASS:
        st.error("ADMIN_PASS Umgebungsvariable nicht konfiguriert.")
        return

    if "admin_authenticated" not in st.session_state:
        st.session_state.admin_authenticated = False

    if not st.session_state.admin_authenticated:
        st.caption("Dieses Dashboard zeigt aggregierte Daten über alle Gruppen.")
        password = st.text_input("Admin-Passwort", type="password", key="admin_pw")
        if st.button("Anmelden"):
            if password == ADMIN_PASS:
                st.session_state.admin_authenticated = True
                st.rerun()
            else:
                st.error("Falsches Passwort.")
        return

    # --- Ab hier: authentifiziert ---
    total = get_total_count()
    if total == 0:
        st.info("Noch keine Daten vorhanden.")
        return

    # Globale KPIs mit Delta
    kpis = get_kpis()
    prev_kpis = get_previous_week_kpis()

    stimmung_delta = None
    komm_delta = None
    if prev_kpis and prev_kpis[0] is not None and kpis[0] is not None:
        stimmung_delta = round(float(kpis[0]) - float(prev_kpis[0]), 1)
    if prev_kpis and prev_kpis[1] is not None and kpis[1] is not None:
        komm_delta = round(float(kpis[1]) - float(prev_kpis[1]), 1)

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric(
        "Ø Stimmung (gesamt)",
        f"{kpis[0]}/5" if kpis[0] else "—",
        delta=f"{stimmung_delta:+.1f}" if stimmung_delta is not None else None,
    )
    col2.metric(
        "Ø Kommunikation",
        f"{kpis[1]}/5" if kpis[1] else "—",
        delta=f"{komm_delta:+.1f}" if komm_delta is not None else None,
    )
    col3.metric("Ø Sentiment", f"{kpis[2]:.2f}" if kpis[2] is not None else "—")
    col4.metric("Teilnehmer gesamt", kpis[3] if kpis[3] else 0)
    col5.metric("Check-Ins gesamt", total)

    st.divider()

    # Tabs
    tab_vergleich, tab_heatmap, tab_attention, tab_themen = st.tabs([
        "📊 Gruppenvergleich", "🗺️ Heatmap", "⚠️ Aufmerksamkeit", "💡 Top-Themen"
    ])

    # --- Gruppenvergleich ---
    with tab_vergleich:
        st.subheader("Ø Stimmung pro Gruppe")
        group_stats = get_group_stats()
        if group_stats.empty:
            st.caption("Keine Gruppendaten vorhanden.")
        else:
            # Anonymisierung: Gruppen < MIN_GROUP_SIZE unter "Sonstige" zusammenfassen
            display_df = _anonymize_small_groups(group_stats)

            if HAS_PLOTLY:
                fig = px.bar(
                    display_df,
                    x="gruppe",
                    y="avg_stimmung",
                    color="avg_stimmung",
                    color_continuous_scale=["#ef4444", "#f59e0b", "#22c55e"],
                    range_color=[1, 5],
                    labels={"gruppe": "Gruppe", "avg_stimmung": "Ø Stimmung"},
                    title="Stimmung pro Gruppe",
                )
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.bar_chart(display_df.set_index("gruppe")["avg_stimmung"])

            # Badges pro Gruppe
            for _, row in display_df.iterrows():
                label, color = _stimmung_badge(float(row["avg_stimmung"]))
                col_name, col_badge, col_stats = st.columns([2, 1, 3])
                col_name.write(f"**{row['gruppe']}**")
                col_badge.badge(label, color=color)
                col_stats.caption(f"{int(row['teilnehmer'])} Teilnehmer · {int(row['anzahl_checkins'])} Check-Ins")

    # --- Heatmap ---
    with tab_heatmap:
        st.subheader("Stimmungs-Heatmap (Gruppe × Woche)")
        heatmap_data = get_group_weekly_heatmap()
        if heatmap_data.empty:
            st.caption("Noch nicht genug Daten für eine Heatmap.")
        else:
            # Anonymisierung: kleine Gruppen ausblenden
            heatmap_data = _anonymize_heatmap(heatmap_data)

            if HAS_PLOTLY:
                heatmap_data["woche_str"] = heatmap_data["woche"].dt.strftime("%d.%m.%y")
                pivot = heatmap_data.pivot_table(
                    index="gruppe",
                    columns="woche_str",
                    values="avg_stimmung",
                    aggfunc="mean",
                )
                fig = go.Figure(data=go.Heatmap(
                    z=pivot.values,
                    x=pivot.columns.tolist(),
                    y=pivot.index.tolist(),
                    colorscale=[[0, "#ef4444"], [0.5, "#f59e0b"], [1, "#22c55e"]],
                    zmin=1,
                    zmax=5,
                    text=pivot.values.round(1),
                    texttemplate="%{text}",
                    hovertemplate="Gruppe: %{y}<br>Woche: %{x}<br>Stimmung: %{z:.1f}<extra></extra>",
                ))
                fig.update_layout(
                    title="Stimmung pro Gruppe und Woche",
                    xaxis_title="Woche",
                    yaxis_title="Gruppe",
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.dataframe(heatmap_data, use_container_width=True)

    # --- Aufmerksamkeit ---
    with tab_attention:
        st.subheader("Welche Gruppe braucht Aufmerksamkeit?")
        trend_data = get_group_trend()
        if trend_data.empty:
            st.caption("Noch nicht genug Daten für Trendanalyse.")
        else:
            alerts = _calculate_attention_alerts(trend_data)
            if not alerts:
                st.success("Alle Gruppen sind stabil — keine Auffälligkeiten.")
            else:
                for alert in alerts:
                    with st.container(border=True):
                        col_info, col_badge = st.columns([4, 1])
                        col_info.write(f"**{alert['gruppe']}**")
                        col_info.caption(alert["reason"])
                        col_badge.badge(alert["label"], color=alert["color"])

    # --- Top-Themen global ---
    with tab_themen:
        st.subheader("Top-Themen über alle Gruppen")
        themen = get_themen_ranking()
        if themen:
            if HAS_PLOTLY:
                top_themen = themen[:15]
                fig = px.bar(
                    x=[t[1] for t in top_themen],
                    y=[t[0] for t in top_themen],
                    orientation="h",
                    labels={"x": "Nennungen", "y": "Thema"},
                    title="Meistgenannte Themen",
                )
                fig.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(fig, use_container_width=True)
            else:
                for topic, count in themen[:10]:
                    st.write(f"**{topic}** — {count}x genannt")
        else:
            st.caption("Noch keine Themen erkannt.")


def _anonymize_small_groups(group_stats):
    """Fasst Gruppen mit < MIN_GROUP_SIZE Teilnehmern unter 'Sonstige' zusammen."""
    import pandas as pd

    large = group_stats[group_stats["teilnehmer"] >= MIN_GROUP_SIZE]
    small = group_stats[group_stats["teilnehmer"] < MIN_GROUP_SIZE]

    if small.empty:
        return large

    # Aggregiere kleine Gruppen
    sonstige = pd.DataFrame([{
        "gruppe": "Sonstige",
        "avg_stimmung": round(small["avg_stimmung"].mean(), 2),
        "avg_kommunikation": round(small["avg_kommunikation"].mean(), 2),
        "avg_sentiment": round(small["avg_sentiment"].mean(), 2),
        "teilnehmer": small["teilnehmer"].sum(),
        "anzahl_checkins": small["anzahl_checkins"].sum(),
    }])

    return pd.concat([large, sonstige], ignore_index=True)


def _anonymize_heatmap(heatmap_data):
    """Filtert Gruppen mit < MIN_GROUP_SIZE aus der Heatmap."""
    # Prüfe Teilnehmer pro Gruppe über alle Wochen
    group_sizes = heatmap_data.groupby("gruppe")["teilnehmer"].max()
    valid_groups = group_sizes[group_sizes >= MIN_GROUP_SIZE].index
    return heatmap_data[heatmap_data["gruppe"].isin(valid_groups)]


def _calculate_attention_alerts(trend_data):
    """Berechnet welche Gruppen Aufmerksamkeit brauchen (stärkster Abwärtstrend)."""
    alerts = []
    gruppen = trend_data["gruppe"].unique()

    for gruppe in gruppen:
        g_data = trend_data[trend_data["gruppe"] == gruppe].sort_values("rn")

        # Anonymisierung: Gruppen mit < MIN_GROUP_SIZE überspringen
        if g_data["teilnehmer"].max() < MIN_GROUP_SIZE:
            continue

        if len(g_data) < 2:
            continue

        current = float(g_data.iloc[0]["avg_stimmung"])
        previous = float(g_data.iloc[1]["avg_stimmung"])
        delta = current - previous

        if delta < -0.5:
            alerts.append({
                "gruppe": gruppe,
                "reason": f"Stimmung gefallen: {previous} → {current} ({delta:+.1f})",
                "label": "Abwärtstrend",
                "color": "red",
                "delta": delta,
            })
        elif current < 2.5:
            alerts.append({
                "gruppe": gruppe,
                "reason": f"Stimmung dauerhaft niedrig: {current}/5",
                "label": "Kritisch",
                "color": "red",
                "delta": delta,
            })
        elif delta < -0.2:
            alerts.append({
                "gruppe": gruppe,
                "reason": f"Leichter Rückgang: {previous} → {current} ({delta:+.1f})",
                "label": "Beobachten",
                "color": "orange",
                "delta": delta,
            })

    # Sortiere nach stärkstem Abwärtstrend
    alerts.sort(key=lambda x: x["delta"])
    return alerts


# ----- Einträge Seite -----
def page_entries():
    st.header("📋 Letzte Einträge")

    # Gruppen-Filter
    gruppen = get_all_gruppen()
    filter_optionen = ["Alle Gruppen"] + gruppen
    selected = st.selectbox("Gruppe filtern", filter_optionen, key="entries_gruppe")
    aktive_gruppe = selected if selected != "Alle Gruppen" else None

    entries = get_recent_entries(50, aktive_gruppe)
    if entries.empty:
        st.info("Noch keine Einträge.")
        return

    st.dataframe(
        entries,
        use_container_width=True,
        column_config={
            "submitted_at": st.column_config.DatetimeColumn("Zeitpunkt", format="DD.MM.YY HH:mm"),
            "gruppe": "Gruppe",
            "stimmung": st.column_config.ProgressColumn("Stimmung", min_value=1, max_value=5),
            "kommunikation": st.column_config.ProgressColumn("Kommunikation", min_value=1, max_value=5),
        },
    )

    # Export
    st.divider()
    st.download_button(
        label="📦 Export als Parquet",
        data=_generate_export_parquet,
        file_name="pulse_checks.parquet",
        mime="application/octet-stream",
    )


if __name__ == "__main__":
    main()
