import streamlit as st
import duckdb
import motherduck  # noqa: F401 — registers md: protocol with duckdb
import hashlib
import smtplib
import pandas as pd
from email.mime.text import MIMEText
from datetime import datetime, timedelta

MIN_GROUP_SIZE = 3
EMOJI_MAP = {1: "😞", 2: "😕", 3: "😐", 4: "🙂", 5: "😄"}


def secret(key, default=""):
    try:
        return st.secrets[key]
    except (KeyError, FileNotFoundError):
        return default


def get_db():
    if "db" not in st.session_state:
        token = secret("MOTHERDUCK_TOKEN")
        if token:
            st.session_state.db = duckdb.connect(f"md:stimmung?motherduck_token={token}")
        else:
            st.session_state.db = duckdb.connect("stimmung_local.duckdb")
        _init_schema(st.session_state.db)
    return st.session_state.db


def _init_schema(db):
    db.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_pulse START 1
    """)
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


def hash_pseudo(pseudo):
    return hashlib.sha256(pseudo.strip().encode()).hexdigest()


def admin_check():
    pw = secret("ADMIN_PASS")
    if not pw:
        st.error("ADMIN_PASS nicht konfiguriert.")
        return False
    if "admin_ok" not in st.session_state:
        st.session_state.admin_ok = False
    if st.session_state.admin_ok:
        return True
    entered = st.text_input("Admin-Passwort", type="password")
    if st.button("Anmelden"):
        if entered == pw:
            st.session_state.admin_ok = True
            st.rerun()
        else:
            st.error("Falsches Passwort.")
    return False


def page_checkin():
    st.title("📋 Stimmungs-Check-In")
    db = get_db()
    params = st.query_params
    gruppe_param = params.get("gruppe", "")
    pseudo_param = params.get("pseudo", "")

    gruppen = [r[0] for r in db.execute("SELECT DISTINCT gruppe FROM teilnehmer ORDER BY gruppe").fetchall()]
    if gruppe_param and gruppe_param in gruppen:
        gruppe = gruppe_param
        st.info(f"Gruppe: **{gruppe}**")
    elif gruppen:
        gruppe = st.selectbox("Gruppe", gruppen)
    else:
        gruppe = st.text_input("Gruppe")

    if pseudo_param:
        st.info(f"Pseudonym: **{pseudo_param}**")
        pseudo = pseudo_param
    else:
        pseudo = st.text_input("Dein Pseudonym")

    if not pseudo or not gruppe:
        st.warning("Bitte Pseudonym und Gruppe angeben.")
        return

    st.divider()
    stimmung = st.slider(
        "Wie ist deine Stimmung?",
        min_value=1, max_value=5, value=3,
        format="%d",
        help="1 = schlecht, 5 = super"
    )
    st.markdown(f"### {EMOJI_MAP[stimmung]}")

    workload = st.radio("Wie ist dein Workload?", ["Zu wenig", "Passt", "Zu viel"], index=1, horizontal=True)
    wl_map = {"Zu wenig": "zu_wenig", "Passt": "passt", "Zu viel": "zu_viel"}

    kommunikation = st.slider(
        "Wie gut ist die Kommunikation im Team?",
        min_value=1, max_value=5, value=3,
        format="%d",
        help="1 = schlecht, 5 = super"
    )

    freitext = st.text_area("Möchtest du noch etwas loswerden? (optional)")

    if st.button("Absenden", type="primary"):
        token = hash_pseudo(pseudo)
        db.execute(
            """INSERT INTO pulse_checks (anon_token, gruppe, stimmung, workload, kommunikation, freitext)
               VALUES (?, ?, ?, ?, ?, ?)""",
            [token, gruppe, stimmung, wl_map[workload], kommunikation, freitext or None]
        )
        st.success("Danke für dein Feedback! 🎉")
        st.balloons()


def page_gruppen_dashboard():
    st.title("📊 Gruppen-Dashboard")
    db = get_db()
    params = st.query_params
    gruppe_param = params.get("gruppe", "")

    gruppen = [r[0] for r in db.execute("SELECT DISTINCT gruppe FROM pulse_checks ORDER BY gruppe").fetchall()]
    if not gruppen:
        st.info("Noch keine Daten vorhanden.")
        return

    if gruppe_param and gruppe_param in gruppen:
        idx = gruppen.index(gruppe_param)
    else:
        idx = 0
    gruppe = st.selectbox("Gruppe wählen", gruppen, index=idx)

    df = db.execute("""
        SELECT submitted_at, stimmung, kommunikation, workload, freitext
        FROM pulse_checks WHERE gruppe = ? ORDER BY submitted_at
    """, [gruppe]).fetchdf()

    if df.empty:
        st.info("Keine Daten für diese Gruppe.")
        return

    df["woche"] = pd.to_datetime(df["submitted_at"]).dt.to_period("W").dt.start_time

    weekly = df.groupby("woche").agg(
        stimmung_avg=("stimmung", "mean"),
        kommunikation_avg=("kommunikation", "mean"),
        count=("stimmung", "count")
    ).reset_index()

    col1, col2, col3 = st.columns(3)
    current = weekly.iloc[-1] if len(weekly) > 0 else None
    previous = weekly.iloc[-2] if len(weekly) > 1 else None

    with col1:
        delta = round(current["stimmung_avg"] - previous["stimmung_avg"], 2) if previous is not None else None
        st.metric("Ø Stimmung", f"{current['stimmung_avg']:.1f}", delta=delta)
    with col2:
        delta = round(current["kommunikation_avg"] - previous["kommunikation_avg"], 2) if previous is not None else None
        st.metric("Ø Kommunikation", f"{current['kommunikation_avg']:.1f}", delta=delta)
    with col3:
        st.metric("Antworten (letzte Woche)", int(current["count"]))

    st.subheader("Stimmungsverlauf")
    chart_data = weekly.set_index("woche")[["stimmung_avg"]].rename(columns={"stimmung_avg": "Stimmung"})
    st.line_chart(chart_data)

    st.subheader("Kommunikationsverlauf")
    chart_data = weekly.set_index("woche")[["kommunikation_avg"]].rename(columns={"kommunikation_avg": "Kommunikation"})
    st.line_chart(chart_data)

    st.subheader("Workload-Verteilung")
    wl_counts = df["workload"].value_counts().rename(index={"zu_wenig": "Zu wenig", "passt": "Passt", "zu_viel": "Zu viel"})
    st.bar_chart(wl_counts)

    st.subheader("Freitext-Kommentare (letzte 2 Wochen)")
    cutoff = datetime.now() - timedelta(weeks=2)
    recent = df[pd.to_datetime(df["submitted_at"]) >= cutoff]
    comments = recent[recent["freitext"].notna() & (recent["freitext"] != "")]["freitext"].tolist()
    if comments:
        for c in comments:
            st.markdown(f"- {c}")
    else:
        st.info("Keine Kommentare in den letzten 2 Wochen.")


def page_gesamt_dashboard():
    st.title("🔒 Gesamt-Dashboard")
    if not admin_check():
        return

    db = get_db()
    df = db.execute("""
        SELECT gruppe, submitted_at, stimmung, kommunikation, workload, freitext
        FROM pulse_checks ORDER BY submitted_at
    """).fetchdf()

    if df.empty:
        st.info("Noch keine Daten vorhanden.")
        return

    df["submitted_at"] = pd.to_datetime(df["submitted_at"])

    gruppe_sizes = df.groupby("gruppe")["stimmung"].count()
    kleine_gruppen = gruppe_sizes[gruppe_sizes < MIN_GROUP_SIZE].index.tolist()
    df["gruppe_display"] = df["gruppe"].apply(lambda g: "Sonstige" if g in kleine_gruppen else g)

    st.subheader("Ø Stimmung pro Gruppe")
    avg_by_group = df.groupby("gruppe_display")["stimmung"].mean().sort_values()
    st.bar_chart(avg_by_group)

    st.subheader("Heatmap: Gruppe × Kalenderwoche")
    df["kw"] = df["submitted_at"].dt.isocalendar().week.astype(int)
    df["jahr"] = df["submitted_at"].dt.year
    df["kw_label"] = df["jahr"].astype(str) + "-KW" + df["kw"].astype(str).str.zfill(2)
    heatmap_data = df.groupby(["gruppe_display", "kw_label"])["stimmung"].mean().reset_index()
    heatmap_pivot = heatmap_data.pivot(index="gruppe_display", columns="kw_label", values="stimmung")
    heatmap_pivot = heatmap_pivot[sorted(heatmap_pivot.columns)]

    styled = heatmap_pivot.style.background_gradient(cmap="RdYlGn", vmin=1, vmax=5).format("{:.1f}")
    st.dataframe(styled, use_container_width=True)

    st.subheader("Welche Gruppe braucht Aufmerksamkeit?")
    df["woche"] = df["submitted_at"].dt.to_period("W").dt.start_time
    weekly_group = df.groupby(["gruppe_display", "woche"])["stimmung"].mean().reset_index()
    trends = {}
    for g in weekly_group["gruppe_display"].unique():
        gdata = weekly_group[weekly_group["gruppe_display"] == g].sort_values("woche")
        if len(gdata) >= 2:
            trends[g] = gdata["stimmung"].iloc[-1] - gdata["stimmung"].iloc[-2]
    if trends:
        worst = min(trends, key=trends.get)
        delta = trends[worst]
        if delta < 0:
            st.warning(f"⚠️ **{worst}** zeigt den stärksten Abwärtstrend (Δ {delta:.2f})")
        else:
            st.success("Alle Gruppen stabil oder im Aufwärtstrend.")

    st.subheader("Teilnehmerzahl & Antwortrate")
    teilnehmer_df = db.execute("SELECT gruppe, COUNT(*) as total FROM teilnehmer WHERE active = true GROUP BY gruppe").fetchdf()
    if not teilnehmer_df.empty:
        letzte_woche = datetime.now() - timedelta(weeks=1)
        antworten = df[df["submitted_at"] >= letzte_woche].groupby("gruppe")["stimmung"].count().reset_index()
        antworten.columns = ["gruppe", "antworten"]
        merged = teilnehmer_df.merge(antworten, on="gruppe", how="left").fillna(0)
        merged["antworten"] = merged["antworten"].astype(int)
        merged["rate"] = (merged["antworten"] / merged["total"] * 100).round(0).astype(int).astype(str) + "%"
        st.dataframe(merged.rename(columns={"gruppe": "Gruppe", "total": "Teilnehmer", "antworten": "Antworten", "rate": "Rate"}), hide_index=True)
    else:
        st.info("Keine Teilnehmer in der Verwaltung hinterlegt.")

    st.subheader("Freitext-Kommentare (alle Gruppen, letzte 2 Wochen)")
    cutoff = datetime.now() - timedelta(weeks=2)
    recent = df[df["submitted_at"] >= cutoff]
    comments = recent[recent["freitext"].notna() & (recent["freitext"] != "")]["freitext"].tolist()
    if comments:
        for c in comments:
            st.markdown(f"- {c}")
    else:
        st.info("Keine Kommentare.")


def page_verwaltung():
    st.title("⚙️ Verwaltung")
    if not admin_check():
        return

    db = get_db()

    st.subheader("Teilnehmer hinzufügen")
    with st.form("add_teilnehmer"):
        col1, col2, col3 = st.columns(3)
        with col1:
            pseudo = st.text_input("Pseudonym")
        with col2:
            gruppe = st.text_input("Gruppe")
        with col3:
            email = st.text_input("E-Mail")
        if st.form_submit_button("Hinzufügen"):
            if pseudo and gruppe and email:
                try:
                    db.execute(
                        "INSERT INTO teilnehmer (pseudo, gruppe, email) VALUES (?, ?, ?)",
                        [pseudo.strip(), gruppe.strip(), email.strip()]
                    )
                    st.success(f"**{pseudo}** zur Gruppe **{gruppe}** hinzugefügt.")
                    st.rerun()
                except duckdb.ConstraintException:
                    st.error("Teilnehmer existiert bereits in dieser Gruppe.")
            else:
                st.warning("Alle Felder ausfüllen.")

    st.subheader("Teilnehmer-Liste")
    teilnehmer = db.execute("SELECT pseudo, gruppe, email, active FROM teilnehmer ORDER BY gruppe, pseudo").fetchdf()
    if teilnehmer.empty:
        st.info("Keine Teilnehmer vorhanden.")
    else:
        for gruppe_name in sorted(teilnehmer["gruppe"].unique()):
            with st.expander(f"Gruppe: {gruppe_name}", expanded=True):
                gt = teilnehmer[teilnehmer["gruppe"] == gruppe_name]
                for _, row in gt.iterrows():
                    col1, col2, col3 = st.columns([3, 3, 1])
                    status = "✅" if row["active"] else "❌"
                    col1.write(f"{status} {row['pseudo']}")
                    col2.write(row["email"])
                    if row["active"]:
                        if col3.button("Deaktivieren", key=f"deact_{row['pseudo']}_{row['gruppe']}"):
                            db.execute(
                                "UPDATE teilnehmer SET active = false WHERE pseudo = ? AND gruppe = ?",
                                [row["pseudo"], row["gruppe"]]
                            )
                            st.rerun()

    st.subheader("Reminder senden")
    reminder_gruppen = [r[0] for r in db.execute("SELECT DISTINCT gruppe FROM teilnehmer WHERE active = true ORDER BY gruppe").fetchall()]
    if not reminder_gruppen:
        st.info("Keine aktiven Teilnehmer vorhanden.")
        return

    sel_gruppe = st.selectbox("Gruppe für Reminder", reminder_gruppen)

    last_sent = db.execute(
        "SELECT sent_at, count FROM reminder_log WHERE gruppe = ? ORDER BY sent_at DESC LIMIT 1",
        [sel_gruppe]
    ).fetchone()
    if last_sent:
        st.caption(f"Letzter Reminder: {last_sent[0].strftime('%d.%m.%Y %H:%M')} ({last_sent[1]} Mails)")

    if st.button("📧 Reminder jetzt senden", type="primary"):
        gmail_user = secret("GMAIL_USER")
        gmail_pass = secret("GMAIL_APP_PASS")
        app_url = secret("APP_URL")
        if not all([gmail_user, gmail_pass, app_url]):
            st.error("GMAIL_USER, GMAIL_APP_PASS und APP_URL müssen in secrets konfiguriert sein.")
            return

        empfaenger = db.execute(
            "SELECT pseudo, email FROM teilnehmer WHERE gruppe = ? AND active = true",
            [sel_gruppe]
        ).fetchall()

        sent_count = 0
        errors = []
        progress = st.progress(0)
        try:
            smtp = smtplib.SMTP_SSL("smtp.gmail.com", 465)
            smtp.login(gmail_user, gmail_pass)
            for i, (pseudo, email) in enumerate(empfaenger):
                link = f"{app_url}?gruppe={sel_gruppe}&pseudo={pseudo}"
                body = (
                    f"Hallo {pseudo},\n\n"
                    f"es ist wieder Zeit für deinen wöchentlichen Stimmungs-Check!\n\n"
                    f"Klick einfach auf den Link — dauert nur 30 Sekunden:\n{link}\n\n"
                    f"Danke und schöne Woche!\n"
                    f"Dein Stimmungsbarometer-Team"
                )
                msg = MIMEText(body, "plain", "utf-8")
                msg["Subject"] = "Dein wöchentlicher Stimmungs-Check"
                msg["From"] = gmail_user
                msg["To"] = email
                try:
                    smtp.sendmail(gmail_user, email, msg.as_string())
                    sent_count += 1
                except Exception as e:
                    errors.append(f"{email}: {e}")
                progress.progress((i + 1) / len(empfaenger))
            smtp.quit()
        except Exception as e:
            st.error(f"SMTP-Fehler: {e}")
            return

        db.execute(
            "INSERT INTO reminder_log (gruppe, count) VALUES (?, ?)",
            [sel_gruppe, sent_count]
        )
        st.success(f"✅ {sent_count} Reminder an Gruppe **{sel_gruppe}** gesendet.")
        if errors:
            for err in errors:
                st.warning(err)


PAGES = {
    "Check-In": page_checkin,
    "Gruppen-Dashboard": page_gruppen_dashboard,
    "Gesamt-Dashboard": page_gesamt_dashboard,
    "Verwaltung": page_verwaltung,
}

st.set_page_config(page_title="Stimmungsbarometer", page_icon="🌡️", layout="wide")
page = st.sidebar.radio("Navigation", list(PAGES.keys()))
PAGES[page]()
