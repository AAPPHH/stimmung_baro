import streamlit as st
import streamlit.components.v1 as components
import psycopg2
import hashlib
import smtplib
import re
import random
import pandas as pd
import plotly.graph_objects as go
from email.mime.text import MIMEText
from datetime import datetime, timedelta

MIN_GROUP_SIZE = 3
NAME_RE = re.compile(r'^[a-zA-Z0-9äöüÄÖÜß\s\-]{1,50}$')
EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')
NEW_GROUP_OPT = "➕ Neue Gruppe…"
DEMO_GRUPPE = "Demo-Team"
DEMO_TEILNEHMER = [
    ("Demo Anna", "demo.anna@example.com"),
    ("Demo Ben", "demo.ben@example.com"),
    ("Demo Chris", "demo.chris@example.com"),
    ("Demo Dana", "demo.dana@example.com"),
    ("Demo Elias", "demo.elias@example.com"),
]


def filter_out_demo(gruppen):
    return [g for g in gruppen if g != DEMO_GRUPPE]


def sort_demo_last(gruppen):
    normal = sorted([g for g in gruppen if g != DEMO_GRUPPE])
    demo = [g for g in gruppen if g == DEMO_GRUPPE]
    return normal + demo


def is_reserved_group(name):
    return name.strip().lower() == DEMO_GRUPPE.lower()

STIMMUNG_OPTIONS = [
    (1, "😞", "Schlecht"),
    (2, "😕", "Mäßig"),
    (3, "😐", "Okay"),
    (4, "🙂", "Gut"),
    (5, "😄", "Super"),
]
KOMM_OPTIONS = [
    (1, "Kaum"),
    (2, "Wenig"),
    (3, "Okay"),
    (4, "Gut"),
    (5, "Exzellent"),
]
WORKLOAD_OPTIONS = [
    ("zu_wenig", "📉", "Zu wenig"),
    ("passt", "⚖️", "Passt"),
    ("zu_viel", "📈", "Zu viel"),
]
WL_SCORE = {"zu_wenig": -1, "passt": 0, "zu_viel": 1}


def wl_label(score):
    if score <= -0.3:
        return "Unterfordert"
    if score >= 0.3:
        return "Überlastet"
    return "Ausgewogen"


def wl_color(score):
    a = abs(score)
    if a < 0.3:
        return "#10B981"
    if a < 0.6:
        return "#F59E0B"
    return "#EF4444"

CUSTOM_CSS = """
<style>
#MainMenu, footer {visibility: hidden;}
[data-testid="stHeader"] {display: none;}
[data-testid="stToolbar"] {display: none !important;}
[data-testid="stDecoration"] {display: none;}

section[data-testid="stSidebar"],
section[data-testid="stSidebar"][aria-expanded="false"] {
    transform: translateX(0) !important;
    margin-left: 0 !important;
    left: 0 !important;
    visibility: visible !important;
    min-width: 244px !important;
    width: 244px !important;
}
section[data-testid="stSidebar"] button[kind="header"],
[data-testid="stSidebarCollapseButton"],
[data-testid="stSidebarCollapsedControl"],
[data-testid="collapsedControl"] {
    display: none !important;
}

.block-container {padding-top: 2.5rem; padding-bottom: 3rem; max-width: 1200px;}

h1 {font-weight: 700; letter-spacing: -0.02em;}
h2, h3 {font-weight: 600; letter-spacing: -0.01em;}

.subtle {color: #94A3B8; font-size: 15px; margin: -8px 0 24px 0;}

.kpi-card {
    background: #1E293B; border: 1px solid #334155;
    border-radius: 14px; padding: 22px 24px;
    transition: border-color 0.2s;
}
.kpi-card:hover {border-color: #475569;}
.kpi-value {font-size: 34px; font-weight: 700; color: #F1F5F9; line-height: 1.1;}
.kpi-label {font-size: 12px; color: #94A3B8; text-transform: uppercase; letter-spacing: 0.6px; margin-top: 6px; font-weight: 600;}
.kpi-delta {font-size: 13px; margin-top: 10px; font-weight: 600;}
.kpi-delta.up {color: #10B981;}
.kpi-delta.down {color: #EF4444;}
.kpi-delta.flat {color: #64748B;}

.stButton > button {
    border-radius: 10px; padding: 10px 18px; font-weight: 500;
    transition: all 0.15s; border: 1px solid #334155; background: #1E293B;
    color: #F1F5F9;
}
.stButton > button:hover {
    transform: translateY(-1px); border-color: #475569; background: #263449;
}
.stButton > button[kind="primary"] {
    background: #2563EB; border-color: #2563EB; color: #FFFFFF;
}
.stButton > button[kind="primary"]:hover {
    background: #1D4ED8; border-color: #1D4ED8;
    box-shadow: 0 6px 18px rgba(37,99,235,0.35);
}
.stButton > button:disabled {opacity: 0.45;}

.choice-label {text-align: center; color: #94A3B8; font-size: 13px; margin: 6px 0 14px 0;}

.alert-warn {
    background: rgba(239,68,68,0.08); border: 1px solid rgba(239,68,68,0.35);
    border-radius: 10px; padding: 14px 18px; color: #FCA5A5; margin: 8px 0 20px 0;
    font-weight: 500;
}
.alert-ok {
    background: rgba(16,185,129,0.08); border: 1px solid rgba(16,185,129,0.35);
    border-radius: 10px; padding: 14px 18px; color: #6EE7B7; margin: 8px 0 20px 0;
    font-weight: 500;
}
.info-box {
    background: rgba(37,99,235,0.08); border: 1px solid rgba(37,99,235,0.3);
    border-radius: 10px; padding: 12px 16px; color: #93C5FD; margin: 8px 0 24px 0;
    font-size: 13px; line-height: 1.5;
}

.status-dot {display: inline-block; width: 9px; height: 9px; border-radius: 50%; margin-right: 8px;}
.status-dot.on {background: #10B981; box-shadow: 0 0 0 3px rgba(16,185,129,0.2);}
.status-dot.off {background: #EF4444; box-shadow: 0 0 0 3px rgba(239,68,68,0.2);}

.greeting {
    background: #1E293B; border: 1px solid #334155;
    border-radius: 12px; padding: 18px 22px; margin: 8px 0 8px 0;
    font-size: 17px; color: #F1F5F9;
}
.greeting b {color: #93C5FD;}

hr {border-color: #334155 !important; margin: 1.5rem 0 !important;}

[data-testid="stDataFrame"] {border-radius: 10px; overflow: hidden;}
</style>
"""


def secret(key, default=""):
    try:
        return st.secrets[key]
    except (KeyError, FileNotFoundError):
        return default


def valid_name(s):
    return bool(s and NAME_RE.match(s.strip()))


def valid_email(s):
    return bool(s and EMAIL_RE.match(s.strip()))


@st.cache_resource
def get_conn():
    conn = psycopg2.connect(st.secrets["DATABASE_URL"])
    conn.autocommit = True
    return conn


def get_db():
    conn = get_conn()
    try:
        conn.cursor().execute("SELECT 1")
    except Exception:
        st.cache_resource.clear()
        conn = get_conn()
    _init_schema(conn)
    _seed_demo_if_missing(conn)
    return conn


def _seed_demo_if_missing(conn):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM teilnehmer WHERE gruppe = %s LIMIT 1", [DEMO_GRUPPE])
    if cur.fetchone():
        cur.close()
        return
    for pseudo, email in DEMO_TEILNEHMER:
        cur.execute(
            "INSERT INTO teilnehmer (pseudo, gruppe, email) VALUES (%s, %s, %s)",
            [pseudo, DEMO_GRUPPE, email]
        )
    rnd = random.Random(42)
    today = datetime.now()
    last_monday = today - timedelta(days=today.weekday())
    mondays = [last_monday - timedelta(weeks=w) for w in range(5, -1, -1)]
    for week_idx, monday in enumerate(mondays):
        base_stimmung = 4.0 - (1.2 * week_idx / 5)
        passt_weight = max(0, 5 - week_idx)
        zu_viel_weight = max(0, week_idx)
        wl_pool = ["passt"] * passt_weight + ["zu_viel"] * zu_viel_weight + ["zu_wenig"]
        for i, (pseudo, _) in enumerate(DEMO_TEILNEHMER):
            anon_token = hash_pseudo(pseudo)
            stimmung = max(1, min(5, round(base_stimmung + rnd.uniform(-0.3, 0.3))))
            komm = rnd.randint(2, 4)
            wl = rnd.choice(wl_pool)
            ts = monday.replace(hour=9 + (i % 8), minute=rnd.randint(0, 59), second=0, microsecond=0)
            cur.execute(
                """INSERT INTO pulse_checks (submitted_at, anon_token, gruppe, stimmung, workload, kommunikation)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                [ts, anon_token, DEMO_GRUPPE, stimmung, wl, komm]
            )
    cur.close()


def _init_schema(conn):
    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
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
            token VARCHAR DEFAULT gen_random_uuid(),
            PRIMARY KEY (pseudo, gruppe)
        )
    """)
    cur.execute("ALTER TABLE teilnehmer ADD COLUMN IF NOT EXISTS token VARCHAR DEFAULT gen_random_uuid()")
    cur.execute("UPDATE teilnehmer SET token = gen_random_uuid() WHERE token IS NULL")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_teilnehmer_token ON teilnehmer(token)")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS reminder_log (
            sent_at TIMESTAMP DEFAULT NOW(),
            gruppe VARCHAR,
            count INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS registrierungen (
            id SERIAL PRIMARY KEY,
            vorname VARCHAR,
            email VARCHAR,
            wunschgruppe VARCHAR,
            pseudo VARCHAR,
            status VARCHAR DEFAULT 'ausstehend',
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.close()


def hash_pseudo(pseudo):
    return hashlib.sha256(pseudo.strip().encode()).hexdigest()


def plotly_layout(**overrides):
    base = dict(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#F1F5F9', family='sans-serif', size=13),
        xaxis=dict(gridcolor='#334155', linecolor='#334155', zerolinecolor='#334155'),
        yaxis=dict(gridcolor='#334155', linecolor='#334155', zerolinecolor='#334155'),
        margin=dict(l=40, r=20, t=20, b=40),
        hoverlabel=dict(bgcolor='#1E293B', font_color='#F1F5F9', bordercolor='#475569'),
        legend=dict(bgcolor='rgba(0,0,0,0)', font=dict(color='#F1F5F9')),
    )
    for k, v in overrides.items():
        if isinstance(v, dict) and k in base and isinstance(base[k], dict):
            base[k].update(v)
        else:
            base[k] = v
    return base


def kpi_card(col, value, label, delta=None):
    delta_html = ""
    if delta is not None:
        if delta > 0.005:
            delta_html = f'<div class="kpi-delta up">▲ +{delta:.2f} vs. Vorwoche</div>'
        elif delta < -0.005:
            delta_html = f'<div class="kpi-delta down">▼ {delta:.2f} vs. Vorwoche</div>'
        else:
            delta_html = f'<div class="kpi-delta flat">— unverändert</div>'
    col.markdown(f"""
<div class="kpi-card">
  <div class="kpi-value">{value}</div>
  <div class="kpi-label">{label}</div>
  {delta_html}
</div>
""", unsafe_allow_html=True)


def admin_check(title="Admin-Zugang"):
    pw = secret("ADMIN_PASS")
    if not pw:
        st.error("ADMIN_PASS nicht konfiguriert.")
        return False
    if "auth_admin" not in st.session_state:
        st.session_state.auth_admin = False
    if st.session_state.auth_admin:
        return True
    _, mid, _ = st.columns([1, 1.4, 1])
    with mid:
        st.markdown(f"### 🔒 {title}")
        st.markdown('<p class="subtle">Zugriff nur für Team-Leads.</p>', unsafe_allow_html=True)
        entered = st.text_input("Passwort", type="password", label_visibility="collapsed", placeholder="Passwort eingeben")
        if st.button("Anmelden", use_container_width=True, type="primary"):
            if entered == pw:
                st.session_state.auth_admin = True
                st.rerun()
            else:
                st.error("Falsches Passwort.")
    return False


def choice_row(state_key, options, n_cols, render_label):
    cols = st.columns(n_cols)
    for i, opt in enumerate(options):
        val = opt[0]
        selected = st.session_state.get(state_key) == val
        btn_type = "primary" if selected else "secondary"
        with cols[i]:
            label_main, label_sub = render_label(opt)
            if st.button(label_main, key=f"{state_key}_{val}", use_container_width=True, type=btn_type):
                st.session_state[state_key] = val
                st.rerun()
            st.markdown(f'<div class="choice-label">{label_sub}</div>', unsafe_allow_html=True)


def send_welcome_email(pseudo, email, token):
    gmail_user = secret("GMAIL_USER")
    gmail_pass = secret("GMAIL_APP_PASS")
    app_url = secret("APP_URL")
    if not all([gmail_user, gmail_pass, app_url]):
        return False, "GMAIL_USER, GMAIL_APP_PASS und APP_URL müssen in secrets konfiguriert sein."
    link = f"{app_url}?token={token}"
    body = (
        f"Hallo {pseudo},\n\n"
        f"deine Anmeldung wurde freigegeben. Hier ist dein persönlicher Link für den wöchentlichen Stimmungs-Check:\n\n"
        f"{link}\n\n"
        f"Dauert nur 30 Sekunden pro Woche.\n\n"
        f"Dein Stimmungsbarometer-Team"
    )
    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = "Willkommen beim Stimmungsbarometer"
        msg["From"] = gmail_user
        msg["To"] = email
        smtp = smtplib.SMTP_SSL("mail.gmx.net", 465)
        smtp.login(gmail_user, gmail_pass)
        smtp.sendmail(gmail_user, email, msg.as_string())
        smtp.quit()
        return True, None
    except Exception as e:
        return False, str(e)


REG_KEYS = ["reg_vorname", "reg_email", "reg_wg", "reg_pseudo", "reg_consent"]


def render_registrierung():
    if st.session_state.get("registration_done"):
        st.markdown("# Anmeldung zum Stimmungsbarometer")
        st.markdown(
            '<div class="alert-ok">🎉 Danke für deine Anmeldung. Du bekommst deinen persönlichen Link per E-Mail, sobald der Administrator dich freigibt.</div>',
            unsafe_allow_html=True
        )
        if st.button("Neue Anmeldung"):
            st.session_state["registration_done"] = False
            for k in REG_KEYS:
                st.session_state.pop(k, None)
            st.rerun()
        return

    conn = get_db()
    cur = conn.cursor()

    st.markdown("# Anmeldung zum Stimmungsbarometer")
    st.markdown('<p class="subtle">Registriere dich für den wöchentlichen Pulse-Check. Du erhältst nach Freigabe deinen persönlichen Link per E-Mail.</p>', unsafe_allow_html=True)
    st.markdown(
        '<div class="info-box">ℹ️ Deine Antworten werden anonym gespeichert. Deine E-Mail-Adresse wird ausschließlich für wöchentliche Erinnerungen verwendet und nicht an Dritte weitergegeben.</div>',
        unsafe_allow_html=True
    )

    cur.execute("SELECT DISTINCT gruppe FROM teilnehmer ORDER BY gruppe")
    existing_groups = filter_out_demo([r[0] for r in cur.fetchall()])

    if not existing_groups:
        st.markdown(
            '<div class="alert-warn">Derzeit sind keine Gruppen verfügbar. Bitte wende dich an den Administrator.</div>',
            unsafe_allow_html=True
        )
        return

    c1, c2 = st.columns(2)
    with c1:
        vorname = st.text_input("Vorname", placeholder="Dein Vorname", key="reg_vorname")
        email = st.text_input("E-Mail", placeholder="name@beispiel.de", key="reg_email")
    with c2:
        wunschgruppe_sel = st.selectbox("Wunschgruppe", existing_groups, key="reg_wg")
    pseudo = st.text_input("Pseudonym", placeholder="Wird im Dashboard statt deinem Klarnamen angezeigt", key="reg_pseudo")

    cc1, cc2 = st.columns([5, 2])
    consent = cc1.checkbox(
        "Ich stimme der Verarbeitung meiner Daten gemäß der Datenschutzerklärung zu",
        key="reg_consent"
    )
    cc2.button(
        "📄 Datenschutzerklärung",
        key="goto_dsgvo",
        use_container_width=True,
        on_click=lambda: st.session_state.update(nav="Impressum & Datenschutz"),
    )

    if st.button("Anmeldung absenden", use_container_width=True, type="primary", disabled=not consent, key="reg_submit"):
        wunschgruppe = wunschgruppe_sel
        vorname_c = vorname.strip()
        email_c = email.strip()
        pseudo_c = pseudo.strip()

        if not all([vorname_c, email_c, wunschgruppe, pseudo_c]):
            st.error("Bitte alle Felder ausfüllen.")
        elif not valid_name(vorname_c):
            st.error("Ungültiger Vorname. Nur Buchstaben, Zahlen, Leerzeichen und Bindestriche erlaubt.")
        elif not valid_email(email_c):
            st.error("Ungültige E-Mail-Adresse.")
        elif not valid_name(pseudo_c):
            st.error("Ungültiges Pseudonym. Nur Buchstaben, Zahlen, Leerzeichen und Bindestriche erlaubt.")
        else:
            cur.execute("SELECT 1 FROM registrierungen WHERE email = %s", [email_c])
            if cur.fetchone():
                st.error("Für diese E-Mail-Adresse existiert bereits eine Anmeldung.")
            else:
                cur.execute(
                    """INSERT INTO registrierungen (vorname, email, wunschgruppe, pseudo)
                       VALUES (%s, %s, %s, %s)""",
                    [vorname_c, email_c, wunschgruppe, pseudo_c]
                )
                st.session_state["registration_done"] = True
                for k in REG_KEYS:
                    st.session_state.pop(k, None)
                st.rerun()


def render_checkin(token):
    st.markdown("# Wöchentlicher Pulse-Check")

    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT pseudo, gruppe, active FROM teilnehmer WHERE token = %s", [token])
    row = cur.fetchone()
    if not row:
        st.markdown('<div class="alert-warn">Ungültiger Link. Bitte nutze den Link aus deiner Einladungsmail.</div>', unsafe_allow_html=True)
        return

    pseudo, gruppe, active = row
    if not active:
        st.markdown('<div class="alert-warn">Dein Zugang wurde deaktiviert. Wende dich an den Administrator.</div>', unsafe_allow_html=True)
        return

    st.markdown(f'<div class="greeting">Hallo <b>{pseudo}</b> — Gruppe <b>{gruppe}</b></div>', unsafe_allow_html=True)
    st.markdown('<p class="subtle">Deine Antworten sind vollständig anonym. Dauer: ca. 30 Sekunden.</p>', unsafe_allow_html=True)
    st.markdown(
        '<div class="info-box">ℹ️ Deine Antworten werden anonym gespeichert. Deine E-Mail-Adresse wird ausschließlich für wöchentliche Erinnerungen verwendet und nicht an Dritte weitergegeben.</div>',
        unsafe_allow_html=True
    )

    anon_token = hash_pseudo(pseudo)
    iso = datetime.now().isocalendar()
    cur.execute(
        """SELECT 1 FROM pulse_checks
           WHERE anon_token = %s
             AND EXTRACT(ISOYEAR FROM submitted_at) = %s
             AND EXTRACT(WEEK FROM submitted_at) = %s""",
        [anon_token, iso.year, iso.week]
    )
    if cur.fetchone():
        st.markdown('<div class="alert-ok">✅ Du hast diese Woche bereits teilgenommen. Nächster Check-In ab Montag.</div>', unsafe_allow_html=True)
        return

    st.markdown("### Wie ist deine Stimmung?")
    choice_row("stimmung_sel", STIMMUNG_OPTIONS, 5, lambda o: (o[1], o[2]))

    st.markdown("### Wie ist dein Workload?")
    choice_row("workload_sel", WORKLOAD_OPTIONS, 3, lambda o: (f"{o[1]}  {o[2]}", ""))

    st.markdown("### Wie gut ist die Kommunikation im Team?")
    choice_row("komm_sel", KOMM_OPTIONS, 5, lambda o: (str(o[0]), o[1]))

    st.divider()
    can_submit = all([
        st.session_state.get("stimmung_sel"),
        st.session_state.get("workload_sel"),
        st.session_state.get("komm_sel"),
    ])

    _, mid, _ = st.columns([1, 1.2, 1])
    with mid:
        submit = st.button("Absenden", type="primary", use_container_width=True, disabled=not can_submit)

    if submit and can_submit:
        cur.execute(
            """INSERT INTO pulse_checks (anon_token, gruppe, stimmung, workload, kommunikation)
               VALUES (%s, %s, %s, %s, %s)""",
            [anon_token, gruppe, st.session_state["stimmung_sel"], st.session_state["workload_sel"], st.session_state["komm_sel"]]
        )
        cur.close()
        st.session_state["stimmung_sel"] = None
        st.session_state["workload_sel"] = None
        st.session_state["komm_sel"] = None
        st.markdown('<div class="alert-ok">🎉 Danke für dein Feedback — bis nächste Woche!</div>', unsafe_allow_html=True)
        components.html("""
<script src="https://cdn.jsdelivr.net/npm/canvas-confetti@1.9.2/dist/confetti.browser.min.js"></script>
<script>
setTimeout(function() {
    try {
        confetti({particleCount: 140, spread: 90, origin: {y: 0.6}, colors: ['#2563EB','#10B981','#F59E0B','#EF4444','#F1F5F9']});
        setTimeout(function(){ confetti({particleCount: 80, spread: 120, origin: {y: 0.7}}); }, 250);
    } catch(e) {}
}, 120);
</script>
""", height=0)


def page_anmeldung():
    render_registrierung()


def page_checkin():
    token = st.query_params.get("token", "").strip()
    if not token:
        st.markdown("# Wöchentlicher Pulse-Check")
        st.markdown('<div class="alert-warn">Bitte nutze den Link aus deiner Einladungsmail.</div>', unsafe_allow_html=True)
        return
    render_checkin(token)


def line_area_chart(x, y, color="#2563EB", y_range=(1, 5)):
    rgb = tuple(int(color.lstrip('#')[i:i+2], 16) for i in (0, 2, 4))
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=x, y=y, mode='lines+markers',
        line=dict(color=color, width=3, shape='spline', smoothing=0.8),
        fill='tozeroy',
        fillcolor=f'rgba({rgb[0]},{rgb[1]},{rgb[2]},0.18)',
        marker=dict(size=8, color=color, line=dict(color='#0F172A', width=2)),
        hovertemplate='%{x|%d.%m.%Y}<br>Ø %{y:.2f}<extra></extra>',
    ))
    fig.update_layout(**plotly_layout(
        yaxis=dict(range=[y_range[0], y_range[1]], gridcolor='#334155'),
        height=280, showlegend=False,
    ))
    return fig


def page_gruppen_dashboard():
    conn = get_db()
    cur = conn.cursor()
    params = st.query_params
    gruppe_param = params.get("gruppe", "")

    cur.execute("SELECT DISTINCT gruppe FROM pulse_checks ORDER BY gruppe")
    gruppen = sort_demo_last([r[0] for r in cur.fetchall()])
    if not gruppen:
        st.markdown("# Gruppen-Dashboard")
        st.info("Noch keine Daten vorhanden.")
        return

    if gruppe_param and gruppe_param in gruppen:
        idx = gruppen.index(gruppe_param)
    elif DEMO_GRUPPE in gruppen:
        idx = gruppen.index(DEMO_GRUPPE)
    else:
        idx = 0
    top1, top2 = st.columns([3, 1])
    with top2:
        gruppe = st.selectbox("Gruppe", gruppen, index=idx, label_visibility="collapsed")
    with top1:
        st.markdown(f"# {gruppe}")
        st.markdown('<p class="subtle">Stimmungsverlauf und Team-Signale</p>', unsafe_allow_html=True)

    cur.execute("""
        SELECT submitted_at, stimmung, kommunikation, workload
        FROM pulse_checks WHERE gruppe = %s ORDER BY submitted_at
    """, [gruppe])
    cols_desc = [d[0] for d in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=cols_desc)

    if df.empty:
        st.info("Keine Daten für diese Gruppe.")
        return

    df["submitted_at"] = pd.to_datetime(df["submitted_at"])
    df["woche"] = df["submitted_at"].dt.to_period("W").dt.start_time
    df["wl_score"] = df["workload"].map(WL_SCORE)

    weekly = df.groupby("woche").agg(
        stimmung_avg=("stimmung", "mean"),
        kommunikation_avg=("kommunikation", "mean"),
        wl_avg=("wl_score", "mean"),
        count=("stimmung", "count")
    ).reset_index()

    current = weekly.iloc[-1] if len(weekly) > 0 else None
    previous = weekly.iloc[-2] if len(weekly) > 1 else None

    k1, k2, k3, k4 = st.columns(4)
    delta_s = (current["stimmung_avg"] - previous["stimmung_avg"]) if previous is not None else None
    delta_k = (current["kommunikation_avg"] - previous["kommunikation_avg"]) if previous is not None else None
    delta_wl = (current["wl_avg"] - previous["wl_avg"]) if previous is not None else None
    wl_cur = float(current["wl_avg"])
    kpi_card(k1, f"{current['stimmung_avg']:.1f}", "Ø Stimmung", delta_s)
    kpi_card(k2, f"{current['kommunikation_avg']:.1f}", "Ø Kommunikation", delta_k)
    kpi_card(k3, f"{int(current['count'])}", "Antworten letzte Woche", None)
    kpi_card(k4, f"{wl_cur:+.2f}", f"Workload-Balance · {wl_label(wl_cur)}", delta_wl)

    st.markdown("### Stimmungsverlauf")
    st.plotly_chart(line_area_chart(weekly["woche"], weekly["stimmung_avg"]), use_container_width=True)

    st.markdown("### Kommunikationsverlauf")
    st.plotly_chart(line_area_chart(weekly["woche"], weekly["kommunikation_avg"], color="#10B981"), use_container_width=True)

    st.markdown("### Workload-Balance")
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=wl_cur,
        number={'font': {'color': '#F1F5F9', 'size': 42}, 'valueformat': '+.2f'},
        title={'text': f"<b style='color:{wl_color(wl_cur)}'>{wl_label(wl_cur)}</b>", 'font': {'size': 18}},
        gauge={
            'axis': {
                'range': [-1, 1],
                'tickvals': [-1, 0, 1],
                'ticktext': ['Zu wenig', 'Passt', 'Zu viel'],
                'tickfont': {'color': '#F1F5F9', 'size': 13},
                'tickcolor': '#334155',
            },
            'bar': {'color': wl_color(wl_cur), 'thickness': 0.28},
            'bgcolor': 'rgba(0,0,0,0)',
            'borderwidth': 1,
            'bordercolor': '#334155',
            'steps': [
                {'range': [-1, -0.3], 'color': 'rgba(245,158,11,0.22)'},
                {'range': [-0.3, 0.3], 'color': 'rgba(16,185,129,0.22)'},
                {'range': [0.3, 1], 'color': 'rgba(239,68,68,0.22)'},
            ],
            'threshold': {
                'line': {'color': '#F1F5F9', 'width': 3},
                'thickness': 0.85,
                'value': wl_cur,
            },
        },
    ))
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font={'color': '#F1F5F9', 'family': 'sans-serif'},
        margin=dict(l=40, r=40, t=60, b=20),
        height=280,
    )
    st.plotly_chart(fig, use_container_width=True)


def page_gesamt_dashboard():
    if not admin_check("Gesamt-Dashboard"):
        return

    st.markdown("# Übersicht aller Gruppen")
    st.markdown('<p class="subtle">Organisationsweite Stimmung, Heatmap und Frühwarnungen</p>', unsafe_allow_html=True)

    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT gruppe, submitted_at, stimmung, kommunikation, workload
        FROM pulse_checks ORDER BY submitted_at
    """)
    cols_desc = [d[0] for d in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=cols_desc)

    if df.empty:
        st.info("Noch keine Daten vorhanden.")
        return

    df["submitted_at"] = pd.to_datetime(df["submitted_at"])

    f1, _ = st.columns([1, 3])
    with f1:
        zeitraum = st.selectbox("Zeitraum", ["Letzte 4 Wochen", "Letzte 8 Wochen", "Letzte 12 Wochen", "Alle"], index=1)
    weeks_map = {"Letzte 4 Wochen": 4, "Letzte 8 Wochen": 8, "Letzte 12 Wochen": 12, "Alle": None}
    weeks = weeks_map[zeitraum]
    if weeks:
        cutoff = datetime.now() - timedelta(weeks=weeks)
        df = df[df["submitted_at"] >= cutoff]

    if df.empty:
        st.info("Keine Daten im gewählten Zeitraum.")
        return

    gruppe_sizes = df.groupby("gruppe")["stimmung"].count()
    kleine = gruppe_sizes[gruppe_sizes < MIN_GROUP_SIZE].index.tolist()
    df["gruppe_display"] = df["gruppe"].apply(lambda g: "Sonstige" if g in kleine else g)

    st.markdown("### Gruppenvergleich")
    avg_by_group = df.groupby("gruppe_display")["stimmung"].mean().sort_values()
    fig = go.Figure(go.Bar(
        x=avg_by_group.values, y=avg_by_group.index,
        orientation='h',
        marker=dict(
            color=avg_by_group.values, colorscale='RdYlGn',
            cmin=1, cmax=5,
            line=dict(color='#334155', width=1),
        ),
        text=[f"{v:.1f}" for v in avg_by_group.values],
        textposition='outside',
        hovertemplate='%{y}: Ø %{x:.2f}<extra></extra>',
    ))
    fig.update_layout(**plotly_layout(
        xaxis=dict(range=[0, 5.5], title="Ø Stimmung"),
        height=max(240, 60 * len(avg_by_group) + 80),
        showlegend=False,
    ))
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("### Heatmap: Gruppe × Kalenderwoche")
    df["kw"] = df["submitted_at"].dt.isocalendar().week.astype(int)
    df["jahr"] = df["submitted_at"].dt.year
    df["kw_label"] = df["jahr"].astype(str) + "-KW" + df["kw"].astype(str).str.zfill(2)
    heatmap_data = df.groupby(["gruppe_display", "kw_label"])["stimmung"].mean().reset_index()
    pivot = heatmap_data.pivot(index="gruppe_display", columns="kw_label", values="stimmung")
    pivot = pivot[sorted(pivot.columns)]
    z = pivot.values.tolist()
    text = [[f"{v:.1f}" if pd.notna(v) else "" for v in row] for row in z]
    fig = go.Figure(data=go.Heatmap(
        z=z, x=list(pivot.columns), y=list(pivot.index),
        colorscale='RdYlGn', zmin=1, zmax=5,
        text=text, texttemplate="%{text}",
        textfont=dict(color='#0F172A', size=13),
        hovertemplate='%{y} • %{x}<br>Ø %{z:.2f}<extra></extra>',
        colorbar=dict(thickness=12, tickcolor='#334155', tickfont=dict(color='#F1F5F9')),
    ))
    fig.update_layout(**plotly_layout(
        height=max(260, 55 * len(pivot.index) + 80),
        xaxis=dict(showgrid=False), yaxis=dict(showgrid=False),
    ))
    st.plotly_chart(fig, use_container_width=True)

    df["woche"] = df["submitted_at"].dt.to_period("W").dt.start_time
    weekly_group = df.groupby(["gruppe_display", "woche"])["stimmung"].mean().reset_index()
    trends = {}
    for g in weekly_group["gruppe_display"].unique():
        gdata = weekly_group[weekly_group["gruppe_display"] == g].sort_values("woche")
        if len(gdata) >= 2:
            trends[g] = gdata["stimmung"].iloc[-1] - gdata["stimmung"].iloc[-2]

    st.markdown("### Frühwarnung")
    if trends:
        worst = min(trends, key=trends.get)
        delta = trends[worst]
        if delta < -0.5:
            st.markdown(f'<div class="alert-warn">⚠️ <b>{worst}</b> zeigt einen deutlichen Abwärtstrend (Δ {delta:.2f}). Jetzt handeln.</div>', unsafe_allow_html=True)
        elif delta < 0:
            st.markdown(f'<div class="alert-warn">⚡ <b>{worst}</b> leicht rückläufig (Δ {delta:.2f}). Beobachten.</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="alert-ok">✅ Alle Gruppen stabil oder im Aufwärtstrend.</div>', unsafe_allow_html=True)
    else:
        st.info("Nicht genug historische Daten für Trends.")

    st.markdown("### Teilnehmerzahl & Antwortrate")
    cur.execute("SELECT gruppe, COUNT(*) as total FROM teilnehmer WHERE active = true GROUP BY gruppe")
    tcols = [d[0] for d in cur.description]
    teilnehmer_df = pd.DataFrame(cur.fetchall(), columns=tcols)
    if not teilnehmer_df.empty:
        letzte_woche = datetime.now() - timedelta(weeks=1)
        antworten = df[df["submitted_at"] >= letzte_woche].groupby("gruppe")["stimmung"].count().reset_index()
        antworten.columns = ["gruppe", "antworten"]
        merged = teilnehmer_df.merge(antworten, on="gruppe", how="left").fillna(0)
        merged["antworten"] = merged["antworten"].astype(int)
        merged["rate"] = (merged["antworten"] / merged["total"]).clip(upper=1.0)
        st.dataframe(
            merged.rename(columns={"gruppe": "Gruppe", "total": "Teilnehmer", "antworten": "Antworten", "rate": "Rate"}),
            hide_index=True, use_container_width=True,
            column_config={
                "Rate": st.column_config.ProgressColumn("Antwortrate", format="%.0f%%", min_value=0, max_value=1),
            },
        )
    else:
        st.info("Keine Teilnehmer in der Verwaltung hinterlegt.")


def render_registrierungen_tab(conn, cur):
    cur.execute("SELECT DISTINCT gruppe FROM teilnehmer ORDER BY gruppe")
    existing_groups = filter_out_demo([r[0] for r in cur.fetchall()])

    cur.execute("""
        SELECT id, vorname, email, wunschgruppe, pseudo, created_at
        FROM registrierungen
        WHERE status = 'ausstehend'
        ORDER BY created_at
    """)
    pending = cur.fetchall()

    if not pending:
        st.info("Keine ausstehenden Registrierungen.")
        return

    st.caption(f"{len(pending)} ausstehende Anmeldung(en)")

    for rid, vorname, email, wunschgruppe, pseudo, created_at in pending:
        with st.container():
            st.markdown(f'<div class="kpi-card" style="padding:16px 20px;">', unsafe_allow_html=True)
            c1, c2, c3 = st.columns([3, 3, 2])
            with c1:
                st.markdown(f"**{vorname}**  ")
                st.markdown(f'<span style="color:#94A3B8;font-size:13px;">{email}</span>', unsafe_allow_html=True)
                st.markdown(f'<span style="color:#64748B;font-size:12px;">angemeldet: {created_at.strftime("%d.%m.%Y %H:%M")}</span>', unsafe_allow_html=True)
            with c2:
                st.markdown(f'<span style="color:#94A3B8;font-size:13px;">Pseudo</span>', unsafe_allow_html=True)
                st.markdown(f"**{pseudo}**")

                default_idx = 0
                options = ([NEW_GROUP_OPT] + existing_groups) if existing_groups else [NEW_GROUP_OPT]
                if wunschgruppe in existing_groups:
                    default_idx = existing_groups.index(wunschgruppe) + 1
                else:
                    options = [NEW_GROUP_OPT, wunschgruppe] + [g for g in existing_groups if g != wunschgruppe]
                    default_idx = 1
                gruppe_choice = st.selectbox(
                    "Gruppe",
                    options,
                    index=default_idx,
                    key=f"reg_gruppe_{rid}",
                )
                new_gruppe_in = st.text_input(
                    "Neuer Gruppenname",
                    placeholder="z.B. Delta",
                    disabled=(gruppe_choice != NEW_GROUP_OPT),
                    key=f"reg_newg_{rid}",
                )
            with c3:
                st.markdown('<div style="height: 60px"></div>', unsafe_allow_html=True)
                if st.button("Freigeben", key=f"reg_approve_{rid}", type="primary", use_container_width=True):
                    final_gruppe = new_gruppe_in.strip() if gruppe_choice == NEW_GROUP_OPT else gruppe_choice
                    if not valid_name(final_gruppe):
                        st.error("Ungültiger Gruppenname.")
                    elif is_reserved_group(final_gruppe):
                        st.error("Dieser Gruppenname ist reserviert.")
                    elif not valid_name(pseudo):
                        st.error("Ungültiges Pseudonym in der Registrierung.")
                    else:
                        cur.execute("SELECT 1 FROM teilnehmer WHERE pseudo = %s AND gruppe = %s", [pseudo, final_gruppe])
                        if cur.fetchone():
                            st.error(f"**{pseudo}** existiert bereits in Gruppe **{final_gruppe}**.")
                        else:
                            cur.execute(
                                "INSERT INTO teilnehmer (pseudo, gruppe, email) VALUES (%s, %s, %s) RETURNING token",
                                [pseudo, final_gruppe, email]
                            )
                            token = cur.fetchone()[0]
                            cur.execute(
                                "UPDATE registrierungen SET status = 'freigegeben' WHERE id = %s",
                                [rid]
                            )
                            ok, err = send_welcome_email(pseudo, email, token)
                            if ok:
                                st.success(f"**{pseudo}** freigegeben — Willkommensmail versendet.")
                            else:
                                st.warning(f"Freigegeben, aber Mail-Versand fehlgeschlagen: {err}")
                            st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)
            st.markdown('<div style="height: 10px"></div>', unsafe_allow_html=True)


def render_teilnehmer_tab(conn, cur):
    cur.execute("SELECT DISTINCT gruppe FROM teilnehmer ORDER BY gruppe")
    existing_groups = filter_out_demo([r[0] for r in cur.fetchall()])
    options = ([NEW_GROUP_OPT] + existing_groups) if existing_groups else [NEW_GROUP_OPT]

    st.markdown("#### Teilnehmer hinzufügen")
    with st.form("add_teilnehmer", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            gruppe_sel = st.selectbox("Gruppe", options, index=1 if existing_groups else 0)
            new_gruppe = st.text_input("Neuer Gruppenname", placeholder="z.B. Delta", disabled=(gruppe_sel != NEW_GROUP_OPT))
        with c2:
            pseudo = st.text_input("Pseudonym", placeholder="z.B. Roter Falke")
            email = st.text_input("E-Mail", placeholder="name@firma.de")
        submit = st.form_submit_button("Hinzufügen", use_container_width=True, type="primary")
        if submit:
            gruppe = new_gruppe.strip() if gruppe_sel == NEW_GROUP_OPT else gruppe_sel
            pseudo_c = pseudo.strip()
            email_c = email.strip()
            if not gruppe or not pseudo_c or not email_c:
                st.error("Alle Felder ausfüllen.")
            elif not valid_name(gruppe):
                st.error("Ungültiger Gruppenname. Nur Buchstaben, Zahlen, Leerzeichen und Bindestriche erlaubt.")
            elif is_reserved_group(gruppe):
                st.error("Dieser Gruppenname ist reserviert.")
            elif not valid_name(pseudo_c):
                st.error("Ungültiges Pseudonym. Nur Buchstaben, Zahlen, Leerzeichen und Bindestriche erlaubt.")
            elif not valid_email(email_c):
                st.error("Ungültige E-Mail-Adresse.")
            else:
                cur.execute("SELECT 1 FROM teilnehmer WHERE pseudo = %s AND gruppe = %s", [pseudo_c, gruppe])
                if cur.fetchone():
                    st.error("Dieses Pseudonym ist in dieser Gruppe bereits vergeben.")
                else:
                    try:
                        cur.execute(
                            "INSERT INTO teilnehmer (pseudo, gruppe, email) VALUES (%s, %s, %s)",
                            [pseudo_c, gruppe, email_c]
                        )
                        st.success(f"**{pseudo_c}** zur Gruppe **{gruppe}** hinzugefügt.")
                        st.rerun()
                    except psycopg2.errors.UniqueViolation:
                        conn.rollback()
                        st.error("Dieses Pseudonym ist in dieser Gruppe bereits vergeben.")

    st.markdown("#### Teilnehmer-Liste")
    cur.execute(
        "SELECT pseudo, gruppe, email, active, token FROM teilnehmer WHERE gruppe <> %s ORDER BY gruppe, pseudo",
        [DEMO_GRUPPE]
    )
    tcols = [d[0] for d in cur.description]
    teilnehmer = pd.DataFrame(cur.fetchall(), columns=tcols)
    if teilnehmer.empty:
        st.info("Keine Teilnehmer vorhanden.")
        return

    gruppen_list = sorted(teilnehmer["gruppe"].unique())
    group_tabs = st.tabs(gruppen_list)
    for i, gname in enumerate(gruppen_list):
        with group_tabs[i]:
            gt = teilnehmer[teilnehmer["gruppe"] == gname].reset_index(drop=True)
            for _, row in gt.iterrows():
                row_id = f"{row['pseudo']}__{row['gruppe']}"
                pending_del = st.session_state.get("pending_del_t")

                c1, c2, c3, c4 = st.columns([3, 3.2, 1.4, 1.4])
                dot_class = "on" if row["active"] else "off"
                status_text = "Aktiv" if row["active"] else "Inaktiv"
                c1.markdown(
                    f'<span class="status-dot {dot_class}"></span>{row["pseudo"]} <span style="color:#64748B;font-size:12px;margin-left:6px;">{status_text}</span>',
                    unsafe_allow_html=True
                )
                c2.markdown(f'<span style="color:#94A3B8">{row["email"]}</span>', unsafe_allow_html=True)

                if pending_del == row_id:
                    if c3.button("Bestätigen", key=f"confirm_del_{row_id}", use_container_width=True, type="primary"):
                        cur.execute(
                            "DELETE FROM teilnehmer WHERE pseudo = %s AND gruppe = %s",
                            [row["pseudo"], row["gruppe"]]
                        )
                        st.session_state.pop("pending_del_t", None)
                        st.rerun()
                    if c4.button("Abbrechen", key=f"cancel_del_{row_id}", use_container_width=True):
                        st.session_state.pop("pending_del_t", None)
                        st.rerun()
                else:
                    if row["active"]:
                        if c3.button("Deaktivieren", key=f"deact_{row_id}", use_container_width=True):
                            cur.execute(
                                "UPDATE teilnehmer SET active = false WHERE pseudo = %s AND gruppe = %s",
                                [row["pseudo"], row["gruppe"]]
                            )
                            st.rerun()
                    if c4.button("🗑 Löschen", key=f"del_{row_id}", use_container_width=True):
                        st.session_state["pending_del_t"] = row_id
                        st.rerun()


def render_reminder_tab(conn, cur):
    cur.execute("SELECT DISTINCT gruppe FROM teilnehmer WHERE active = true ORDER BY gruppe")
    reminder_gruppen = filter_out_demo([r[0] for r in cur.fetchall()])
    if not reminder_gruppen:
        st.info("Keine aktiven Teilnehmer vorhanden.")
        return

    sel_gruppen = st.multiselect("Gruppen", reminder_gruppen, default=reminder_gruppen[:1] if reminder_gruppen else [])

    if sel_gruppen:
        cur.execute(
            "SELECT gruppe, MAX(sent_at) FROM reminder_log WHERE gruppe = ANY(%s) GROUP BY gruppe",
            [sel_gruppen]
        )
        last_rows = {r[0]: r[1] for r in cur.fetchall()}
        for g in sel_gruppen:
            if g in last_rows and last_rows[g]:
                days_ago = (datetime.now() - last_rows[g]).days
                st.caption(f"**{g}** — zuletzt gesendet vor {days_ago} Tag{'en' if days_ago != 1 else ''}")
            else:
                st.caption(f"**{g}** — noch kein Reminder gesendet")

    if st.button("📧 Reminder jetzt senden", type="primary", disabled=not sel_gruppen):
        gmail_user = secret("GMAIL_USER")
        gmail_pass = secret("GMAIL_APP_PASS")
        app_url = secret("APP_URL")
        if not all([gmail_user, gmail_pass, app_url]):
            st.error("GMAIL_USER, GMAIL_APP_PASS und APP_URL müssen in secrets konfiguriert sein.")
            return

        total_sent = 0
        all_errors = []
        with st.spinner("Sende Reminder..."):
            try:
                smtp = smtplib.SMTP_SSL("mail.gmx.net", 465)
                smtp.login(gmail_user, gmail_pass)
            except Exception as e:
                st.error(f"SMTP-Fehler: {e}")
                return

            for g in sel_gruppen:
                cur.execute(
                    "SELECT pseudo, email, token FROM teilnehmer WHERE gruppe = %s AND active = true",
                    [g]
                )
                empfaenger = cur.fetchall()
                sent_count = 0
                for pseudo_r, email_r, token_r in empfaenger:
                    link = f"{app_url}?token={token_r}"
                    body = (
                        f"Hallo {pseudo_r}, hier ist dein persönlicher Link für den wöchentlichen Check-In: "
                        f"{link}. Dauert nur 30 Sekunden."
                    )
                    msg = MIMEText(body, "plain", "utf-8")
                    msg["Subject"] = "Dein wöchentlicher Stimmungs-Check"
                    msg["From"] = gmail_user
                    msg["To"] = email_r
                    try:
                        smtp.sendmail(gmail_user, email_r, msg.as_string())
                        sent_count += 1
                    except Exception as e:
                        all_errors.append(f"{g} / {email_r}: {e}")
                cur.execute(
                    "INSERT INTO reminder_log (gruppe, count) VALUES (%s, %s)",
                    [g, sent_count]
                )
                total_sent += sent_count
            smtp.quit()

        st.markdown(f'<div class="alert-ok">✅ {total_sent} Reminder an {len(sel_gruppen)} Gruppe(n) gesendet.</div>', unsafe_allow_html=True)
        if all_errors:
            for err in all_errors:
                st.warning(err)


def render_checkins_tab(conn, cur):
    cur.execute("SELECT DISTINCT gruppe FROM pulse_checks ORDER BY gruppe")
    groups = filter_out_demo([r[0] for r in cur.fetchall()])
    if not groups:
        st.info("Noch keine Check-Ins vorhanden.")
        return

    sel_group = st.selectbox("Gruppe", groups, key="ci_group")

    cur.execute("SELECT pseudo FROM teilnehmer WHERE gruppe = %s", [sel_group])
    pseudo_map = {hash_pseudo(r[0]): r[0] for r in cur.fetchall()}

    cur.execute(
        """SELECT id, submitted_at, anon_token, stimmung, workload, kommunikation
           FROM pulse_checks WHERE gruppe = %s ORDER BY submitted_at DESC LIMIT 100""",
        [sel_group]
    )
    rows = cur.fetchall()
    if not rows:
        st.info("Keine Check-Ins für diese Gruppe.")
        return

    st.caption(f"Neueste {len(rows)} Check-Ins — zum Löschen falsch eingetragener Abgaben")

    pending_del_ci = st.session_state.get("pending_del_ci")
    for cid, submitted_at, anon_token, stimmung, workload, kommunikation in rows:
        pseudo = pseudo_map.get(anon_token, "—")
        c1, c2, c3, c4, c5 = st.columns([2.5, 2.3, 2, 1.4, 1.4])
        c1.markdown(
            f'**{pseudo}**  \n<span style="color:#94A3B8;font-size:12px;">{submitted_at.strftime("%d.%m.%Y %H:%M")}</span>',
            unsafe_allow_html=True
        )
        c2.markdown(f"Stimmung: **{stimmung}**  \nKomm: **{kommunikation}**")
        c3.markdown(f"Workload: **{workload}**")

        if pending_del_ci == cid:
            if c4.button("Bestätigen", key=f"ci_ok_{cid}", use_container_width=True, type="primary"):
                cur.execute("DELETE FROM pulse_checks WHERE id = %s", [cid])
                st.session_state.pop("pending_del_ci", None)
                st.rerun()
            if c5.button("Abbrechen", key=f"ci_x_{cid}", use_container_width=True):
                st.session_state.pop("pending_del_ci", None)
                st.rerun()
        else:
            if c5.button("🗑 Löschen", key=f"ci_del_{cid}", use_container_width=True):
                st.session_state["pending_del_ci"] = cid
                st.rerun()


def page_verwaltung():
    if not admin_check("Verwaltung"):
        return

    st.markdown("# Verwaltung")
    st.markdown('<p class="subtle">Registrierungen, Teilnehmer, Check-Ins und Reminder</p>', unsafe_allow_html=True)

    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM registrierungen WHERE status = 'ausstehend'")
    pending_count = cur.fetchone()[0]

    reg_label = f"📥 Registrierungen ({pending_count})" if pending_count else "📥 Registrierungen"
    tabs = st.tabs([reg_label, "👥 Teilnehmer", "📊 Check-Ins", "📧 Reminder"])
    with tabs[0]:
        render_registrierungen_tab(conn, cur)
    with tabs[1]:
        render_teilnehmer_tab(conn, cur)
    with tabs[2]:
        render_checkins_tab(conn, cur)
    with tabs[3]:
        render_reminder_tab(conn, cur)


def page_impressum():
    st.markdown("# Impressum & Datenschutz")

    st.markdown("## Impressum")
    st.markdown("""
**John Hamelmann**
Hochschule Düsseldorf
Münsterstraße 156
40476 Düsseldorf

E-Mail: jfhamelmann@gmail.com
""")

    st.markdown("### Kontext")
    st.markdown(
        "Studentisches Projekt im Rahmen des Studiengangs **Data Science (B.Sc.)** "
        "an der Hochschule Düsseldorf. Betreut durch "
        "**Prof. Dr. Dennis Müller** und **Prof. Dr. Dominik Austermann**."
    )
    st.markdown(
        "Beim Rollout unterstützt durch **Yannik Huber** (Studiengang Data Science, HSD) — "
        "E-Mail: yannik.huber@study.hs-duesseldorf.de."
    )

    st.divider()

    st.markdown("## Datenschutzerklärung")

    st.markdown("### Welche Daten werden erhoben")
    st.markdown("""
- **Vorname** (bei der Anmeldung)
- **E-Mail-Adresse** (für den Versand des persönlichen Check-In-Links und wöchentlicher Erinnerungen)
- **Pseudonym** (wird im Dashboard anstelle des Klarnamens angezeigt)
- **Stimmungsdaten** (Stimmung, Workload, Kommunikation — gespeichert mit einem anonymen Hash des Pseudonyms, nicht mit Klarnamen oder E-Mail verknüpft)
""")

    st.markdown("### Zweck der Verarbeitung")
    st.markdown(
        "Die Daten werden ausschließlich für anonymes Team-Feedback im Rahmen "
        "von Lehrveranstaltungen genutzt. Ziel ist, Stimmungstrends in Gruppen "
        "sichtbar zu machen, nicht einzelne Personen zu bewerten."
    )

    st.markdown("### Speicherort")
    st.markdown(
        "Die Daten werden auf **Supabase** (PostgreSQL) in der **EU-Region** gespeichert. "
        "Es erfolgt **keine Weitergabe an Dritte**."
    )

    st.markdown("### Löschung")
    st.markdown(
        "Auf Anfrage per E-Mail an **jfhamelmann@gmail.com** werden alle zu einer "
        "E-Mail-Adresse gehörenden Daten unverzüglich gelöscht."
    )

    st.markdown("### Tracking")
    st.markdown("Kein Tracking, keine Cookies, keine Analytics.")


PAGES = {
    "Anmeldung": page_anmeldung,
    "Check-In": page_checkin,
    "Gruppen-Dashboard": page_gruppen_dashboard,
    "Gesamt-Dashboard": page_gesamt_dashboard,
    "Verwaltung": page_verwaltung,
    "Impressum & Datenschutz": page_impressum,
}

st.set_page_config(page_title="Stimmungsbarometer", page_icon="🌡️", layout="wide", initial_sidebar_state="expanded")
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

if "nav" not in st.session_state:
    st.session_state.nav = "Check-In" if st.query_params.get("token", "").strip() else "Anmeldung"

st.sidebar.title("🌡️ Stimmungsbarometer")
st.sidebar.divider()
page = st.sidebar.radio("Navigation", list(PAGES.keys()), key="nav")
if st.session_state.get("auth_admin"):
    if st.sidebar.button("Abmelden", use_container_width=True, key="logout_btn"):
        st.session_state.auth_admin = False
        st.rerun()
st.sidebar.divider()
st.sidebar.caption("v1.0 — HSD Business Analytics")

PAGES[page]()
