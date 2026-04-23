import hashlib
import random
import re
import smtplib
from datetime import datetime, timedelta
from email.mime.text import MIMEText
import plotly.graph_objects as go
import psycopg2
import streamlit as st

MIN_GROUP_SIZE = 3
NAME_RE = re.compile(r'^[a-zA-Z0-9äöüÄÖÜß\s\-·]{1,60}$')
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
STIMMUNG_OPTIONS = [(1, "😞", "Schlecht"), (2, "😕", "Mäßig"), (3, "😐", "Okay"), (4, "🙂", "Gut"), (5, "😄", "Super")]
KOMM_OPTIONS = [(1, "Kaum"), (2, "Wenig"), (3, "Okay"), (4, "Gut"), (5, "Exzellent")]
WORKLOAD_OPTIONS = [("zu_wenig", "📉", "Zu wenig"), ("passt", "⚖️", "Passt"), ("zu_viel", "📈", "Zu viel")]
WL_SCORE = {"zu_wenig": -1, "passt": 0, "zu_viel": 1}
CONSENT_VERSION = "v1.0"
PAGE_TITLE = "Stimmungsbarometer"
PAGE_ICON = "🌡️"
APP_VERSION = "v1.0 — HSD Business Analytics"
NAV_ANMELDUNG = "Anmeldung"
NAV_CHECKIN = "Check-In"
NAV_DASH_GRUPPE = "Gruppen-Dashboard"
NAV_DASH_GESAMT = "Gesamt-Dashboard"
NAV_VERWALTUNG = "Verwaltung"
NAV_IMPRESSUM = "Impressum & Datenschutz"

CUSTOM_CSS = """
<style>
#MainMenu, footer {visibility: hidden;}
[data-testid="stHeader"] {display: none;}
[data-testid="stToolbar"] {display: none !important;}
[data-testid="stDecoration"] {display: none;}
section[data-testid="stSidebar"],
section[data-testid="stSidebar"][aria-expanded="false"] {
    transform: translateX(0) !important; margin-left: 0 !important; left: 0 !important;
    visibility: visible !important; min-width: 244px !important; width: 244px !important;
}
section[data-testid="stSidebar"] button[kind="header"],
[data-testid="stSidebarCollapseButton"],
[data-testid="stSidebarCollapsedControl"],
[data-testid="collapsedControl"] {display: none !important;}
.block-container {padding-top: 2.5rem; padding-bottom: 3rem; max-width: 1200px;}
h1 {font-weight: 700; letter-spacing: -0.02em;}
h2, h3 {font-weight: 600; letter-spacing: -0.01em;}
.subtle {color: #94A3B8; font-size: 15px; margin: -8px 0 24px 0;}
.kpi-card {background: #1E293B; border: 1px solid #334155; border-radius: 14px; padding: 22px 24px; transition: border-color 0.2s;}
.kpi-card:hover {border-color: #475569;}
.kpi-value {font-size: 34px; font-weight: 700; color: #F1F5F9; line-height: 1.1;}
.kpi-label {font-size: 12px; color: #94A3B8; text-transform: uppercase; letter-spacing: 0.6px; margin-top: 6px; font-weight: 600;}
.kpi-delta {font-size: 13px; margin-top: 10px; font-weight: 600;}
.kpi-delta.up {color: #10B981;}
.kpi-delta.down {color: #EF4444;}
.kpi-delta.flat {color: #64748B;}
.stButton > button {border-radius: 10px; padding: 10px 18px; font-weight: 500; transition: all 0.15s; border: 1px solid #334155; background: #1E293B; color: #F1F5F9;}
.stButton > button:hover {transform: translateY(-1px); border-color: #475569; background: #263449;}
.stButton > button[kind="primary"] {background: #2563EB; border-color: #2563EB; color: #FFFFFF;}
.stButton > button[kind="primary"]:hover {background: #1D4ED8; border-color: #1D4ED8; box-shadow: 0 6px 18px rgba(37,99,235,0.35);}
.stButton > button:disabled {opacity: 0.45;}
.choice-label {text-align: center; color: #94A3B8; font-size: 13px; margin: 6px 0 14px 0;}
.alert-warn {background: rgba(239,68,68,0.08); border: 1px solid rgba(239,68,68,0.35); border-radius: 10px; padding: 14px 18px; color: #FCA5A5; margin: 8px 0 20px 0; font-weight: 500;}
.alert-ok {background: rgba(16,185,129,0.08); border: 1px solid rgba(16,185,129,0.35); border-radius: 10px; padding: 14px 18px; color: #6EE7B7; margin: 8px 0 20px 0; font-weight: 500;}
.info-box {background: rgba(37,99,235,0.08); border: 1px solid rgba(37,99,235,0.3); border-radius: 10px; padding: 12px 16px; color: #93C5FD; margin: 8px 0 24px 0; font-size: 13px; line-height: 1.5;}
.status-dot {display: inline-block; width: 9px; height: 9px; border-radius: 50%; margin-right: 8px;}
.status-dot.on {background: #10B981; box-shadow: 0 0 0 3px rgba(16,185,129,0.2);}
.status-dot.off {background: #EF4444; box-shadow: 0 0 0 3px rgba(239,68,68,0.2);}
.greeting {background: #1E293B; border: 1px solid #334155; border-radius: 12px; padding: 18px 22px; margin: 8px 0 8px 0; font-size: 17px; color: #F1F5F9;}
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

def hash_pseudo(pseudo, gruppe):
    return hashlib.sha256(f"{pseudo.strip()}|{gruppe.strip()}".encode()).hexdigest()

def filter_out_demo(gruppen):
    return [g for g in gruppen if g != DEMO_GRUPPE]

def sort_demo_last(gruppen):
    normal = sorted([g for g in gruppen if g != DEMO_GRUPPE])
    demo = [g for g in gruppen if g == DEMO_GRUPPE]
    return normal + demo

def is_reserved_group(name):
    return name.strip().lower() == DEMO_GRUPPE.lower()

def firma_of(gruppe_name):
    if " · " in gruppe_name:
        return gruppe_name.split(" · ", 1)[0].strip()
    return "Andere"

def groups_by_firma(cur):
    cur.execute("""SELECT name, firma FROM gruppen
        WHERE name IS NOT NULL AND name <> '' AND name <> %s""", [DEMO_GRUPPE])
    result = {}
    for name, firma in cur.fetchall():
        if firma:
            key = firma
        else:
            parsed = firma_of(name)
            if parsed == "Andere":
                continue
            key = parsed
        result.setdefault(key, []).append(name)
    for f in result:
        result[f] = sorted(result[f])
    return dict(sorted(result.items()))

def list_firmen(cur):
    cur.execute("SELECT name FROM firmen ORDER BY name")
    return [r[0] for r in cur.fetchall()]

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

@st.cache_resource
def _get_conn():
    conn = psycopg2.connect(st.secrets["DATABASE_URL"])
    conn.autocommit = True
    return conn

@st.cache_resource
def _bootstrap():
    conn = _get_conn()
    _init_schema(conn)
    _seed_demo_if_missing(conn)
    _migrate_firmen(conn)
    return True

def get_db():
    conn = _get_conn()
    try:
        conn.cursor().execute("SELECT 1")
    except Exception:
        st.cache_resource.clear()
        conn = _get_conn()
    _bootstrap()
    return conn

def _init_schema(conn):
    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    cur.execute("""CREATE TABLE IF NOT EXISTS pulse_checks (
        id SERIAL PRIMARY KEY, submitted_at TIMESTAMP DEFAULT NOW(),
        anon_token VARCHAR, gruppe VARCHAR, stimmung INTEGER,
        workload VARCHAR, kommunikation INTEGER)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS teilnehmer (
        pseudo VARCHAR, gruppe VARCHAR, email VARCHAR,
        active BOOLEAN DEFAULT true, token VARCHAR DEFAULT gen_random_uuid(),
        PRIMARY KEY (pseudo, gruppe))""")
    cur.execute("ALTER TABLE teilnehmer ADD COLUMN IF NOT EXISTS token VARCHAR DEFAULT gen_random_uuid()")
    cur.execute("UPDATE teilnehmer SET token = gen_random_uuid() WHERE token IS NULL")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_teilnehmer_token ON teilnehmer(token)")
    cur.execute("""CREATE TABLE IF NOT EXISTS reminder_log (
        sent_at TIMESTAMP DEFAULT NOW(), gruppe VARCHAR, count INTEGER)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS registrierungen (
        id SERIAL PRIMARY KEY, vorname VARCHAR, email VARCHAR,
        wunschgruppe VARCHAR, pseudo VARCHAR,
        status VARCHAR DEFAULT 'ausstehend', created_at TIMESTAMP DEFAULT NOW(),
        consented_at TIMESTAMP, consent_version VARCHAR)""")
    cur.execute("ALTER TABLE registrierungen ADD COLUMN IF NOT EXISTS consented_at TIMESTAMP")
    cur.execute("ALTER TABLE registrierungen ADD COLUMN IF NOT EXISTS consent_version VARCHAR")
    cur.execute("""CREATE TABLE IF NOT EXISTS firmen (
        name VARCHAR PRIMARY KEY, created_at TIMESTAMP DEFAULT NOW())""")
    cur.execute("""CREATE TABLE IF NOT EXISTS gruppen (
        name VARCHAR PRIMARY KEY, firma VARCHAR, created_at TIMESTAMP DEFAULT NOW())""")
    cur.execute("ALTER TABLE gruppen ADD COLUMN IF NOT EXISTS firma VARCHAR")
    try:
        cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS idx_pulse_checks_week
            ON pulse_checks (anon_token, (date_trunc('week', submitted_at)))""")
    except psycopg2.Error:
        pass
    cur.close()

def _migrate_firmen(conn):
    cur = conn.cursor()
    cur.execute("""SELECT DISTINCT t.gruppe FROM teilnehmer t
        LEFT JOIN gruppen g ON g.name = t.gruppe
        WHERE g.name IS NULL AND t.gruppe IS NOT NULL AND t.gruppe <> ''""")
    for (name,) in cur.fetchall():
        firma = name.split(" · ", 1)[0].strip() if " · " in name else None
        cur.execute("INSERT INTO gruppen (name, firma) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    [name, firma])
    cur.execute("SELECT name FROM gruppen WHERE firma IS NULL AND name <> %s", [DEMO_GRUPPE])
    for (name,) in cur.fetchall():
        if " · " in name:
            firma = name.split(" · ", 1)[0].strip()
            cur.execute("UPDATE gruppen SET firma = %s WHERE name = %s", [firma, name])
    cur.execute("SELECT DISTINCT firma FROM gruppen WHERE firma IS NOT NULL AND firma <> ''")
    for (firma,) in cur.fetchall():
        cur.execute("INSERT INTO firmen (name) VALUES (%s) ON CONFLICT DO NOTHING", [firma])
    cur.close()

def _seed_demo_if_missing(conn):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM teilnehmer WHERE gruppe = %s LIMIT 1", [DEMO_GRUPPE])
    if cur.fetchone():
        cur.close()
        return
    for pseudo, email in DEMO_TEILNEHMER:
        cur.execute("INSERT INTO teilnehmer (pseudo, gruppe, email) VALUES (%s, %s, %s)",
                    [pseudo, DEMO_GRUPPE, email])
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
            anon_token = hash_pseudo(pseudo, DEMO_GRUPPE)
            stimmung = max(1, min(5, round(base_stimmung + rnd.uniform(-0.3, 0.3))))
            komm = rnd.randint(2, 4)
            wl = rnd.choice(wl_pool)
            ts = monday.replace(hour=9 + (i % 8), minute=rnd.randint(0, 59), second=0, microsecond=0)
            cur.execute("""INSERT INTO pulse_checks (submitted_at, anon_token, gruppe, stimmung, workload, kommunikation)
                VALUES (%s, %s, %s, %s, %s, %s)""", [ts, anon_token, DEMO_GRUPPE, stimmung, wl, komm])
    cur.close()

def list_groups(cur):
    cur.execute("""SELECT name FROM (
        SELECT name FROM gruppen UNION
        SELECT DISTINCT gruppe AS name FROM teilnehmer
    ) g WHERE name IS NOT NULL AND name <> '' ORDER BY name""")
    return filter_out_demo([r[0] for r in cur.fetchall()])

def _smtp_credentials():
    gmail_user = secret("GMAIL_USER")
    gmail_pass = secret("GMAIL_APP_PASS")
    app_url = secret("APP_URL")
    smtp_host = secret("SMTP_HOST", "") or "mail.gmx.net"
    port_raw = secret("SMTP_PORT", 465)
    try:
        smtp_port = int(port_raw) if port_raw else 465
    except (TypeError, ValueError):
        smtp_port = 465
    if not all([gmail_user, gmail_pass, app_url]):
        return None, None, None, None, None
    return gmail_user, gmail_pass, app_url, smtp_host, smtp_port

def _build_message(subject, body, sender, recipient):
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient
    return msg

def send_registration_confirmation(vorname, email):
    gmail_user, gmail_pass, _, smtp_host, smtp_port = _smtp_credentials()
    if not gmail_user:
        return False, "SMTP nicht konfiguriert."
    body = (f"Hallo {vorname},\n\ndanke für deine Anmeldung beim Stimmungsbarometer. Wir haben sie erhalten.\n\n"
            f"Sobald der Administrator dich freigibt, bekommst du eine zweite Mail mit deinem persönlichen Check-In-Link.\n\n"
            f"Dein Stimmungsbarometer-Team")
    try:
        msg = _build_message("Deine Anmeldung ist bei uns angekommen", body, gmail_user, email)
        smtp = smtplib.SMTP_SSL(smtp_host, smtp_port)
        smtp.login(gmail_user, gmail_pass)
        smtp.sendmail(gmail_user, email, msg.as_string())
        smtp.quit()
        return True, None
    except Exception as e:
        return False, str(e)

def send_welcome_email(pseudo, email, token):
    gmail_user, gmail_pass, app_url, smtp_host, smtp_port = _smtp_credentials()
    if not gmail_user:
        return False, "GMAIL_USER, GMAIL_APP_PASS und APP_URL müssen in secrets konfiguriert sein."
    link = f"{app_url}?token={token}"
    body = (f"Hallo {pseudo},\n\ndeine Anmeldung wurde freigegeben. Hier ist dein persönlicher Link für den wöchentlichen Stimmungs-Check:\n\n"
            f"{link}\n\nDauert nur 30 Sekunden pro Woche.\n\nDein Stimmungsbarometer-Team")
    try:
        msg = _build_message("Willkommen beim Stimmungsbarometer", body, gmail_user, email)
        smtp = smtplib.SMTP_SSL(smtp_host, smtp_port)
        smtp.login(gmail_user, gmail_pass)
        smtp.sendmail(gmail_user, email, msg.as_string())
        smtp.quit()
        return True, None
    except Exception as e:
        return False, str(e)

def send_reminder_batch(recipients_by_group):
    gmail_user, gmail_pass, app_url, smtp_host, smtp_port = _smtp_credentials()
    if not gmail_user:
        return {}, "GMAIL_USER, GMAIL_APP_PASS und APP_URL müssen in secrets konfiguriert sein."
    try:
        smtp = smtplib.SMTP_SSL(smtp_host, smtp_port)
        smtp.login(gmail_user, gmail_pass)
    except Exception as e:
        return {}, f"SMTP-Fehler: {e}"
    results = {}
    errors = []
    for gruppe, empfaenger in recipients_by_group.items():
        sent = 0
        for pseudo, email, token in empfaenger:
            link = f"{app_url}?token={token}"
            body = (f"Hallo {pseudo}, hier ist dein persönlicher Link für den wöchentlichen Check-In: "
                    f"{link}. Dauert nur 30 Sekunden.")
            try:
                msg = _build_message("Dein wöchentlicher Stimmungs-Check", body, gmail_user, email)
                smtp.sendmail(gmail_user, email, msg.as_string())
                sent += 1
            except Exception as e:
                errors.append(f"{gruppe} / {email}: {e}")
        results[gruppe] = sent
    smtp.quit()
    return results, (errors or None)

def plotly_layout(**overrides):
    base = dict(
        plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
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
            delta_html = '<div class="kpi-delta flat">— unverändert</div>'
    col.markdown(f'<div class="kpi-card"><div class="kpi-value">{value}</div>'
                 f'<div class="kpi-label">{label}</div>{delta_html}</div>', unsafe_allow_html=True)

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

def line_area_chart(x, y, color="#2563EB", y_range=(1, 5)):
    rgb = tuple(int(color.lstrip('#')[i:i + 2], 16) for i in (0, 2, 4))
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=x, y=y, mode='lines+markers',
        line=dict(color=color, width=3, shape='spline', smoothing=0.8),
        fill='tozeroy', fillcolor=f'rgba({rgb[0]},{rgb[1]},{rgb[2]},0.18)',
        marker=dict(size=8, color=color, line=dict(color='#0F172A', width=2)),
        hovertemplate='%{x|%d.%m.%Y}<br>Ø %{y:.2f}<extra></extra>',
    ))
    fig.update_layout(**plotly_layout(
        yaxis=dict(range=[y_range[0], y_range[1]], gridcolor='#334155'),
        height=280, showlegend=False))
    return fig

def workload_gauge(wl_cur):
    wl_cur = round(float(wl_cur), 2)
    fig = go.Figure(go.Indicator(
        mode="gauge+number", value=wl_cur,
        number={'font': {'color': '#F1F5F9', 'size': 42}, 'valueformat': '+.2f'},
        title={'text': f"<b style='color:{wl_color(wl_cur)}'>{wl_label(wl_cur)}</b>", 'font': {'size': 18}},
        gauge={
            'axis': {'range': [-1, 1], 'tickvals': [-1, 0, 1],
                     'ticktext': ['Zu wenig', 'Passt', 'Zu viel'],
                     'tickfont': {'color': '#F1F5F9', 'size': 13}, 'tickcolor': '#334155'},
            'bar': {'color': wl_color(wl_cur), 'thickness': 0.28},
            'bgcolor': 'rgba(0,0,0,0)', 'borderwidth': 1, 'bordercolor': '#334155',
            'steps': [
                {'range': [-1, -0.3], 'color': 'rgba(245,158,11,0.22)'},
                {'range': [-0.3, 0.3], 'color': 'rgba(16,185,129,0.22)'},
                {'range': [0.3, 1], 'color': 'rgba(239,68,68,0.22)'}],
            'threshold': {'line': {'color': '#F1F5F9', 'width': 3}, 'thickness': 0.85, 'value': wl_cur},
        }))
    fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
                      font={'color': '#F1F5F9', 'family': 'sans-serif'},
                      margin=dict(l=40, r=40, t=60, b=20), height=280)
    return fig

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

def alert_warn(text):
    st.markdown(f'<div class="alert-warn">{text}</div>', unsafe_allow_html=True)

def alert_ok(text):
    st.markdown(f'<div class="alert-ok">{text}</div>', unsafe_allow_html=True)

def info_box(text):
    st.markdown(f'<div class="info-box">{text}</div>', unsafe_allow_html=True)

def subtle(text):
    st.markdown(f'<p class="subtle">{text}</p>', unsafe_allow_html=True)

def page_header(title, sub=None):
    st.markdown(f"# {title}")
    if sub:
        subtle(sub)
