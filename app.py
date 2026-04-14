import streamlit as st
import streamlit.components.v1 as components
import psycopg2
import hashlib
import smtplib
import pandas as pd
import plotly.graph_objects as go
from email.mime.text import MIMEText
from datetime import datetime, timedelta

MIN_GROUP_SIZE = 3

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

CUSTOM_CSS = """
<style>
#MainMenu, footer {visibility: hidden;}
[data-testid="stHeader"] {display: none;}
[data-testid="stToolbar"] {display: none !important;}
[data-testid="stDecoration"] {display: none;}

.block-container {padding-top: 2.5rem; padding-bottom: 3rem; max-width: 1200px;}

h1 {font-weight: 700; letter-spacing: -0.02em;}
h2, h3 {font-weight: 600; letter-spacing: -0.01em;}

.subtle {color: #94A3B8; font-size: 15px; margin: -8px 0 24px 0;}

.sidebar-brand {
    display: flex; align-items: center; gap: 10px;
    padding: 6px 0 20px 0;
    border-bottom: 1px solid #334155; margin-bottom: 18px;
}
.brand-icon {font-size: 26px;}
.brand-name {font-size: 17px; font-weight: 700; color: #F1F5F9;}

[data-testid="stSidebar"] [data-testid="stRadio"] label {
    padding: 10px 12px; border-radius: 8px; margin: 2px 0;
    transition: background 0.15s;
}
[data-testid="stSidebar"] [data-testid="stRadio"] label:hover {background: rgba(37,99,235,0.08);}

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

.status-dot {display: inline-block; width: 9px; height: 9px; border-radius: 50%; margin-right: 8px;}
.status-dot.on {background: #10B981; box-shadow: 0 0 0 3px rgba(16,185,129,0.2);}
.status-dot.off {background: #EF4444; box-shadow: 0 0 0 3px rgba(239,68,68,0.2);}

.section-card {
    background: #1E293B; border: 1px solid #334155;
    border-radius: 14px; padding: 24px; margin-bottom: 20px;
}

hr {border-color: #334155 !important; margin: 1.5rem 0 !important;}

[data-testid="stDataFrame"] {border-radius: 10px; overflow: hidden;}
</style>
"""


def secret(key, default=""):
    try:
        return st.secrets[key]
    except (KeyError, FileNotFoundError):
        return default


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
    return conn


def _init_schema(conn):
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
    if "admin_ok" not in st.session_state:
        st.session_state.admin_ok = False
    if st.session_state.admin_ok:
        return True
    _, mid, _ = st.columns([1, 1.4, 1])
    with mid:
        st.markdown(f"### 🔒 {title}")
        st.markdown('<p class="subtle">Zugriff nur für Team-Leads.</p>', unsafe_allow_html=True)
        entered = st.text_input("Passwort", type="password", label_visibility="collapsed", placeholder="Passwort eingeben")
        if st.button("Anmelden", use_container_width=True, type="primary"):
            if entered == pw:
                st.session_state.admin_ok = True
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


def page_checkin():
    st.markdown("# Willkommen zum wöchentlichen Pulse-Check")
    st.markdown('<p class="subtle">Deine Antworten sind vollständig anonym. Dauer: ca. 30 Sekunden.</p>', unsafe_allow_html=True)

    conn = get_db()
    cur = conn.cursor()
    params = st.query_params
    gruppe_param = params.get("gruppe", "")
    pseudo_param = params.get("pseudo", "")

    cur.execute("SELECT DISTINCT gruppe FROM teilnehmer ORDER BY gruppe")
    gruppen = [r[0] for r in cur.fetchall()]

    c1, c2 = st.columns(2)
    with c1:
        if gruppe_param and gruppe_param in gruppen:
            gruppe = gruppe_param
            st.text_input("Gruppe", value=gruppe, disabled=True)
        elif gruppen:
            gruppe = st.selectbox("Gruppe", gruppen)
        else:
            gruppe = st.text_input("Gruppe")
    with c2:
        if pseudo_param:
            pseudo = pseudo_param
            st.text_input("Pseudonym", value=pseudo, disabled=True)
        else:
            pseudo = st.text_input("Dein Pseudonym")

    if not pseudo or not gruppe:
        st.markdown('<div class="alert-warn">Bitte Pseudonym und Gruppe angeben.</div>', unsafe_allow_html=True)
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
        token = hash_pseudo(pseudo)
        cur.execute(
            """INSERT INTO pulse_checks (anon_token, gruppe, stimmung, workload, kommunikation)
               VALUES (%s, %s, %s, %s, %s)""",
            [token, gruppe, st.session_state["stimmung_sel"], st.session_state["workload_sel"], st.session_state["komm_sel"]]
        )
        cur.close()
        st.session_state["stimmung_sel"] = None
        st.session_state["workload_sel"] = None
        st.session_state["komm_sel"] = None
        st.markdown('<div class="alert-ok">🎉 Danke für dein Feedback — bis nächste Woche!</div>', unsafe_allow_html=True)
        components.html("""
<div id="confetti-root"></div>
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


def line_area_chart(x, y, color="#2563EB", y_range=(1, 5)):
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=x, y=y, mode='lines+markers',
        line=dict(color=color, width=3, shape='spline', smoothing=0.8),
        fill='tozeroy',
        fillcolor=f"rgba{tuple(int(color.lstrip('#')[i:i+2], 16) for i in (0, 2, 4)) + (0.18,)}",
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
    gruppen = [r[0] for r in cur.fetchall()]
    if not gruppen:
        st.markdown("# Gruppen-Dashboard")
        st.info("Noch keine Daten vorhanden.")
        return

    idx = gruppen.index(gruppe_param) if gruppe_param and gruppe_param in gruppen else 0
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

    weekly = df.groupby("woche").agg(
        stimmung_avg=("stimmung", "mean"),
        kommunikation_avg=("kommunikation", "mean"),
        count=("stimmung", "count")
    ).reset_index()

    current = weekly.iloc[-1] if len(weekly) > 0 else None
    previous = weekly.iloc[-2] if len(weekly) > 1 else None

    k1, k2, k3 = st.columns(3)
    delta_s = (current["stimmung_avg"] - previous["stimmung_avg"]) if previous is not None else None
    delta_k = (current["kommunikation_avg"] - previous["kommunikation_avg"]) if previous is not None else None
    kpi_card(k1, f"{current['stimmung_avg']:.1f}", "Ø Stimmung", delta_s)
    kpi_card(k2, f"{current['kommunikation_avg']:.1f}", "Ø Kommunikation", delta_k)
    kpi_card(k3, f"{int(current['count'])}", "Antworten letzte Woche", None)

    st.markdown("### Stimmungsverlauf")
    st.plotly_chart(line_area_chart(weekly["woche"], weekly["stimmung_avg"]), use_container_width=True)

    st.markdown("### Kommunikationsverlauf")
    st.plotly_chart(line_area_chart(weekly["woche"], weekly["kommunikation_avg"], color="#10B981"), use_container_width=True)

    st.markdown("### Workload-Verteilung")
    wl_counts = df["workload"].value_counts()
    wl_colors = {"zu_wenig": "#F59E0B", "passt": "#10B981", "zu_viel": "#EF4444"}
    wl_labels = {"zu_wenig": "Zu wenig", "passt": "Passt", "zu_viel": "Zu viel"}
    fig = go.Figure()
    for wl in ["zu_wenig", "passt", "zu_viel"]:
        count = int(wl_counts.get(wl, 0))
        if count == 0:
            continue
        fig.add_trace(go.Bar(
            y=[""], x=[count], name=wl_labels[wl],
            orientation='h', marker_color=wl_colors[wl],
            text=[f"{count}"], textposition='inside', insidetextanchor='middle',
            hovertemplate=f"{wl_labels[wl]}: %{{x}}<extra></extra>",
        ))
    fig.update_layout(**plotly_layout(
        barmode='stack', height=130,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        margin=dict(l=10, r=10, t=10, b=40),
        legend=dict(orientation='h', yanchor='top', y=-0.1, xanchor='center', x=0.5),
    ))
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


def page_verwaltung():
    if not admin_check("Verwaltung"):
        return

    st.markdown("# Verwaltung")
    st.markdown('<p class="subtle">Teilnehmer, Gruppen und Reminder</p>', unsafe_allow_html=True)

    conn = get_db()
    cur = conn.cursor()

    st.markdown("### Teilnehmer hinzufügen")
    with st.container():
        with st.form("add_teilnehmer", clear_on_submit=True):
            c1, c2, c3, c4 = st.columns([3, 3, 4, 2])
            with c1:
                pseudo = st.text_input("Pseudonym", placeholder="z.B. Roter Falke")
            with c2:
                gruppe = st.text_input("Gruppe", placeholder="z.B. Alpha")
            with c3:
                email = st.text_input("E-Mail", placeholder="name@firma.de")
            with c4:
                st.markdown('<div style="height: 28px"></div>', unsafe_allow_html=True)
                submit = st.form_submit_button("Hinzufügen", use_container_width=True, type="primary")
            if submit:
                if pseudo and gruppe and email:
                    try:
                        cur.execute(
                            "INSERT INTO teilnehmer (pseudo, gruppe, email) VALUES (%s, %s, %s)",
                            [pseudo.strip(), gruppe.strip(), email.strip()]
                        )
                        st.success(f"**{pseudo}** zur Gruppe **{gruppe}** hinzugefügt.")
                        st.rerun()
                    except psycopg2.errors.UniqueViolation:
                        conn.rollback()
                        st.error("Teilnehmer existiert bereits in dieser Gruppe.")
                else:
                    st.warning("Alle Felder ausfüllen.")

    st.markdown("### Teilnehmer")
    cur.execute("SELECT pseudo, gruppe, email, active FROM teilnehmer ORDER BY gruppe, pseudo")
    tcols = [d[0] for d in cur.description]
    teilnehmer = pd.DataFrame(cur.fetchall(), columns=tcols)
    if teilnehmer.empty:
        st.info("Keine Teilnehmer vorhanden.")
    else:
        gruppen_list = sorted(teilnehmer["gruppe"].unique())
        tabs = st.tabs(gruppen_list)
        for i, gname in enumerate(gruppen_list):
            with tabs[i]:
                gt = teilnehmer[teilnehmer["gruppe"] == gname].reset_index(drop=True)
                for _, row in gt.iterrows():
                    c1, c2, c3 = st.columns([3, 4, 1.2])
                    dot_class = "on" if row["active"] else "off"
                    status_text = "Aktiv" if row["active"] else "Inaktiv"
                    c1.markdown(f'<span class="status-dot {dot_class}"></span>{row["pseudo"]} <span style="color:#64748B;font-size:12px;margin-left:6px;">{status_text}</span>', unsafe_allow_html=True)
                    c2.markdown(f'<span style="color:#94A3B8">{row["email"]}</span>', unsafe_allow_html=True)
                    if row["active"]:
                        if c3.button("Deaktivieren", key=f"deact_{row['pseudo']}_{row['gruppe']}", use_container_width=True):
                            cur.execute(
                                "UPDATE teilnehmer SET active = false WHERE pseudo = %s AND gruppe = %s",
                                [row["pseudo"], row["gruppe"]]
                            )
                            st.rerun()

    st.markdown("### Reminder senden")
    cur.execute("SELECT DISTINCT gruppe FROM teilnehmer WHERE active = true ORDER BY gruppe")
    reminder_gruppen = [r[0] for r in cur.fetchall()]
    if not reminder_gruppen:
        st.info("Keine aktiven Teilnehmer vorhanden.")
        return

    sel_gruppen = st.multiselect("Gruppen", reminder_gruppen, default=reminder_gruppen[:1] if reminder_gruppen else [])

    if sel_gruppen:
        cur.execute(
            "SELECT gruppe, MAX(sent_at), SUM(count) FROM reminder_log WHERE gruppe = ANY(%s) GROUP BY gruppe",
            [sel_gruppen]
        )
        last_rows = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
        for g in sel_gruppen:
            if g in last_rows and last_rows[g][0]:
                days_ago = (datetime.now() - last_rows[g][0]).days
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
                smtp = smtplib.SMTP_SSL("smtp.gmail.com", 465)
                smtp.login(gmail_user, gmail_pass)
            except Exception as e:
                st.error(f"SMTP-Fehler: {e}")
                return

            for g in sel_gruppen:
                cur.execute(
                    "SELECT pseudo, email FROM teilnehmer WHERE gruppe = %s AND active = true",
                    [g]
                )
                empfaenger = cur.fetchall()
                sent_count = 0
                for pseudo, email in empfaenger:
                    link = f"{app_url}?gruppe={g}&pseudo={pseudo}"
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
                        all_errors.append(f"{g} / {email}: {e}")
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


PAGES = {
    "Check-In": page_checkin,
    "Gruppen-Dashboard": page_gruppen_dashboard,
    "Gesamt-Dashboard": page_gesamt_dashboard,
    "Verwaltung": page_verwaltung,
}

st.set_page_config(page_title="Stimmungsbarometer", page_icon="🌡️", layout="wide", initial_sidebar_state="expanded")
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

with st.sidebar:
    st.markdown('<div class="sidebar-brand"><span class="brand-icon">🌡️</span><span class="brand-name">Stimmungsbarometer</span></div>', unsafe_allow_html=True)
    page = st.radio("Navigation", list(PAGES.keys()), label_visibility="collapsed")

PAGES[page]()
