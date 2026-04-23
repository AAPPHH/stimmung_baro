import html
import io
import zipfile
from datetime import datetime, timedelta
import pandas as pd
import plotly.graph_objects as go
import psycopg2
import streamlit as st
from core import (
    APP_VERSION, CONSENT_VERSION, CUSTOM_CSS, DEMO_GRUPPE, KOMM_OPTIONS,
    MIN_GROUP_SIZE, NAV_ANMELDUNG, NAV_CHECKIN, NAV_DASH_GESAMT,
    NAV_DASH_GRUPPE, NAV_IMPRESSUM, NAV_VERWALTUNG, NEW_GROUP_OPT, PAGE_ICON,
    PAGE_TITLE, STIMMUNG_OPTIONS, WL_SCORE, WORKLOAD_OPTIONS,
    admin_check, alert_ok, alert_warn, choice_row, filter_out_demo, firma_of,
    get_db, groups_by_firma, hash_pseudo, info_box, is_reserved_group,
    kpi_card, line_area_chart, list_firmen, list_groups, page_header,
    plotly_layout, secret, send_registration_confirmation, send_reminder_batch,
    send_welcome_email, sort_demo_last, valid_email, valid_name,
    wl_label, workload_gauge,
)

KEINE_AUSWAHL = "— keine —"
FIXED_REG_KEYS = ["reg_vorname", "reg_email", "reg_pseudo", "reg_consent"]

def _datenschutz_body():
    st.markdown("### Welche Daten werden erhoben")
    st.markdown(
        "- **Vorname** (bei der Anmeldung)\n"
        "- **E-Mail-Adresse** (für den Versand des persönlichen Check-In-Links und wöchentlicher Erinnerungen)\n"
        "- **Pseudonym** (wird im Dashboard anstelle des Klarnamens angezeigt)\n"
        "- **Stimmungsdaten** (Stimmung, Workload, Kommunikation — gespeichert mit einem anonymen Hash des Pseudonyms, nicht mit Klarnamen oder E-Mail verknüpft)"
    )
    st.markdown("### Zweck der Verarbeitung")
    st.markdown("Die Daten werden ausschließlich für anonymes Team-Feedback im Rahmen von Lehrveranstaltungen genutzt. Ziel ist, Stimmungstrends in Gruppen sichtbar zu machen, nicht einzelne Personen zu bewerten.")
    st.markdown("### Speicherort")
    st.markdown("Die Daten werden auf **Supabase** (PostgreSQL) in der **EU-Region** gespeichert. Es erfolgt **keine Weitergabe an Dritte**.")
    st.markdown("### Löschung")
    st.markdown("Auf Anfrage per E-Mail an **john.hamelmann@study.hs-duesseldorf.de** werden alle zu einer E-Mail-Adresse gehörenden Daten unverzüglich gelöscht.")
    st.markdown("### Tracking")
    st.markdown("Kein Tracking, keine Cookies, keine Analytics.")

def page_impressum():
    st.markdown("# Impressum & Datenschutz")
    st.markdown("## Impressum")
    st.markdown("**John Hamelmann**\nHochschule Düsseldorf\nMünsterstraße 156\n40476 Düsseldorf\n\nE-Mail: john.hamelmann@study.hs-duesseldorf.de")
    st.markdown("### Kontext")
    st.markdown("Studentisches Projekt im Rahmen des Studiengangs **Data Science (B.Sc.)** an der Hochschule Düsseldorf. Betreut durch **Prof. Dr. Dennis Müller** und **Prof. Dr. Dominik Austermann**.")
    st.markdown("Beim Rollout unterstützt durch **Yannik Huber** (Studiengang Data Science, HSD) — E-Mail: yannik.huber@study.hs-duesseldorf.de.")
    st.divider()
    st.markdown("## Datenschutzerklärung")
    _datenschutz_body()

def _anm_firma_key(firma):
    return f"reg_wg_{firma}"

def _anm_all_keys(firmen):
    return FIXED_REG_KEYS + [_anm_firma_key(f) for f in firmen]

def _anm_reset(firmen):
    st.session_state["registration_done"] = False
    st.session_state.pop("registration_mail_err", None)
    for k in _anm_all_keys(firmen):
        st.session_state.pop(k, None)

def _anm_render_success(firmen):
    page_header("Anmeldung zum Stimmungsbarometer")
    alert_ok("🎉 Danke für deine Anmeldung. Sobald der Administrator dich freigibt, bekommst du pro Gruppe einen eigenen Check-In-Link per E-Mail.")
    mail_err = st.session_state.get("registration_mail_err")
    if mail_err:
        alert_warn(f"Bestätigungsmail konnte nicht versendet werden: {html.escape(str(mail_err))}. Deine Registrierung ist trotzdem gespeichert — der Admin sieht sie.")
    if st.button("Neue Anmeldung"):
        _anm_reset(firmen)
        st.rerun()

def _anm_render_picker(gruppen_nach_firma):
    firmen = list(gruppen_nach_firma.keys())
    cols = st.columns(len(firmen)) if firmen else []
    gewaehlte = []
    for col, firma in zip(cols, firmen):
        with col:
            options = [KEINE_AUSWAHL] + gruppen_nach_firma[firma]
            wahl = st.selectbox(f"Projekt bei {firma}", options, key=_anm_firma_key(firma),
                                help=f"Wähle dein Projekt bei {firma}, oder lasse leer wenn du dort nicht arbeitest.")
            if wahl != KEINE_AUSWAHL:
                gewaehlte.append(wahl)
    return gewaehlte

def _anm_validate(vorname, email, pseudo, gewaehlte):
    if not vorname or not email or not pseudo:
        return "Bitte Vorname, E-Mail und Pseudonym ausfüllen."
    if not gewaehlte:
        return "Bitte wähle mindestens ein Projekt aus."
    if not valid_name(vorname):
        return "Ungültiger Vorname. Nur Buchstaben, Zahlen, Leerzeichen und Bindestriche erlaubt."
    if not valid_email(email):
        return "Ungültige E-Mail-Adresse."
    if not valid_name(pseudo):
        return "Ungültiges Pseudonym. Nur Buchstaben, Zahlen, Leerzeichen und Bindestriche erlaubt."
    return None

def _anm_find_existing(cur, email, gruppen):
    cur.execute("SELECT wunschgruppe FROM registrierungen WHERE email = %s AND wunschgruppe = ANY(%s)", [email, gruppen])
    row = cur.fetchone()
    return row[0] if row else None

def _anm_store(cur, vorname, email, pseudo, gruppen):
    now = datetime.now()
    for g in gruppen:
        cur.execute("""INSERT INTO registrierungen
            (vorname, email, wunschgruppe, pseudo, consented_at, consent_version)
            VALUES (%s, %s, %s, %s, %s, %s)""",
            [vorname, email, g, pseudo, now, CONSENT_VERSION])

def page_anmeldung():
    conn = get_db()
    cur = conn.cursor()
    gruppen_nach_firma = groups_by_firma(cur)
    existing_groups = [g for gs in gruppen_nach_firma.values() for g in gs]
    firmen = list(gruppen_nach_firma.keys())
    if st.session_state.get("registration_done"):
        _anm_render_success(firmen)
        return
    page_header("Anmeldung zum Stimmungsbarometer",
                "Registriere dich für den wöchentlichen Pulse-Check. Du erhältst nach Freigabe pro Gruppe deinen persönlichen Link per E-Mail.")
    info_box("ℹ️ Deine Antworten werden anonym gespeichert. Deine E-Mail-Adresse wird ausschließlich für wöchentliche Erinnerungen verwendet und nicht an Dritte weitergegeben.")
    if not existing_groups:
        alert_warn("Derzeit sind keine Projekte verfügbar. Bitte wende dich an den Administrator.")
        return
    c1, c2 = st.columns(2)
    with c1:
        vorname = st.text_input("Vorname", placeholder="Dein Vorname", key="reg_vorname")
    with c2:
        email = st.text_input("E-Mail", placeholder="name@beispiel.de", key="reg_email")
    pseudo = st.text_input("Pseudonym", placeholder="Wird im Dashboard statt deinem Klarnamen angezeigt", key="reg_pseudo")
    st.markdown("#### Deine Projekte")
    st.caption("Wähle dein Projekt pro Firma. Mindestens eines muss ausgewählt sein.")
    gewaehlte = _anm_render_picker(gruppen_nach_firma)
    consent = st.checkbox("Ich stimme der Verarbeitung meiner Daten gemäß der Datenschutzerklärung zu", key="reg_consent")
    st.button("📄 Datenschutzerklärung ansehen",
              on_click=lambda: st.session_state.update(nav=NAV_IMPRESSUM),
              key="anm_to_impressum", help="Öffnet die Seite Impressum & Datenschutz")
    if not st.button("Anmeldung absenden", use_container_width=True, type="primary", disabled=not consent, key="reg_submit"):
        return
    vorname_c = vorname.strip()
    email_c = email.strip()
    pseudo_c = pseudo.strip()
    err = _anm_validate(vorname_c, email_c, pseudo_c, gewaehlte)
    if err:
        st.error(err)
        return
    bereits = _anm_find_existing(cur, email_c, gewaehlte)
    if bereits:
        st.error(f"Du bist bereits für **{bereits}** angemeldet. Wähle nur Projekte aus, für die du dich noch nicht registriert hast.")
        return
    _anm_store(cur, vorname_c, email_c, pseudo_c, gewaehlte)
    mail_ok, mail_err = send_registration_confirmation(vorname_c, email_c)
    st.session_state["registration_done"] = True
    st.session_state["registration_mail_err"] = None if mail_ok else mail_err
    for k in _anm_all_keys(firmen):
        st.session_state.pop(k, None)
    st.rerun()

def _chk_lookup(cur, token):
    cur.execute("SELECT pseudo, gruppe, active FROM teilnehmer WHERE token = %s", [token])
    return cur.fetchone()

def _chk_already_submitted(cur, anon_token):
    iso = datetime.now().isocalendar()
    cur.execute("""SELECT 1 FROM pulse_checks WHERE anon_token = %s
        AND EXTRACT(ISOYEAR FROM submitted_at) = %s AND EXTRACT(WEEK FROM submitted_at) = %s""",
        [anon_token, iso.year, iso.week])
    return cur.fetchone() is not None

def _chk_render_choices():
    st.markdown("### Wie ist deine Stimmung?")
    choice_row("stimmung_sel", STIMMUNG_OPTIONS, 5, lambda o: (o[1], o[2]))
    st.markdown("### Wie ist dein Workload?")
    choice_row("workload_sel", WORKLOAD_OPTIONS, 3, lambda o: (f"{o[1]}  {o[2]}", ""))
    st.markdown("### Wie gut ist die Kommunikation im Team?")
    choice_row("komm_sel", KOMM_OPTIONS, 5, lambda o: (str(o[0]), o[1]))

def _chk_submit(cur, gruppe, anon_token):
    try:
        cur.execute("""INSERT INTO pulse_checks (anon_token, gruppe, stimmung, workload, kommunikation)
            VALUES (%s, %s, %s, %s, %s)""",
            [anon_token, gruppe, st.session_state["stimmung_sel"], st.session_state["workload_sel"], st.session_state["komm_sel"]])
    except psycopg2.errors.UniqueViolation:
        pass
    cur.close()
    st.session_state["stimmung_sel"] = None
    st.session_state["workload_sel"] = None
    st.session_state["komm_sel"] = None
    alert_ok("🎉 Danke für dein Feedback — bis nächste Woche!")

def _chk_render(token):
    page_header("Wöchentlicher Pulse-Check")
    conn = get_db()
    cur = conn.cursor()
    admin_contact = secret("ADMIN_CONTACT") or secret("GMAIL_USER") or "den Administrator"
    row = _chk_lookup(cur, token)
    if not row:
        alert_warn(
            f"Ungültiger oder abgelaufener Link.<br><br>"
            f"<b>Was du jetzt tun kannst:</b><br>"
            f"• Prüfe, ob du den vollständigen Link aus der Einladungsmail kopiert hast (manche Mail-Clients kürzen).<br>"
            f"• Falls dein Zugang deaktiviert wurde, wende dich an: <b>{admin_contact}</b><br>"
            f"• Oder registriere dich neu unter „Anmeldung“ im Menü."
        )
        return
    pseudo, gruppe, active = row
    if not active:
        alert_warn(f"Dein Zugang wurde deaktiviert. Bei Fragen: <b>{admin_contact}</b>")
        return
    st.markdown(f'<div class="greeting">Hallo <b>{pseudo}</b> — Gruppe <b>{gruppe}</b></div>', unsafe_allow_html=True)
    st.markdown('<p class="subtle">Deine Antworten sind vollständig anonym. Dauer: ca. 30 Sekunden.</p>', unsafe_allow_html=True)
    info_box("ℹ️ Deine Antworten werden anonym gespeichert. Deine E-Mail-Adresse wird ausschließlich für wöchentliche Erinnerungen verwendet und nicht an Dritte weitergegeben.")
    anon_token = hash_pseudo(pseudo, gruppe)
    if _chk_already_submitted(cur, anon_token):
        alert_ok("✅ Du hast diese Woche bereits teilgenommen. Nächster Check-In ab Montag.")
        return
    _chk_render_choices()
    st.divider()
    can_submit = all([st.session_state.get("stimmung_sel"), st.session_state.get("workload_sel"), st.session_state.get("komm_sel")])
    _, mid, _ = st.columns([1, 1.2, 1])
    with mid:
        submit = st.button("Absenden", type="primary", use_container_width=True, disabled=not can_submit)
    if submit and can_submit:
        _chk_submit(cur, gruppe, anon_token)

def page_checkin():
    token = st.query_params.get("token", "").strip()
    if not token:
        page_header("Wöchentlicher Pulse-Check")
        alert_warn("Bitte nutze den Link aus deiner Einladungsmail.")
        return
    _chk_render(token)

def _dg_resolve_access(cur, token, is_admin):
    user_gruppe = None
    if token:
        cur.execute("SELECT gruppe FROM teilnehmer WHERE token = %s AND active = true", [token])
        row = cur.fetchone()
        if row:
            user_gruppe = row[0]
    if is_admin:
        cur.execute("""SELECT DISTINCT p.gruppe FROM pulse_checks p
            INNER JOIN gruppen g ON g.name = p.gruppe ORDER BY p.gruppe""")
        return sort_demo_last([r[0] for r in cur.fetchall()]), user_gruppe, True
    if user_gruppe:
        return sort_demo_last(list({user_gruppe, DEMO_GRUPPE})), user_gruppe, True
    return [DEMO_GRUPPE], None, False

def _dg_pick_active(gruppen, user_gruppe):
    gruppe_param = st.query_params.get("gruppe", "")
    if gruppe_param and gruppe_param in gruppen:
        return gruppen.index(gruppe_param)
    if user_gruppe and user_gruppe in gruppen:
        return gruppen.index(user_gruppe)
    if DEMO_GRUPPE in gruppen:
        return gruppen.index(DEMO_GRUPPE)
    return 0

def _dg_load_weekly(cur, gruppe):
    cur.execute("SELECT submitted_at, stimmung, kommunikation, workload FROM pulse_checks WHERE gruppe = %s ORDER BY submitted_at", [gruppe])
    cols = [d[0] for d in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=cols)
    if df.empty:
        return df, None
    df["submitted_at"] = pd.to_datetime(df["submitted_at"])
    df["woche"] = df["submitted_at"].dt.to_period("W").dt.start_time
    df["wl_score"] = df["workload"].map(WL_SCORE)
    weekly = df.groupby("woche").agg(
        stimmung_avg=("stimmung", "mean"), kommunikation_avg=("kommunikation", "mean"),
        wl_avg=("wl_score", "mean"), count=("stimmung", "count")).reset_index()
    return df, weekly

def _dg_render_kpis(weekly):
    current = weekly.iloc[-1]
    previous = weekly.iloc[-2] if len(weekly) > 1 else None
    delta_s = (current["stimmung_avg"] - previous["stimmung_avg"]) if previous is not None else None
    delta_k = (current["kommunikation_avg"] - previous["kommunikation_avg"]) if previous is not None else None
    delta_wl = (current["wl_avg"] - previous["wl_avg"]) if previous is not None else None
    wl_cur = float(current["wl_avg"])
    k1, k2, k3, k4 = st.columns(4)
    kpi_card(k1, f"{current['stimmung_avg']:.1f}", "Ø Stimmung", delta_s)
    kpi_card(k2, f"{current['kommunikation_avg']:.1f}", "Ø Kommunikation", delta_k)
    kpi_card(k3, f"{int(current['count'])}", "Antworten letzte Woche", None)
    kpi_card(k4, f"{wl_cur:+.2f}", f"Workload-Balance · {wl_label(wl_cur)}", delta_wl)
    return wl_cur

def page_gruppen_dashboard():
    conn = get_db()
    cur = conn.cursor()
    token = st.query_params.get("token", "").strip()
    is_admin = st.session_state.get("auth_admin", False)
    allowed, user_gruppe, has_access = _dg_resolve_access(cur, token, is_admin)
    if not has_access:
        page_header("Gruppen-Dashboard")
        alert_warn(
            "Dieses Dashboard zeigt Daten nur für <b>deine eigene Gruppe</b>. "
            "Zugang bekommst du automatisch durch den Link in deiner Einladungsmail.<br><br>"
            "<b>Noch nicht angemeldet?</b> Registriere dich im Menü unter „Anmeldung“ — "
            "danach schaltet der Administrator dich frei und du bekommst deinen Zugangslink."
        )
        _, mid, _ = st.columns([1, 1, 1])
        with mid:
            st.button("📋 Jetzt registrieren", use_container_width=True, type="primary",
                      on_click=lambda: st.session_state.update(nav=NAV_ANMELDUNG),
                      key="dash_gruppe_goto_anmeldung")
        st.markdown("---")
        st.caption("Unterhalb siehst du Beispiel-Daten aus der Demo-Gruppe.")
    cur.execute("SELECT DISTINCT gruppe FROM pulse_checks WHERE gruppe = ANY(%s) ORDER BY gruppe", [allowed])
    gruppen = sort_demo_last([r[0] for r in cur.fetchall()])
    if not gruppen:
        page_header("Gruppen-Dashboard")
        st.info("Noch keine Daten vorhanden.")
        return
    idx = _dg_pick_active(gruppen, user_gruppe)
    top1, top2 = st.columns([3, 1])
    with top2:
        gruppe = st.selectbox("Gruppe", gruppen, index=idx, label_visibility="collapsed") if len(gruppen) > 1 else gruppen[0]
    with top1:
        page_header(gruppe, "Stimmungsverlauf und Team-Signale")
    df, weekly = _dg_load_weekly(cur, gruppe)
    if df.empty or weekly is None or weekly.empty:
        st.info("Keine Daten für diese Gruppe.")
        return
    wl_cur = _dg_render_kpis(weekly)
    st.markdown("### Stimmungsverlauf")
    st.plotly_chart(line_area_chart(weekly["woche"], weekly["stimmung_avg"]), use_container_width=True)
    st.markdown("### Kommunikationsverlauf")
    st.plotly_chart(line_area_chart(weekly["woche"], weekly["kommunikation_avg"], color="#10B981"), use_container_width=True)
    st.markdown("### Workload-Balance")
    st.plotly_chart(workload_gauge(wl_cur), use_container_width=True)

ZEITRAUM_OPTIONS = {"Letzte 4 Wochen": 4, "Letzte 8 Wochen": 8, "Letzte 12 Wochen": 12, "Alle": None}

def _dgg_load_data():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""SELECT p.gruppe, p.submitted_at, p.stimmung, p.kommunikation, p.workload, g.firma
        FROM pulse_checks p INNER JOIN gruppen g ON g.name = p.gruppe
        ORDER BY p.submitted_at""")
    cols = [d[0] for d in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=cols)
    if not df.empty:
        df["firma"] = df["firma"].fillna(df["gruppe"].apply(firma_of))
    return conn, cur, df

def _dgg_render_filters(df):
    f_firma, f_zeit, _ = st.columns([1, 1, 2])
    with f_firma:
        firmen = sorted(df["firma"].dropna().unique())
        sel_firma = st.selectbox("Firma", ["Alle"] + list(firmen), index=0)
    with f_zeit:
        zeitraum = st.selectbox("Zeitraum", list(ZEITRAUM_OPTIONS.keys()), index=1)
    weeks = ZEITRAUM_OPTIONS[zeitraum]
    if weeks:
        cutoff = datetime.now() - timedelta(weeks=weeks)
        df = df[df["submitted_at"] >= cutoff]
    if sel_firma != "Alle":
        df = df[df["firma"] == sel_firma]
    return df, sel_firma

def _dgg_collapse_small(df):
    sizes = df.groupby("gruppe")["stimmung"].count()
    kleine = sizes[sizes < MIN_GROUP_SIZE].index.tolist()
    df = df.copy()
    df["gruppe_display"] = df["gruppe"].apply(lambda g: "Sonstige" if g in kleine else g)
    return df

def _dgg_render_vergleich(df):
    st.markdown("### Gruppenvergleich")
    avg_by_group = df.groupby("gruppe_display")["stimmung"].mean().sort_values()
    fig = go.Figure(go.Bar(
        x=avg_by_group.values, y=avg_by_group.index, orientation='h',
        marker=dict(color=avg_by_group.values, colorscale='RdYlGn', cmin=1, cmax=5, line=dict(color='#334155', width=1)),
        text=[f"{v:.1f}" for v in avg_by_group.values], textposition='outside',
        hovertemplate='%{y}: Ø %{x:.2f}<extra></extra>'))
    fig.update_layout(**plotly_layout(xaxis=dict(range=[0, 5.5], title="Ø Stimmung"),
                                      height=max(240, 60 * len(avg_by_group) + 80), showlegend=False))
    st.plotly_chart(fig, use_container_width=True)

def _dgg_render_heatmap(df):
    st.markdown("### Heatmap: Gruppe × Kalenderwoche")
    df = df.copy()
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
        text=text, texttemplate="%{text}", textfont=dict(color='#0F172A', size=13),
        hovertemplate='%{y} • %{x}<br>Ø %{z:.2f}<extra></extra>',
        colorbar=dict(thickness=12, tickcolor='#334155', tickfont=dict(color='#F1F5F9'))))
    fig.update_layout(**plotly_layout(height=max(260, 55 * len(pivot.index) + 80),
                                      xaxis=dict(showgrid=False), yaxis=dict(showgrid=False)))
    st.plotly_chart(fig, use_container_width=True)

def _dgg_render_fruehwarnung(df):
    st.markdown("### Frühwarnung")
    df = df.copy()
    df["woche"] = df["submitted_at"].dt.to_period("W").dt.start_time
    weekly_stats = df.groupby(["gruppe_display", "woche"])["stimmung"].agg(["mean", "count"]).reset_index()
    trends = {}
    for g in weekly_stats["gruppe_display"].unique():
        gdata = weekly_stats[weekly_stats["gruppe_display"] == g].sort_values("woche")
        if len(gdata) >= 2:
            last_two = gdata.iloc[-2:]
            if (last_two["count"] >= 5).all():
                trends[g] = last_two["mean"].iloc[-1] - last_two["mean"].iloc[-2]
    if not trends:
        st.info("Nicht genug Antworten pro Woche für aussagekräftige Frühwarnung (benötigt: min. 5 Antworten pro Woche).")
        return
    worst = min(trends, key=trends.get)
    delta = trends[worst]
    if delta < -0.7:
        alert_warn(f"⚠️ <b>{worst}</b> zeigt einen deutlichen Abwärtstrend (Δ {delta:.2f}). Jetzt handeln.")
    elif delta < 0:
        alert_warn(f"⚡ <b>{worst}</b> leicht rückläufig (Δ {delta:.2f}). Beobachten.")
    else:
        alert_ok("✅ Alle Gruppen stabil oder im Aufwärtstrend.")

def _dgg_render_antwortrate(cur, df):
    st.markdown("### Teilnehmerzahl & Antwortrate")
    cur.execute("SELECT gruppe, COUNT(*) as total FROM teilnehmer WHERE active = true GROUP BY gruppe")
    tcols = [d[0] for d in cur.description]
    teilnehmer_df = pd.DataFrame(cur.fetchall(), columns=tcols)
    if teilnehmer_df.empty:
        st.info("Keine Teilnehmer in der Verwaltung hinterlegt.")
        return
    letzte_woche = datetime.now() - timedelta(weeks=1)
    antworten = df[df["submitted_at"] >= letzte_woche].groupby("gruppe")["stimmung"].count().reset_index()
    antworten.columns = ["gruppe", "antworten"]
    merged = teilnehmer_df.merge(antworten, on="gruppe", how="left").fillna(0)
    merged["antworten"] = merged["antworten"].astype(int)
    merged["rate"] = (merged["antworten"] / merged["total"]).clip(upper=1.0)
    st.dataframe(
        merged.rename(columns={"gruppe": "Gruppe", "total": "Teilnehmer", "antworten": "Antworten", "rate": "Rate"}),
        hide_index=True, use_container_width=True,
        column_config={"Rate": st.column_config.ProgressColumn("Antwortrate", format="%.0f%%", min_value=0, max_value=1)})

def page_gesamt_dashboard():
    if not admin_check("Gesamt-Dashboard"):
        return
    page_header("Übersicht aller Gruppen", "Organisationsweite Stimmung, Heatmap und Frühwarnungen")
    conn, cur, df = _dgg_load_data()
    if df.empty:
        st.info("Noch keine Daten vorhanden.")
        return
    df["submitted_at"] = pd.to_datetime(df["submitted_at"])
    df, sel_firma = _dgg_render_filters(df)
    if df.empty:
        st.info("Keine Daten im gewählten Zeitraum.")
        return
    df = _dgg_collapse_small(df)
    _dgg_render_vergleich(df)
    _dgg_render_heatmap(df)
    _dgg_render_fruehwarnung(df)
    _dgg_render_antwortrate(cur, df)

def _reg_gruppe_options(existing_groups, wunschgruppe):
    if wunschgruppe in existing_groups:
        options = [NEW_GROUP_OPT] + existing_groups if existing_groups else [NEW_GROUP_OPT]
        default_idx = existing_groups.index(wunschgruppe) + 1
    else:
        options = [NEW_GROUP_OPT, wunschgruppe] + [g for g in existing_groups if g != wunschgruppe]
        default_idx = 1
    return options, default_idx

def _reg_approve(cur, rid, pseudo, email, final_gruppe):
    cur.execute("SELECT 1 FROM teilnehmer WHERE pseudo = %s AND gruppe = %s", [pseudo, final_gruppe])
    if cur.fetchone():
        st.error(f"**{pseudo}** existiert bereits in Gruppe **{final_gruppe}**.")
        return
    _ensure_gruppe_tracked(cur, final_gruppe)
    cur.execute("INSERT INTO teilnehmer (pseudo, gruppe, email) VALUES (%s, %s, %s) RETURNING token",
                [pseudo, final_gruppe, email])
    token = cur.fetchone()[0]
    cur.execute("UPDATE registrierungen SET status = 'freigegeben' WHERE id = %s", [rid])
    ok, err = send_welcome_email(pseudo, email, token)
    if ok:
        st.success(f"**{pseudo}** freigegeben — Willkommensmail versendet.")
    else:
        st.warning(f"Freigegeben, aber Mail-Versand fehlgeschlagen: {err}")
    st.rerun()

def _reg_render_row(cur, existing_groups, rid, vorname, email, wunschgruppe, pseudo, created_at):
    with st.container():
        st.markdown('<div class="kpi-card" style="padding:16px 20px;">', unsafe_allow_html=True)
        c1, c2, c3 = st.columns([3, 3, 2])
        with c1:
            st.markdown(f"**{vorname}**  ")
            st.markdown(f'<span style="color:#94A3B8;font-size:13px;">{html.escape(email)}</span>', unsafe_allow_html=True)
            st.markdown(f'<span style="color:#64748B;font-size:12px;">angemeldet: {created_at.strftime("%d.%m.%Y %H:%M")}</span>', unsafe_allow_html=True)
        with c2:
            st.markdown('<span style="color:#94A3B8;font-size:13px;">Pseudo</span>', unsafe_allow_html=True)
            st.markdown(f"**{pseudo}**")
            options, default_idx = _reg_gruppe_options(existing_groups, wunschgruppe)
            gruppe_choice = st.selectbox("Gruppe", options, index=default_idx, key=f"reg_gruppe_{rid}")
            new_gruppe_in = st.text_input("Neuer Gruppenname", placeholder="z.B. Delta",
                                          disabled=(gruppe_choice != NEW_GROUP_OPT), key=f"reg_newg_{rid}")
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
                    _reg_approve(cur, rid, pseudo, email, final_gruppe)
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown('<div style="height: 10px"></div>', unsafe_allow_html=True)

def tab_registrierungen(conn, cur):
    existing_groups = list_groups(cur)
    cur.execute("SELECT id, vorname, email, wunschgruppe, pseudo, created_at FROM registrierungen WHERE status = 'ausstehend' ORDER BY created_at")
    pending = cur.fetchall()
    if not pending:
        st.info("Keine ausstehenden Registrierungen.")
        return
    st.caption(f"{len(pending)} ausstehende Anmeldung(en)")
    for rid, vorname, email, wunschgruppe, pseudo, created_at in pending:
        _reg_render_row(cur, existing_groups, rid, vorname, email, wunschgruppe, pseudo, created_at)

def _ensure_gruppe_tracked(cur, name):
    firma = name.split(" · ", 1)[0].strip() if " · " in name else None
    if firma:
        cur.execute("INSERT INTO firmen (name) VALUES (%s) ON CONFLICT DO NOTHING", [firma])
    cur.execute("""INSERT INTO gruppen (name, firma) VALUES (%s, %s)
        ON CONFLICT (name) DO UPDATE SET firma = COALESCE(gruppen.firma, EXCLUDED.firma)""",
        [name, firma])

def _firma_add_form(cur):
    st.markdown("#### Firma anlegen")
    with st.form("add_firma", clear_on_submit=True):
        c1, c2 = st.columns([3, 1])
        with c1:
            new_name = st.text_input("Firmenname", placeholder="z.B. Aptiv", label_visibility="collapsed")
        with c2:
            submit = st.form_submit_button("Anlegen", use_container_width=True, type="primary")
        if not submit:
            return
        name = new_name.strip()
        if not name:
            st.error("Bitte einen Firmennamen eingeben.")
        elif not valid_name(name):
            st.error("Ungültiger Firmenname. Nur Buchstaben, Zahlen, Leerzeichen und Bindestriche erlaubt.")
        elif name in list_firmen(cur):
            st.error(f"Firma **{name}** existiert bereits.")
        else:
            cur.execute("INSERT INTO firmen (name) VALUES (%s) ON CONFLICT DO NOTHING", [name])
            st.success(f"Firma **{name}** angelegt.")
            st.rerun()

def _firma_rename(cur, old_name, new_name):
    cur.execute("UPDATE firmen SET name = %s WHERE name = %s", [new_name, old_name])
    cur.execute("UPDATE gruppen SET firma = %s WHERE firma = %s", [new_name, old_name])

def _firma_list(cur):
    st.markdown("#### Firmen-Liste")
    firmen = list_firmen(cur)
    if not firmen:
        st.info("Noch keine Firmen vorhanden. Oben eine hinzufügen.")
        return
    cur.execute("SELECT firma, COUNT(*) FROM gruppen WHERE firma IS NOT NULL GROUP BY firma")
    counts = dict(cur.fetchall())
    pending_del = st.session_state.get("pending_del_f")
    pending_rename = st.session_state.get("pending_rename_f")
    for fname in firmen:
        count = counts.get(fname, 0)
        if pending_rename == fname:
            c1, c2, c3 = st.columns([4, 1.2, 1.2])
            with c1:
                new_value = st.text_input(f"Neuer Name für **{fname}**", value=fname,
                                          key=f"f_rename_input_{fname}", label_visibility="collapsed")
            save_clicked = c2.button("💾 Speichern", key=f"f_rn_ok_{fname}", use_container_width=True, type="primary")
            cancel_clicked = c3.button("Abbrechen", key=f"f_rn_x_{fname}", use_container_width=True)
            if cancel_clicked:
                st.session_state.pop("pending_rename_f", None)
                st.rerun()
            if save_clicked:
                new_name = (new_value or "").strip()
                if not new_name:
                    st.error("Bitte einen Firmennamen eingeben.")
                elif new_name == fname:
                    st.error("Neuer Name ist identisch mit dem alten.")
                elif not valid_name(new_name):
                    st.error("Ungültiger Firmenname. Nur Buchstaben, Zahlen, Leerzeichen und Bindestriche erlaubt.")
                elif new_name in list_firmen(cur):
                    st.error(f"Firma **{new_name}** existiert bereits.")
                else:
                    try:
                        _firma_rename(cur, fname, new_name)
                    except Exception as e:
                        st.error(f"Umbenennung fehlgeschlagen: {e}")
                    else:
                        st.session_state.pop("pending_rename_f", None)
                        st.success(f"Firma umbenannt: **{fname}** → **{new_name}**. Projekt-Namen (<code>{fname} · X</code>) wurden nicht automatisch mit umbenannt.")
                        st.rerun()
            continue
        c1, c2, c3, c4 = st.columns([3, 2, 1.4, 1.4])
        c1.markdown(f"**{fname}**")
        c2.markdown(f'<span style="color:#94A3B8">{count} Projekt(e)</span>', unsafe_allow_html=True)
        if pending_del == fname:
            if c3.button("Bestätigen", key=f"f_ok_{fname}", use_container_width=True, type="primary"):
                cur.execute("DELETE FROM firmen WHERE name = %s", [fname])
                st.session_state.pop("pending_del_f", None)
                st.rerun()
            if c4.button("Abbrechen", key=f"f_x_{fname}", use_container_width=True):
                st.session_state.pop("pending_del_f", None)
                st.rerun()
        else:
            if c3.button("✏️ Umbenennen", key=f"f_rn_{fname}", use_container_width=True):
                st.session_state["pending_rename_f"] = fname
                st.rerun()
            if count == 0:
                if c4.button("🗑 Löschen", key=f"f_del_{fname}", use_container_width=True):
                    st.session_state["pending_del_f"] = fname
                    st.rerun()
            else:
                c4.caption("nur leere Firmen löschbar")

def tab_firmen(conn, cur):
    info_box("🏢 <b>Firmen</b> sind Container für Projekte. Lege erst eine Firma an, dann im Tab <b>🏷️ Projekte</b> die zugehörigen Projekte. Teilnehmer sehen bei der Anmeldung pro Firma eine eigene Auswahl.")
    _firma_add_form(cur)
    _firma_list(cur)

def _grp_rename(cur, old_name, new_name):
    cur.execute("UPDATE teilnehmer SET gruppe = %s WHERE gruppe = %s", [new_name, old_name])
    cur.execute("UPDATE pulse_checks SET gruppe = %s WHERE gruppe = %s", [new_name, old_name])
    cur.execute("UPDATE reminder_log SET gruppe = %s WHERE gruppe = %s", [new_name, old_name])
    cur.execute("UPDATE registrierungen SET wunschgruppe = %s WHERE wunschgruppe = %s", [new_name, old_name])
    cur.execute("UPDATE gruppen SET name = %s WHERE name = %s", [new_name, old_name])

def _grp_add_form(cur):
    firmen_list = list_firmen(cur)
    st.markdown("#### Projekt anlegen")
    if not firmen_list:
        alert_warn("Es gibt noch keine Firmen. Lege zuerst im Tab <b>🏢 Firmen</b> eine Firma an, danach kannst du ihr Projekte zuordnen.")
        return
    with st.form("add_gruppe", clear_on_submit=True):
        c1, c2, c3 = st.columns([2, 3, 1.2])
        with c1:
            sel_firma = st.selectbox("Firma", firmen_list)
        with c2:
            new_proj = st.text_input("Projektname", placeholder="z.B. Intelligent Cockpit")
        with c3:
            st.markdown('<div style="height: 28px"></div>', unsafe_allow_html=True)
            submit = st.form_submit_button("Anlegen", use_container_width=True, type="primary")
        if not submit:
            return
        proj = new_proj.strip()
        full_name = f"{sel_firma} · {proj}" if proj else ""
        if not proj:
            st.error("Bitte einen Projektnamen eingeben.")
        elif not valid_name(full_name):
            st.error("Ungültiger Projektname. Nur Buchstaben, Zahlen, Leerzeichen und Bindestriche erlaubt.")
        elif is_reserved_group(full_name):
            st.error("Dieser Gruppenname ist reserviert.")
        elif full_name in list_groups(cur):
            st.error(f"Gruppe **{full_name}** existiert bereits.")
        else:
            cur.execute("INSERT INTO gruppen (name, firma) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        [full_name, sel_firma])
            st.success(f"Gruppe **{full_name}** angelegt.")
            st.rerun()

def _grp_list(cur):
    st.markdown("#### Gruppen-Liste")
    groups = list_groups(cur)
    if not groups:
        st.info("Noch keine Gruppen vorhanden.")
        return
    cur.execute("SELECT gruppe, COUNT(*) FROM teilnehmer WHERE gruppe <> %s GROUP BY gruppe", [DEMO_GRUPPE])
    counts = dict(cur.fetchall())
    cur.execute("SELECT name, firma FROM gruppen")
    firma_by_name = {n: f for n, f in cur.fetchall()}
    pending_del = st.session_state.get("pending_del_g")
    pending_rename = st.session_state.get("pending_rename_g")
    for gname in groups:
        count = counts.get(gname, 0)
        firma = firma_by_name.get(gname) or "—"
        if pending_rename == gname:
            c1, c2, c3 = st.columns([4, 1.2, 1.2])
            with c1:
                new_value = st.text_input(f"Neuer Name für **{gname}**", value=gname,
                                          key=f"g_rename_input_{gname}", label_visibility="collapsed")
            save_clicked = c2.button("💾 Speichern", key=f"g_rn_ok_{gname}", use_container_width=True, type="primary")
            cancel_clicked = c3.button("Abbrechen", key=f"g_rn_x_{gname}", use_container_width=True)
            if cancel_clicked:
                st.session_state.pop("pending_rename_g", None)
                st.rerun()
            if save_clicked:
                new_name = (new_value or "").strip()
                if not new_name:
                    st.error("Bitte einen Gruppennamen eingeben.")
                elif new_name == gname:
                    st.error("Neuer Name ist identisch mit dem alten.")
                elif not valid_name(new_name):
                    st.error("Ungültiger Gruppenname. Nur Buchstaben, Zahlen, Leerzeichen und Bindestriche erlaubt.")
                elif is_reserved_group(new_name):
                    st.error("Dieser Gruppenname ist reserviert.")
                elif new_name in list_groups(cur):
                    st.error(f"Gruppe **{new_name}** existiert bereits.")
                else:
                    try:
                        _grp_rename(cur, gname, new_name)
                    except Exception as e:
                        st.error(f"Umbenennung fehlgeschlagen: {e}")
                    else:
                        st.session_state.pop("pending_rename_g", None)
                        st.success(f"Gruppe umbenannt: **{gname}** → **{new_name}**")
                        st.rerun()
            continue
        c1, c2, c3, c4 = st.columns([3, 2, 1.4, 1.4])
        c1.markdown(f"**{gname}**  \n<span style='color:#64748B;font-size:12px;'>Firma: {firma}</span>", unsafe_allow_html=True)
        c2.markdown(f'<span style="color:#94A3B8">{count} Teilnehmer</span>', unsafe_allow_html=True)
        if pending_del == gname:
            if c3.button("Bestätigen", key=f"g_ok_{gname}", use_container_width=True, type="primary"):
                cur.execute("DELETE FROM gruppen WHERE name = %s", [gname])
                st.session_state.pop("pending_del_g", None)
                st.rerun()
            if c4.button("Abbrechen", key=f"g_x_{gname}", use_container_width=True):
                st.session_state.pop("pending_del_g", None)
                st.rerun()
        else:
            if c3.button("✏️ Umbenennen", key=f"g_rn_{gname}", use_container_width=True):
                st.session_state["pending_rename_g"] = gname
                st.rerun()
            if count == 0:
                if c4.button("🗑 Löschen", key=f"g_del_{gname}", use_container_width=True):
                    st.session_state["pending_del_g"] = gname
                    st.rerun()
            else:
                c4.caption("nur leere Gruppen löschbar")

def tab_gruppen(conn, cur):
    info_box("🏷️ Projekte gehören immer zu einer <b>Firma</b>. Wähle Firma + Projektname — der Systemname wird automatisch als <code>Firma · Projekt</code> zusammengesetzt.")
    _grp_add_form(cur)
    _grp_list(cur)

def _tn_add_form(conn, cur):
    existing_groups = list_groups(cur)
    options = [NEW_GROUP_OPT] + existing_groups if existing_groups else [NEW_GROUP_OPT]
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
        if not submit:
            return
        gruppe = new_gruppe.strip() if gruppe_sel == NEW_GROUP_OPT else gruppe_sel
        pseudo_c = pseudo.strip()
        email_c = email.strip()
        if not gruppe or not pseudo_c or not email_c:
            st.error("Alle Felder ausfüllen.")
            return
        if not valid_name(gruppe):
            st.error("Ungültiger Gruppenname. Nur Buchstaben, Zahlen, Leerzeichen und Bindestriche erlaubt.")
            return
        if is_reserved_group(gruppe):
            st.error("Dieser Gruppenname ist reserviert.")
            return
        if not valid_name(pseudo_c):
            st.error("Ungültiges Pseudonym. Nur Buchstaben, Zahlen, Leerzeichen und Bindestriche erlaubt.")
            return
        if not valid_email(email_c):
            st.error("Ungültige E-Mail-Adresse.")
            return
        cur.execute("SELECT 1 FROM teilnehmer WHERE pseudo = %s AND gruppe = %s", [pseudo_c, gruppe])
        if cur.fetchone():
            st.error("Dieses Pseudonym ist in dieser Gruppe bereits vergeben.")
            return
        try:
            _ensure_gruppe_tracked(cur, gruppe)
            cur.execute("INSERT INTO teilnehmer (pseudo, gruppe, email) VALUES (%s, %s, %s)", [pseudo_c, gruppe, email_c])
            st.success(f"**{pseudo_c}** zur Gruppe **{gruppe}** hinzugefügt.")
            st.rerun()
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            st.error("Dieses Pseudonym ist in dieser Gruppe bereits vergeben.")

def _tn_render_row(cur, row):
    row_id = f"{row['pseudo']}__{row['gruppe']}"
    pending_del = st.session_state.get("pending_del_t")
    c1, c2, c3, c4 = st.columns([3, 3.2, 1.4, 1.4])
    dot_class = "on" if row["active"] else "off"
    status_text = "Aktiv" if row["active"] else "Inaktiv"
    c1.markdown(f'<span class="status-dot {dot_class}"></span>{row["pseudo"]} <span style="color:#64748B;font-size:12px;margin-left:6px;">{status_text}</span>', unsafe_allow_html=True)
    c2.markdown(f'<span style="color:#94A3B8">{html.escape(row["email"])}</span>', unsafe_allow_html=True)
    if pending_del == row_id:
        if c3.button("Bestätigen", key=f"confirm_del_{row_id}", use_container_width=True, type="primary"):
            cur.execute("DELETE FROM teilnehmer WHERE pseudo = %s AND gruppe = %s", [row["pseudo"], row["gruppe"]])
            st.session_state.pop("pending_del_t", None)
            st.rerun()
        if c4.button("Abbrechen", key=f"cancel_del_{row_id}", use_container_width=True):
            st.session_state.pop("pending_del_t", None)
            st.rerun()
    else:
        if row["active"]:
            if c3.button("Deaktivieren", key=f"deact_{row_id}", use_container_width=True,
                         help="Sperrt Check-Ins, behält historische Daten. Kann später reaktiviert werden."):
                cur.execute("UPDATE teilnehmer SET active = false WHERE pseudo = %s AND gruppe = %s", [row["pseudo"], row["gruppe"]])
                st.rerun()
        if c4.button("🗑 Löschen", key=f"del_{row_id}", use_container_width=True,
                     help="Entfernt den Teilnehmer komplett. Anonymisierte Check-Ins bleiben erhalten."):
            st.session_state["pending_del_t"] = row_id
            st.rerun()

def _tn_list(cur):
    st.markdown("#### Teilnehmer-Liste")
    cur.execute("SELECT pseudo, gruppe, email, active, token FROM teilnehmer WHERE gruppe <> %s ORDER BY gruppe, pseudo", [DEMO_GRUPPE])
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
                _tn_render_row(cur, row)

def tab_teilnehmer(conn, cur):
    _tn_add_form(conn, cur)
    _tn_list(cur)

def _ci_render_row(cur, pseudo_map, cid, submitted_at, anon_token, stimmung, workload, kommunikation):
    pseudo = pseudo_map.get(anon_token)
    is_unknown = pseudo is None
    pseudo_display = pseudo if pseudo else "❓ Unbekannt"
    pending_del = st.session_state.get("pending_del_ci")
    c1, c2, c3, c4, c5 = st.columns([2.5, 2.3, 2, 1.4, 1.4])
    pseudo_label = pseudo_display
    if is_unknown:
        pseudo_label += ' <span style="color:#64748B;font-size:11px;" title="Pseudo konnte nicht zugeordnet werden — z.B. weil der Teilnehmer gelöscht wurde oder aus der Demo-Gruppe stammt">(?)</span>'
    c1.markdown(f'**{pseudo_label}**  \n<span style="color:#94A3B8;font-size:12px;">{submitted_at.strftime("%d.%m.%Y %H:%M")}</span>', unsafe_allow_html=True)
    c2.markdown(f"Stimmung: **{stimmung}**  \nKomm: **{kommunikation}**")
    c3.markdown(f"Workload: **{workload}**")
    if pending_del == cid:
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

def tab_checkins(conn, cur):
    cur.execute("SELECT DISTINCT gruppe FROM pulse_checks ORDER BY gruppe")
    groups = filter_out_demo([r[0] for r in cur.fetchall()])
    if not groups:
        st.info("Noch keine Check-Ins vorhanden.")
        st.caption("Sobald Teilnehmer abgeben, erscheinen hier ihre Einträge.")
        return
    sel_group = st.selectbox("Gruppe", groups, key="ci_group")
    info_box("⚠️ <b>Vorsicht:</b> Einträge hier löschen ist <b>nicht rückgängig</b> zu machen. Nur nutzen, um offensichtlich falsche Abgaben zu entfernen (z.B. Test-Submits). Reguläre Daten nicht anfassen.")
    cur.execute("SELECT pseudo FROM teilnehmer WHERE gruppe = %s", [sel_group])
    pseudo_map = {hash_pseudo(r[0], sel_group): r[0] for r in cur.fetchall()}
    cur.execute("""SELECT id, submitted_at, anon_token, stimmung, workload, kommunikation
        FROM pulse_checks WHERE gruppe = %s ORDER BY submitted_at DESC LIMIT 100""", [sel_group])
    rows = cur.fetchall()
    if not rows:
        st.info("Keine Check-Ins für diese Gruppe.")
        return
    caption = "Neueste Abgabe" if len(rows) == 1 else f"Neueste {len(rows)} Abgaben"
    st.caption(f"{caption} — hier ausschließlich **falsch eingetragene** Abgaben löschen.")
    for cid, submitted_at, anon_token, stimmung, workload, kommunikation in rows:
        _ci_render_row(cur, pseudo_map, cid, submitted_at, anon_token, stimmung, workload, kommunikation)

def _rem_last_sent_info(cur, sel_gruppen):
    cur.execute("SELECT gruppe, MAX(sent_at) FROM reminder_log WHERE gruppe = ANY(%s) GROUP BY gruppe", [sel_gruppen])
    last_rows = {r[0]: r[1] for r in cur.fetchall()}
    for g in sel_gruppen:
        if g in last_rows and last_rows[g]:
            days_ago = (datetime.now() - last_rows[g]).days
            st.caption(f"**{g}** — zuletzt gesendet vor {days_ago} Tag{'en' if days_ago != 1 else ''}")
        else:
            st.caption(f"**{g}** — noch kein Reminder gesendet")

def _rem_collect(cur, sel_gruppen):
    recipients_by_group = {}
    for g in sel_gruppen:
        cur.execute("SELECT pseudo, email, token FROM teilnehmer WHERE gruppe = %s AND active = true", [g])
        recipients_by_group[g] = cur.fetchall()
    return recipients_by_group

def tab_reminder(conn, cur):
    cur.execute("SELECT DISTINCT gruppe FROM teilnehmer WHERE active = true ORDER BY gruppe")
    reminder_gruppen = filter_out_demo([r[0] for r in cur.fetchall()])
    if not reminder_gruppen:
        st.info("Keine aktiven Teilnehmer vorhanden.")
        return
    sel_gruppen = st.multiselect("Gruppen", reminder_gruppen, default=reminder_gruppen,
                                 help="Standardmäßig alle aktiven Gruppen vorausgewählt — abwählen, wenn eine Gruppe nicht benachrichtigt werden soll.")
    if sel_gruppen:
        _rem_last_sent_info(cur, sel_gruppen)
    if not st.button("📧 Reminder jetzt senden", type="primary", disabled=not sel_gruppen):
        return
    with st.spinner("Sende Reminder..."):
        recipients_by_group = _rem_collect(cur, sel_gruppen)
        results, err = send_reminder_batch(recipients_by_group)
    if err and not results:
        st.error(err)
        return
    total_sent = 0
    for g, sent in results.items():
        cur.execute("INSERT INTO reminder_log (gruppe, count) VALUES (%s, %s)", [g, sent])
        total_sent += sent
    alert_ok(f"✅ {total_sent} Reminder an {len(sel_gruppen)} Gruppe(n) gesendet.")
    if err:
        for line in err:
            st.warning(line)

BACKUP_TABLES = [
    ("pulse_checks", "SELECT * FROM pulse_checks ORDER BY submitted_at"),
    ("teilnehmer", "SELECT * FROM teilnehmer ORDER BY gruppe, pseudo"),
    ("registrierungen", "SELECT * FROM registrierungen ORDER BY created_at"),
    ("reminder_log", "SELECT * FROM reminder_log ORDER BY sent_at"),
    ("gruppen", "SELECT * FROM gruppen ORDER BY name"),
]

def _bk_fetch(cur):
    out = {}
    for name, query in BACKUP_TABLES:
        cur.execute(query)
        cols = [d[0] for d in cur.description]
        df = pd.DataFrame(cur.fetchall(), columns=cols)
        out[name] = (df.to_csv(index=False).encode("utf-8"), len(df))
    return out

def _bk_zip(csvs):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, (data, _) in csvs.items():
            zf.writestr(f"{name}.csv", data)
    return buf.getvalue()

def tab_backup(conn, cur):
    info_box("📦 Daten-Export zum Backup oder für externe Auswertung. Enthält personenbezogene Daten aus <code>teilnehmer</code> und <code>registrierungen</code> — gemäß Datenschutzerklärung vertraulich behandeln.")
    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    csvs = _bk_fetch(cur)
    st.download_button("📦 Komplett-Backup (ZIP)", _bk_zip(csvs),
                       f"stimmungsbarometer_backup_{stamp}.zip", mime="application/zip",
                       use_container_width=True, type="primary", key="bk_zip")
    st.markdown("#### Einzelne Tabellen")
    for name, (data, count) in csvs.items():
        c1, c2 = st.columns([3, 1])
        c1.markdown(f"**{name}** · <span style='color:#94A3B8'>{count} Zeilen</span>", unsafe_allow_html=True)
        c2.download_button("⬇ CSV", data, f"{name}_{stamp}.csv",
                           mime="text/csv", key=f"bk_csv_{name}", use_container_width=True)

def page_verwaltung():
    if not admin_check("Verwaltung"):
        return
    page_header("Verwaltung", "Registrierungen, Teilnehmer, Check-Ins, Reminder und Backup")
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM registrierungen WHERE status = 'ausstehend'")
    pending_count = cur.fetchone()[0]
    reg_label = f"📥 Registrierungen ({pending_count})" if pending_count else "📥 Registrierungen"
    tabs = st.tabs([reg_label, "🏢 Firmen", "🏷️ Projekte", "👥 Teilnehmer", "📊 Check-Ins", "📧 Reminder", "📦 Backup"])
    with tabs[0]:
        tab_registrierungen(conn, cur)
    with tabs[1]:
        tab_firmen(conn, cur)
    with tabs[2]:
        tab_gruppen(conn, cur)
    with tabs[3]:
        tab_teilnehmer(conn, cur)
    with tabs[4]:
        tab_checkins(conn, cur)
    with tabs[5]:
        tab_reminder(conn, cur)
    with tabs[6]:
        tab_backup(conn, cur)

PAGES = {
    NAV_ANMELDUNG: page_anmeldung,
    NAV_CHECKIN: page_checkin,
    NAV_DASH_GRUPPE: page_gruppen_dashboard,
    NAV_DASH_GESAMT: page_gesamt_dashboard,
    NAV_VERWALTUNG: page_verwaltung,
    NAV_IMPRESSUM: page_impressum,
}

st.set_page_config(page_title=PAGE_TITLE, page_icon=PAGE_ICON, layout="wide", initial_sidebar_state="expanded")
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
if "nav" not in st.session_state:
    st.session_state.nav = NAV_CHECKIN if st.query_params.get("token", "").strip() else NAV_ANMELDUNG
st.sidebar.title(f"{PAGE_ICON} {PAGE_TITLE}")
st.sidebar.divider()
page = st.sidebar.radio("Navigation", list(PAGES.keys()), key="nav")
if st.session_state.get("auth_admin"):
    if st.sidebar.button("Abmelden", use_container_width=True, key="logout_btn"):
        st.session_state.auth_admin = False
        st.rerun()
st.sidebar.divider()
st.sidebar.caption(APP_VERSION)
PAGES[page]()
