# IMPROVEMENTS.md

Analyse-Stand: 2026-04-23. Basis: `app.py` (845 LOC), `core.py` (408 LOC), `seed.py` (150 LOC), `requirements.txt`, `.streamlit/`. Scope: Verbesserungen im aktuellen Stack (Streamlit + Supabase Postgres + Gmail SMTP).

## 1. Codebase-Overview

Drei Python-Dateien, gesamt ~1.400 LOC, ein Entry-Point `app.py` mit Sidebar-Routing auf sechs Seiten. `core.py` bündelt Konstanten, DB-Connection, SMTP, CSS, Plotly-Layouts und gemeinsame UI-Helpers; `app.py` enthält alle Seiten und ihre Sub-Renderer (`_anm_*`, `_chk_*`, `_dg_*`, `_dgg_*`, `_reg_*`, `_grp_*`, `_tn_*`, `_ci_*`, `_rem_*` Präfixe). Architektur: Flat, kein Paket, keine Tests, kein Migrations-Framework — Schema wird on-the-fly in `_init_schema` erstellt. Community-Detection (stimmung-baro-render/demo/hash) ist directory-basiert und liefert keine Kohäsions-Signale bei 3 Dateien.

## 2. Kritische Issues

**Stored XSS über E-Mail-Feld in Admin-Ansichten.** [app.py:481](app.py#L481) und [app.py:670](app.py#L670) rendern `email` über `st.markdown(..., unsafe_allow_html=True)`. `EMAIL_RE` in [core.py:13](core.py#L13) (`^[^@\s]+@[^@\s]+\.[^@\s]+$`) erlaubt `<`, `>`, `"`. Ein Self-Registrant kann `<img src=x onerror=...>@x.y` hinterlegen; der Admin bekommt beim Öffnen von Registrierungen/Teilnehmer-Liste Code-Execution im Admin-Session-Context. Fix: `html.escape(email)` vor dem Einsetzen oder EMAIL_RE verschärfen auf RFC-konform (`[A-Za-z0-9._%+-]+@...`).

**DSGVO-Consent wird nicht persistiert.** [app.py:128-131](app.py#L128-L131): Consent-Checkbox steuert nur `disabled` am Submit-Button, es wird weder Zeitpunkt noch Version in `registrierungen` geschrieben. Rechenschaftspflicht (Art. 7 Abs. 1 DSGVO) nicht erfüllt. Fix: Spalte `consented_at TIMESTAMP`, `consent_version VARCHAR` in `registrierungen` + in `_anm_store` setzen.

**Geteilte psycopg2-Connection über alle Sessions.** [core.py:139-143](core.py#L139-L143) `@st.cache_resource` liefert dieselbe Connection für alle gleichzeitigen Streamlit-Sessions. Streamlit bedient parallele User in Threads; eine Connection ist in psycopg2 nicht für gleichzeitige Cursor-Operationen sicher, bei 20-60 Usern drohen verschränkte Queries / Protokollfehler. Fix: `psycopg2.pool.ThreadedConnectionPool` mit `getconn`/`putconn` pro Request, oder `@st.cache_resource` auf einen Pool statt auf eine Connection.

**Admin-Login ohne Brute-Force-Schutz.** [core.py:385](core.py#L385) vergleicht per `entered == pw` (nicht konstanzeitig), kein Lockout, kein Rate-Limit, kein Log. Angreifer kann mit einem Skript im Sekundentakt gegen `ADMIN_PASS` raten. Fix: `hmac.compare_digest`, Counter pro `st.session_state` mit exponentiellem Delay ab Versuch 3.

**`conn.rollback()` im Autocommit-Modus ist No-Op.** [app.py:660](app.py#L660) in `_tn_add_form` fängt `UniqueViolation` und ruft `conn.rollback()` — [core.py:142](core.py#L142) hat `autocommit = True` gesetzt, Rollback bleibt ohne Wirkung. Bei anderen Fehlern können halb-ausgeführte Statements inkonsistent bleiben, weil die nachfolgende Schreib-Logik nicht sauber aufgesetzt ist. Fix: Autocommit abschalten und explizite Transaktionen in kritischen Flows (Freigabe, Umbenennung).

## 3. Bugs & Correctness

**TOCTOU beim wöchentlichen Submit.** [app.py:206](app.py#L206) prüft per `_chk_already_submitted`, [app.py:216](app.py#L216) inserted. Zwei schnelle Klicks in derselben Session / zweite Tab-Öffnung erzeugen zwei Zeilen. Fix: Partial Unique Index auf `(anon_token, date_trunc('week', submitted_at))` in Postgres + `ON CONFLICT DO NOTHING`.

**Hash-Drift zwischen Seed und Live.** [seed.py:25](seed.py#L25) `hash_pseudo(pseudo)` (1 Arg) vs [core.py:97](core.py#L97) `hash_pseudo(pseudo, gruppe)` (2 Args). Historische Seed-Rows haben andere `anon_token`-Werte als echte Check-Ins derselben Person. Heute irrelevant weil Tokens nicht person-crossweek korreliert werden, Feature wie „Teilnahme-Quote pro Person" bräche sofort. Fix: `seed.py` importiert `hash_pseudo` aus `core`.

**`_init_schema` + `_seed_demo_if_missing` laufen bei jedem Page-Render.** [core.py:152-153](core.py#L152-L153) rufen beide aus `get_db` heraus, das jede Seite aufruft. DDL (`CREATE TABLE IF NOT EXISTS`), `ALTER TABLE ADD COLUMN IF NOT EXISTS` und `UPDATE teilnehmer SET token = ... WHERE token IS NULL` sind bei jedem Reload Roundtrips zu Supabase. Fix: einmaliger Guard via `@st.cache_resource def _bootstrap(): ...` oder Flag in `st.session_state["_schema_ready"]`.

**Gestreute Cursor-Leaks.** Nur [app.py:177](app.py#L177) schließt den Cursor. Die übrigen Seiten (`page_anmeldung`, `page_gruppen_dashboard`, `page_gesamt_dashboard`, `page_verwaltung`, Sub-Renderer) öffnen Cursor ohne `close` / `with`. Bei vielen Sessions führt das zu kumulativem Speicher auf der gemeinsamen Connection. Fix: `with conn.cursor() as cur:` durchgängig.

**`_anm_find_existing` prüft nicht alle gewählten Gruppen einzeln.** [app.py:94-97](app.py#L94-L97) nutzt `ANY(%s)` und gibt nur die erste Treffer-Gruppe zurück. User kriegt nicht zu sehen, in welcher seiner 3 gewählten Gruppen er schon registriert ist, und die andere(n) zu-viel-gewählte(n) Gruppe(n) werden nicht gemeldet. Fix: alle Treffer zurückgeben und im `st.error` auflisten, oder pro Gruppe unabhängig inserten mit `ON CONFLICT (email, wunschgruppe) DO NOTHING`.

**Silent Fail beim Registrierungs-Mail.** [app.py:146-148](app.py#L146-L148) `except Exception: pass`. Wenn SMTP-Versand fehlschlägt, sieht der User trotzdem Erfolg, kriegt aber nie die zweite Mail. Fix: `st.warning` mit Klartext „Anmeldung gespeichert, Bestätigungsmail fehlgeschlagen — melde dich bei $ADMIN_CONTACT".

**Cache-Clear auf Connection-Reconnect ist zahnlos.** [core.py:149-151](core.py#L149-L151): `st.cache_resource.clear()` löscht den gecachten Wert, aber die bereits im Aufrufer referenzierte `conn` ist immer noch die tote. Eine zweite Session könnte die neue bekommen, aber die aktuelle bleibt auf der toten hängen bis der nächste Page-Render. Fix: `_get_conn.clear()` direkt auf die dekorierte Funktion + neue Connection in derselben Request-Kette zurückgeben.

## 4. Code-Qualität & Wartbarkeit

**45-Zeilen-CSS-String inline in `core.py`.** [core.py:39-83](core.py#L39-L83). Kein Syntax-Highlight, kein Tooling. Fix: auslagern nach `.streamlit/style.css` + einmal lesen + markdown.

**Schema-Migration vermischt mit `CREATE TABLE IF NOT EXISTS`.** [core.py:167-169](core.py#L167-L169) enthält Einmal-Migration (`ALTER TABLE ADD COLUMN`, `UPDATE WHERE NULL`) direkt neben DDL. Kein Versionsbegriff, kein Rollback. Für 6 Tabellen reicht eine einfache `schema_version`-Tabelle + nummerierte `.sql`-Dateien in `migrations/`, ausgeführt in einem Bootstrap-Skript.

**Ad-hoc Demo-Filterung an vielen Stellen.** `filter_out_demo` / `sort_demo_last` / `is_reserved_group` (in [core.py:100-109](core.py#L100-L109)) werden inkonsistent eingesetzt — z.B. [app.py:299-300](app.py#L299-L300) wendet `sort_demo_last` an, aber `_dg_resolve_access` in [app.py:226-238](app.py#L226-L238) enthält Demo-Logik erneut inline. Fix: eine Access-Resolver-Funktion mit klarem Contract, Demo-Gruppe als Feature-Flag oder als Bool-Spalte `is_demo` in `gruppen`.

**`_grp_list` mischt CRUD-Aktionen, Pending-States und Rendering auf 65 Zeilen.** [app.py:547-611](app.py#L547-L611). Jeder neue Zustand (rename-pending, delete-pending) blähte die Funktion. Fix: State-Maschine pro Zeile in eigene Funktion ziehen.

**`seed.py` dupliziert DDL.** [seed.py:65-94](seed.py#L65-L94) schreibt `pulse_checks`/`teilnehmer`/`reminder_log`-DDL, aber ohne `gruppen` und `registrierungen` — drift-anfällig. Fix: Seed ruft `core._init_schema(conn)`.

## 5. Performance

**Keine Query-Caches.** Nur `@st.cache_resource` auf Connection (siehe §2). Keine `@st.cache_data` auf die Pandas-Loader ([app.py:250-262](app.py#L250-L262), [app.py:325-331](app.py#L325-L331)). Bei 20-60 Usern, die das Dashboard offen halten, bedeutet jeder Rerun eine volle `SELECT submitted_at, stimmung, kommunikation, workload FROM pulse_checks WHERE gruppe = %s`. Fix: `@st.cache_data(ttl=60)` auf `_dg_load_weekly` und `_dgg_load_data` mit gruppe/zeitraum als Cache-Key.

**Keine `st.fragment`.** Jeder Admin-Klick (Deaktivieren, Reminder senden, Gruppe umbenennen) führt zu Full-Page-Rerun inklusive aller Tabs und deren DB-Queries. Fix: `@st.fragment` um die Tab-Render-Funktionen — reine CRUD-Aktionen bleiben im Fragment, Parent-Rerun entfällt.

**Client-seitige Zeitraum-Filterung.** [app.py:342-343](app.py#L342-L343) lädt alle Rows und filtert mit Pandas `df >= cutoff`. Bei Kilobytes heute okay, aber trivial server-seitig per `WHERE submitted_at >= %s` in [app.py:328](app.py#L328) lösbar.

**N+1 in Reminder-Collection.** [app.py:767-772](app.py#L767-L772) macht einen `SELECT` pro Gruppe. Fix: ein `WHERE gruppe = ANY(%s) AND active = true ORDER BY gruppe` + Group-by im Python.

## 6. DX & Operability

**Null Logging.** Kein `logging`-Setup, keine `print`s in app.py. Fehler werden per `st.error` in der UI gezeigt und sind weg, sobald der User weg navigiert. Fix: `logging.basicConfig(level=INFO)` + `logger.exception` in den Catch-Blöcken; Supabase hat Logs, aber SMTP/Consent/Admin-Login-Fehler laufen dort nicht ein.

**Kein Health-Check / kein Status.** Kein Weg zu sehen, ob DB und SMTP konfiguriert/erreichbar sind ohne Page-Reload. Fix: `admin_check`-interne Statusleiste mit „DB: ok / SMTP: n/a" basierend auf `secret()`-Probe + `smtplib.SMTP_SSL(...).noop()`.

**Ungepinnte Dependencies.** [requirements.txt](requirements.txt) hat `streamlit>=1.32`, dazu psycopg2-binary/plotly/pandas ohne Version. In einem laufenden Rollout ist das ein Risiko: Streamlit 1.40+ hat Breaking Changes bei `st.query_params` (seit 1.30 non-deprecated, okay), aber pandas 3.0 wird `to_datetime`-Defaults ändern. Fix: Lock via `pip-compile` oder `uv pip compile` → `requirements.lock`.

**SMTP-Host/Provider-Mismatch.** [core.py:27](core.py#L27) setzt `SMTP_HOST = "mail.gmx.net"`, [README.md:30-31](README.md#L30-L31) redet von Gmail + App-Passwort. Das ist ein DX-Killer beim Setup: wer nach README vorgeht, bekommt Auth-Fehler. Fix: Host in `secrets.toml`, `smtp.gmail.com:465` als Default.

**Keine `.env`-Unterstützung / kein Docker.** Lokale Entwicklung zwingt zur `.streamlit/secrets.toml` auch für CLI-Aufrufe (`seed.py` nutzt `os.environ`, nicht `st.secrets` — inkonsistent). Fix: zentrale `config.py`, die aus env-Variablen mit Fallback auf `st.secrets` lädt.

**Null Tests.** Bei 1.400 LOC mit Live-Nutzern und Schema-Drift-Risiko fehlen selbst Smoke-Tests für `valid_name` / `valid_email` / `hash_pseudo` / `firma_of`.

## 7. Top 5 konkrete Quick-Wins

1. **Email-Escape in Admin-Views** — [app.py:481](app.py#L481), [app.py:670](app.py#L670). `html.escape(email)` einsetzen. **15 min**. Schließt Stored-XSS, der einzige ACE-Pfad im Repo.
2. **Consent persistieren** — Spalte `consented_at` in `registrierungen`, in `_anm_store` mit `datetime.now()` setzen, im Impressum Consent-Version als Konstante. **30 min**. Ohne das ist die Datenerhebung DSGVO-angreifbar.
3. **Bootstrap-Guard für `_init_schema`/`_seed_demo`** — einmaliger `@st.cache_resource` um beide Aufrufe. [core.py:145-154](core.py#L145-L154). **15 min**. Spart pro Page-Render ~6 Supabase-Roundtrips.
4. **SMTP-Host in Secrets ziehen** — `SMTP_HOST`/`SMTP_PORT` aus `secrets.toml`, README-Tabelle aktualisieren. [core.py:27-28](core.py#L27-L28). **15 min**. Behebt offensichtlichen README-Widerspruch, erlaubt Gmail/GMX/andere.
5. **Requirements pinnen** — `pip freeze > requirements.txt` oder `uv pip compile`. **10 min**. Macht Deployments reproduzierbar.

**2-Stunden-Budget-Empfehlung:** Die Quick-Wins 1–5 zusammen ≈ 1h 25min. Die restlichen 35min in den **Partial Unique Index** gegen Doppelsubmissions (Finding §3, TOCTOU) — ein einziges `CREATE UNIQUE INDEX CONCURRENTLY idx_pulse_week ON pulse_checks(anon_token, date_trunc('week', submitted_at))` per psql + `ON CONFLICT DO NOTHING` in `_chk_submit`.

## 8. Was ich nicht vorschlage und warum

**Kein Rust-/FastAPI-/Django-Rewrite.** 1.400 LOC, 20–60 User, Live-Rollout läuft, Team ist 1 Person + Rollout-Hilfe. Ein Stack-Wechsel bedeutet Wochen an Re-Implementierung plus neuer Operability-Fragen (Auth, Sessions, Templates, Deployment). Streamlit liefert die fachliche Funktion in dieser Größe sauber.

**Kein Alembic / Django-Migrations.** Für 6 Tabellen mit ~2 erwarteten Änderungen pro Semester lohnt sich kein Migrations-Framework. Eine `schema_version`-Tabelle plus nummerierte `.sql`-Dateien reicht, falls §4 überhaupt angegangen wird.

**Kein SSO/OAuth-Login.** Token-per-URL ist für anonymen Pulse-Check der richtige Flow; SSO würde Anonymität untergraben und den Aufwand vervielfachen.

**Kein dediziertes Admin-User-Modell.** 1–2 Team-Leads, Single-Password ist die richtige Ebene. Statt RBAC reicht der in §2 vorgeschlagene Brute-Force-Schutz.

**Kein DuckLake / Analytics-Warehouse / Data-Lake.** Daten sind in Kilobyte-Größe, Dashboards laufen auf direkter SQL-Aggregation in Sekundenbruchteilen. Der ganze Stack ist an der Stelle overkill.

**Keine Auslagerung von Email auf SES/Postmark/Brevo.** Solange Versand ≤50 Mails/Woche bleibt, ist Gmail/GMX günstiger und ausreichend. Umstellung lohnt ab ≥500/Woche oder Delivery-Problemen.
