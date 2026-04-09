#!/usr/bin/env python3
"""
Creative KPIs Dashboard Generator
Reads Slack data for the last 3 months, computes KPI metrics,
and writes index.html + data.json to disk.
The GitHub Actions workflow commits and pushes those files.
"""

import json
import os
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import requests
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION  (tokens come from environment variables in GitHub Actions)
# ═══════════════════════════════════════════════════════════════════════════════

SLACK_TOKEN  = os.environ.get("SLACK_TOKEN")
if not SLACK_TOKEN:
    raise ValueError("SLACK_TOKEN environment variable is not set. Add it as a GitHub Actions secret.")
CHANNEL_ID    = "C042J20J3M5"
MANAGER_NAMES = ["Joe", "Gabe", "Alexa"]

# Full team roster — used so Ds/Person always divides by 12
# and every member appears in the drill-down (with 0 if they didn't post)
TEAM_MEMBERS = [
    "Nata", "Nandu", "Dan Howard", "Alex", "Carlos Miras",
    "Anastasia", "Evan Brown", "Krystyna", "Saba Talat",
    "Andrew Kallemeyn", "Olexii Lysenko", "Spencer Arney",
]

TARGETS = {
    "num_ds":         48,
    "ds_per_person":  4,
    "cycles_per_d":   None,   # TBD — update here when decided
    "replies_per_d":  None,
    "response_per_d": None,
}

BH_START = 8   # Business hours start (24h)
BH_END   = 17  # Business hours end   (24h)
NO_RESPONSE_THRESHOLD_BH = 72  # hours

MONTHS     = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
MONTH_KEYS = ["01","02","03","04","05","06","07","08","09","10","11","12"]

# ═══════════════════════════════════════════════════════════════════════════════
#  DATE RANGE  — rolling last 3 complete months
# ═══════════════════════════════════════════════════════════════════════════════

def get_date_range():
    """Return (start, end) covering exactly the last 3 months from today.
    e.g. if today is April 14 → start = January 14, end = April 14 (now)
    """
    today = datetime.now(tz=timezone.utc)
    m, y = today.month - 3, today.year
    if m <= 0:
        m += 12
        y -= 1
    # Same day of month, 3 months back
    try:
        start = datetime(y, m, today.day, 0, 0, 0, tzinfo=timezone.utc)
    except ValueError:
        # Edge case: e.g. May 31 → Feb 31 doesn't exist, use last day of that month
        import calendar
        last_day = calendar.monthrange(y, m)[1]
        start = datetime(y, m, last_day, 0, 0, 0, tzinfo=timezone.utc)
    end = today
    return start, end

# ═══════════════════════════════════════════════════════════════════════════════
#  SLACK — USER FETCHING
# ═══════════════════════════════════════════════════════════════════════════════

def get_all_users(client):
    users, cursor = {}, None
    while True:
        try:
            resp = client.users_list(limit=200, cursor=cursor)
        except SlackApiError as e:
            print(f"  Error fetching users: {e}"); break
        for u in resp.get("members", []):
            if u.get("deleted") or u.get("is_bot") or u["id"] == "USLACKBOT":
                continue
            p  = u.get("profile", {})
            dn = (p.get("display_name") or "").strip()
            rn = (p.get("real_name")    or "").strip()
            users[u["id"]] = {
                "id":           u["id"],
                "name":         rn or u.get("name", "Unknown"),
                "display_name": dn or rn or u.get("name", "Unknown"),
                "tz":           u.get("tz", "America/New_York"),
            }
        cursor = resp.get("response_metadata", {}).get("next_cursor")
        if not cursor: break
    return users


def find_managers(users, manager_names):
    managers = {}
    for uid, u in users.items():
        combined = (u["display_name"] + " " + u["name"]).lower()
        for mname in manager_names:
            if mname.lower() in combined:
                managers[uid] = {**u, "manager_label": mname}
                print(f"  ✓ Manager '{mname}': {u['display_name']} (TZ: {u['tz']})")
                break
    return managers

# ═══════════════════════════════════════════════════════════════════════════════
#  SLACK — MESSAGE FETCHING
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_history(client, channel_id, oldest, latest):
    msgs, cursor = [], None
    while True:
        try:
            r = client.conversations_history(
                channel=channel_id, oldest=str(oldest),
                latest=str(latest), limit=200, cursor=cursor)
        except SlackApiError as e:
            print(f"  Error fetching history: {e}"); break
        msgs.extend(r.get("messages", []))
        if not r.get("has_more"): break
        cursor = r.get("response_metadata", {}).get("next_cursor")
        if not cursor: break
        time.sleep(1)
    return msgs


def fetch_thread(client, channel_id, thread_ts):
    msgs, cursor = [], None
    while True:
        try:
            r = client.conversations_replies(
                channel=channel_id, ts=thread_ts, limit=200, cursor=cursor)
        except SlackApiError as e:
            print(f"  Error fetching thread {thread_ts}: {e}"); break
        msgs.extend(r.get("messages", []))
        if not r.get("has_more"): break
        cursor = r.get("response_metadata", {}).get("next_cursor")
        if not cursor: break
        time.sleep(0.5)
    return msgs  # index 0 = root

# ═══════════════════════════════════════════════════════════════════════════════
#  BUSINESS HOURS
# ═══════════════════════════════════════════════════════════════════════════════

def business_hours_between(start_ts, end_ts, tz_str):
    try:    tz = ZoneInfo(tz_str)
    except: tz = ZoneInfo("America/New_York")
    s = datetime.fromtimestamp(float(start_ts), tz=tz)
    e = datetime.fromtimestamp(float(end_ts),   tz=tz)
    if e <= s: return 0.0
    total, cur = 0.0, s
    while cur < e:
        if cur.weekday() < 5:
            open_  = cur.replace(hour=BH_START, minute=0, second=0, microsecond=0)
            close_ = cur.replace(hour=BH_END,   minute=0, second=0, microsecond=0)
            ws, we = max(cur, open_), min(e, close_)
            if ws < we:
                total += (we - ws).total_seconds() / 3600.0
        cur = (cur + timedelta(days=1)).replace(hour=BH_START, minute=0, second=0, microsecond=0)
    return round(total, 2)

# ═══════════════════════════════════════════════════════════════════════════════
#  DATA PROCESSING
# ═══════════════════════════════════════════════════════════════════════════════

def ts_month(ts):  return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m")
def has(txt, p):   return p.lower() in txt.lower()
def mgr_tags(txt, ids): return [i for i in ids if f"<@{i}>" in txt]
def is_root(m):    return m.get("thread_ts", m.get("ts")) == m.get("ts")


def process_slack(client, channel_id, users, managers, start_dt, end_dt):
    mgr_ids = list(managers.keys())
    print(f"\n  Date range: {start_dt.date()} → {end_dt.date()}")
    all_msgs = fetch_history(client, channel_id, start_dt.timestamp(), end_dt.timestamp())
    print(f"  Total channel messages: {len(all_msgs)}")

    # Candidate roots: "For review:" roots are confirmed deliverables.
    # "For feedback:" roots are candidates — confirmed only if a reply contains "For review:".
    review_roots   = [m for m in all_msgs
                      if is_root(m) and not m.get("subtype") and has(m.get("text",""), "for review:")]
    feedback_roots = [m for m in all_msgs
                      if is_root(m) and not m.get("subtype") and has(m.get("text",""), "for feedback:")
                      and not has(m.get("text",""), "for review:")]  # avoid double-counting
    print(f"  'For review:' roots: {len(review_roots)}")
    print(f"  'For feedback:' roots (pending validation): {len(feedback_roots)}")

    # ── DEBUG: write all "For review:" / "For feedback:" messages to debug.txt ──
    evan_id = next((uid for uid, u in users.items()
                    if "evan" in u.get("display_name","").lower() or
                       "evan" in u.get("name","").lower()), None)
    debug_lines = [
        f"Fetch window: {start_dt.date()} → {end_dt.date()}",
        f"Evan's Slack user ID: {evan_id}",
        f"Total messages fetched: {len(all_msgs)}",
        "",
        f"{'Date':<18} {'Name':<22} {'Phrase':<14} {'Root':<6} {'Subtype':<16} Reason",
        "-" * 90,
    ]
    for m in all_msgs:
        txt = m.get("text","")
        if not (has(txt, "for review:") or has(txt, "for feedback:")):
            continue
        uid   = m.get("user","")
        uname = users.get(uid,{}).get("display_name","?")
        ts    = m.get("ts","")
        tts   = m.get("thread_ts","")
        sub   = m.get("subtype") or "none"
        root  = is_root(m)
        dt    = datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        phrase = "For review:" if has(txt,"for review:") else "For feedback:"
        if not root:
            reason = "SKIPPED — reply sent to channel (thread_ts != ts)"
        elif sub != "none":
            reason = f"SKIPPED — has subtype: {sub}"
        else:
            reason = "COUNTED"
        debug_lines.append(
            f"{dt:<18} {uname:<22} {phrase:<14} {str(root):<6} {sub:<16} {reason}"
        )
    debug_lines.append("")
    debug_lines.append(f"'For review:' roots counted:  {len(review_roots)}")
    debug_lines.append(f"'For feedback:' roots found:  {len(feedback_roots)}")
    with open("debug.txt", "w") as f:
        f.write("\n".join(debug_lines))
    print("  DEBUG output written to debug.txt")

    print(f"\n  Managers being tracked for response time:")
    for mid, m in managers.items():
        print(f"    {m['manager_label']}: id={mid} tz={m['tz']}")

    month_data = {}
    skipped_managers = 0
    feedback_confirmed = 0
    feedback_skipped   = 0

    # Combine: confirmed roots first, then feedback candidates
    all_candidate_roots = [(root, "review") for root in review_roots] + \
                          [(root, "feedback") for root in feedback_roots]

    for root, root_type in all_candidate_roots:
        rts   = root["ts"]
        month = ts_month(rts)
        pid   = root.get("user","")
        pu    = users.get(pid, {})
        pname = pu.get("display_name") or pu.get("name","Unknown")

        # Skip deliverables posted by managers — they are reviewers only
        if pid in managers:
            skipped_managers += 1
            continue

        thread  = fetch_thread(client, channel_id, rts)
        replies = thread[1:]

        # For "For feedback:" roots, confirm at least one reply has "For review:"
        if root_type == "feedback":
            has_review_reply = any(has(m.get("text",""), "for review:") for m in replies)
            if not has_review_reply:
                feedback_skipped += 1
                continue
            feedback_confirmed += 1

        cycles, other = [], []
        for msg in replies:
            txt = msg.get("text","")
            if has(txt, "for feedback:") or has(txt, "for review:"):
                cycles.append({"ts": msg["ts"], "user": msg.get("user",""), "tagged": mgr_tags(txt, mgr_ids)})
            else:
                other.append(msg)

        cycle_data = []
        for cyc in cycles:
            cts, tagged = float(cyc["ts"]), cyc["tagged"]
            resp_time, resp_mgr = None, None
            tagged_names = [managers.get(t, {}).get("display_name", t) for t in tagged]

            # Any manager can respond — find first manager reply after this cycle
            best_ts, best_mgr = None, None
            for msg in replies:
                mts = float(msg["ts"])
                if mts > cts and msg.get("user") in mgr_ids:
                    if best_ts is None or mts < best_ts:
                        best_ts, best_mgr = mts, msg["user"]

            if best_ts and best_mgr:
                bh = business_hours_between(cts, best_ts, managers[best_mgr]["tz"])
                mgr_label = managers[best_mgr].get("manager_label", best_mgr)
                print(f"    CYCLE → tagged={tagged_names} | responder={mgr_label} "
                      f"| biz_hrs={bh} | excluded={'YES (>72h)' if bh > NO_RESPONSE_THRESHOLD_BH else 'no'}")
                if bh <= NO_RESPONSE_THRESHOLD_BH:
                    resp_time, resp_mgr = bh, best_mgr
            else:
                print(f"    CYCLE → tagged={tagged_names} | NO MANAGER RESPONSE FOUND")

            cycle_data.append({
                "ts": cyc["ts"], "tagged": tagged,
                "response_time_hours": resp_time,
                "responding_manager_id": resp_mgr,
            })

        month_data.setdefault(month, []).append({
            "root_ts": rts, "month": month,
            "poster_id": pid, "poster_name": pname,
            "cycle_count": len(cycles), "reply_count": len(other),
            "cycles": cycle_data,
        })

    print(f"  Skipped {skipped_managers} deliverables posted by managers")
    print(f"  'For feedback:' roots confirmed as deliverables: {feedback_confirmed}")
    print(f"  'For feedback:' roots skipped (no 'For review:' reply): {feedback_skipped}")
    return month_data


def compute_metrics(month_data, managers):
    result = {}
    for month, deliverables in month_data.items():
        n = len(deliverables)
        if n == 0:
            result[month] = {"num_ds":0,"ds_per_person":0,"cycles_per_d":0,
                             "replies_per_d":0,"response_per_d":None,"drill":{}}
            continue

        pd, pc, pr = {}, {}, {}
        for d in deliverables:
            p = d["poster_name"]
            pd[p] = pd.get(p,0)+1; pc[p] = pc.get(p,0)+d["cycle_count"]; pr[p] = pr.get(p,0)+d["reply_count"]

        tc = sum(d["cycle_count"]  for d in deliverables)
        tr = sum(d["reply_count"]  for d in deliverables)

        mgr_times = {}
        for d in deliverables:
            for c in d["cycles"]:
                if c["response_time_hours"] is not None:
                    mid   = c["responding_manager_id"]
                    label = managers.get(mid,{}).get("manager_label", mid)
                    mgr_times.setdefault(label,[]).append(c["response_time_hours"])

        all_t    = [t for ts in mgr_times.values() for t in ts]
        avg_resp = round(sum(all_t)/len(all_t),1) if all_t else None
        mgr_avgs = {k: round(sum(v)/len(v),1) for k,v in mgr_times.items()}

        sd = lambda d: dict(sorted(d.items(), key=lambda x:-x[1]))
        sa = lambda d: dict(sorted(d.items(), key=lambda x: x[1]))

        # Ds/Person always divides by the full team (12 members), not just those who posted.
        # All 12 members appear in the drill-down; non-posters show 0.
        full_team_ds = {name: pd.get(name, 0) for name in TEAM_MEMBERS}
        # Also include anyone who posted but isn't in the hardcoded list (edge case)
        for name, count in pd.items():
            if name not in full_team_ds:
                full_team_ds[name] = count

        result[month] = {
            "num_ds":        n,
            "ds_per_person": round(n / len(TEAM_MEMBERS), 2),
            "cycles_per_d":  round(tc/n,2),
            "replies_per_d": round(tr/n,2),
            "response_per_d": avg_resp,
            "drill": {
                "num_ds":        sd(full_team_ds),
                "ds_per_person": sd(full_team_ds),
                "cycles_per_d":  sd({k:round(pc[k]/pd[k],2) for k in pd}),
                "replies_per_d": sd({k:round(pr[k]/pd[k],2) for k in pd}),
                "response_per_d": sa(mgr_avgs),
            }
        }
    return result

# ═══════════════════════════════════════════════════════════════════════════════
#  HTML GENERATION
# ═══════════════════════════════════════════════════════════════════════════════

METRIC_DEFS = [
    {"key":"num_ds",        "label":"# Deliverables",          "section":"deliverables"},
    {"key":"ds_per_person", "label":"Deliverables / Person",   "section":"deliverables"},
    {"key":"cycles_per_d",  "label":"Cycles / Deliverable",    "section":"deliverables"},
    {"key":"replies_per_d", "label":"Replies / Deliverable",   "section":"deliverables"},
    {"key":"response_per_d","label":"Avg. Response Time",      "section":"response"},
]

# Info panel content — shown when a metric title is clicked
METRIC_INFO = {
    "num_ds": {
        "label": "# Deliverables",
        "definition": "Total number of creative pieces submitted for review by the team in a given month.",
        "formula": "Count of qualifying Slack threads posted that month.",
        "rules": [
            "A thread qualifies if the root message contains \u201cFor review:\u201d",
            "OR the root contains \u201cFor feedback:\u201d and at least one reply contains \u201cFor review:\u201d",
            "Multiple \u201cFor review:\u201d messages in the same thread still count as 1 deliverable",
            "Only counts deliverables posted by team members \u2014 manager posts are excluded",
            "Counted in the month the root message was posted",
        ],
    },
    "ds_per_person": {
        "label": "Deliverables / Person",
        "definition": "How many deliverables each team member submitted on average.",
        "formula": "Total deliverables \u00f7 12 (full team size)",
        "rules": [
            "Always divided by all 12 team members, even if someone posted 0 that month",
            "Click any monthly value to see the per-person breakdown, including zeros",
        ],
    },
    "cycles_per_d": {
        "label": "Cycles / Deliverable",
        "definition": "On average, how many review rounds a deliverable goes through before completion. Lower is better \u2014 fewer cycles means faster approvals.",
        "formula": "Total cycles \u00f7 Total deliverables",
        "rules": [
            "A cycle = any message in the thread (after the root) containing \u201cFor feedback:\u201d or \u201cFor review:\u201d",
            "The root message is never counted as a cycle",
            "Click any monthly value to see cycles per person\u2019s deliverables",
        ],
    },
    "replies_per_d": {
        "label": "Replies / Deliverable",
        "definition": "Average number of discussion messages per deliverable thread, excluding cycle messages.",
        "formula": "Total non-cycle replies \u00f7 Total deliverables",
        "rules": [
            "Replies = any thread message that does not contain \u201cFor feedback:\u201d or \u201cFor review:\u201d",
            "Includes comments, questions, and general feedback discussion",
            "Click any monthly value to see replies per person\u2019s deliverables",
        ],
    },
    "response_per_d": {
        "label": "Avg. Response Time",
        "definition": "How quickly a manager first responds to a review or feedback request, measured in business hours. Lower is better.",
        "formula": "Average business hours from cycle message to first manager reply",
        "rules": [
            "Only Mon\u2013Fri, 8am\u20135pm in the responding manager\u2019s timezone counts",
            "Weekends and US federal holidays are excluded",
            "Any manager (Joe, Gabe, or Alexa) can respond \u2014 the first one to reply gets credit",
            "If a cycle has no manager response within 72 business hours, it is marked \u201cNo Response\u201d and excluded from the average",
            "Click any monthly value to see the average per manager",
        ],
    },
}

def fmt(val, key):
    if val is None: return None
    if key == "num_ds":        return str(int(val))
    if key == "response_per_d": return f"{val}h"
    return str(val)

def build_rows(metrics, section, year):
    html = ""
    for row in METRIC_DEFS:
        if row["section"] != section: continue
        key, label = row["key"], row["label"]
        tval = TARGETS.get(key)
        tstr = str(tval) if tval is not None else "TBD"

        cells = ""
        for i, mk in enumerate(MONTH_KEYS):
            ym     = f"{year}-{mk}"
            md     = metrics.get(ym, {})
            val    = md.get(key)
            drill  = md.get("drill", {}).get(key, {})
            disp   = fmt(val, key)
            suffix = "h" if key == "response_per_d" else ""

            if disp is None or not md:
                cells += '<td class="mc"><span class="empty">—</span></td>'
            else:
                dj = json.dumps(drill).replace('"','&quot;')
                cells += (
                    f'<td class="mc"><span class="mv click"'
                    f' data-month="{MONTHS[i]}" data-year="{year}"'
                    f' data-metric="{label}" data-suffix="{suffix}"'
                    f' data-drill="{dj}" onclick="showDrill(this)">'
                    f'{disp}</span></td>'
                )

        html += f"""
        <tr class="mr">
          <td class="ml">
            <div class="mn click-title" onclick="showInfo('{key}')">{label}</div>
            <div class="mt">Target: {tstr}</div>
          </td>{cells}
        </tr>"""
    return html


def generate_html(metrics, year=2026):
    mh  = "".join(f'<th class="mh">{m}</th>' for m in MONTHS)
    dr  = build_rows(metrics, "deliverables", year)
    rr  = build_rows(metrics, "response",     year)
    upd = datetime.now(tz=timezone.utc).strftime("%B %d, %Y")
    info_js = json.dumps(METRIC_INFO)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Creative KPIs</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Helvetica Neue',Arial,sans-serif;background:#fff;color:#111;padding:52px 64px}}
h1{{font-size:2.6rem;font-weight:300;letter-spacing:-.5px;margin-bottom:52px}}
.sl{{font-size:10.5px;font-weight:600;letter-spacing:2px;text-transform:uppercase;color:#bbb;margin-bottom:10px}}
table{{width:100%;border-collapse:collapse;margin-bottom:52px}}
th,td{{padding:18px 10px;border-bottom:1px solid #f0f0f0;vertical-align:middle}}
.mh{{font-size:10px;font-weight:500;letter-spacing:1.2px;text-transform:uppercase;color:#c0c0c0;text-align:center;min-width:62px}}
.ml{{min-width:200px;padding-right:28px}}
.mn{{font-size:1.05rem;font-weight:600;color:#111;margin-bottom:4px}}
.mn.click-title{{cursor:pointer;display:inline-block}}
.mn.click-title:hover{{color:#0057d9;text-decoration:underline;text-decoration-style:dotted;text-decoration-color:#0057d9}}
.mt{{font-size:.72rem;color:#c0c0c0}}
.mc{{text-align:center}}
.mv{{font-size:.97rem;font-weight:500;color:#222}}
.mv.click{{cursor:pointer;text-decoration:underline;text-decoration-style:dotted;text-decoration-color:#ddd;transition:color .15s,text-decoration-color .15s}}
.mv.click:hover{{color:#0057d9;text-decoration-color:#0057d9}}
.empty{{color:#e0e0e0;font-size:.8rem}}
.ov{{display:none;position:fixed;inset:0;background:rgba(0,0,0,.1);z-index:100}}
.ov.on{{display:block}}
.pnl{{position:fixed;top:0;right:-440px;width:420px;height:100vh;background:#fff;box-shadow:-4px 0 28px rgba(0,0,0,.09);z-index:101;transition:right .27s cubic-bezier(.4,0,.2,1);display:flex;flex-direction:column}}
.pnl.open{{right:0}}
.ph{{padding:28px 28px 20px;border-bottom:1px solid #f2f2f2;display:flex;justify-content:space-between;align-items:flex-start}}
.pt{{font-size:1.05rem;font-weight:600;color:#111}}
.ps{{font-size:.73rem;color:#aaa;margin-top:4px}}
.px{{background:none;border:none;cursor:pointer;color:#bbb;font-size:1.15rem;padding:0;margin-left:10px;line-height:1;flex-shrink:0}}
.px:hover{{color:#333}}
.pb{{flex:1;overflow-y:auto;padding:24px 28px 32px}}
/* Drill-down rows */
.dr{{display:flex;justify-content:space-between;align-items:center;padding:13px 0;border-bottom:1px solid #f8f8f8}}
.dr:last-child{{border:none}}
.dn{{font-size:.88rem;color:#444}}
.dv{{font-size:.88rem;font-weight:600;color:#111}}
.nd{{color:#bbb;font-size:.82rem;padding:20px 0;text-align:center}}
/* Info panel sections */
.info-section{{margin-bottom:24px}}
.info-label{{font-size:9.5px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;color:#bbb;margin-bottom:8px}}
.info-text{{font-size:.88rem;color:#333;line-height:1.6}}
.info-formula{{font-size:.88rem;color:#333;background:#f7f7f7;border-radius:6px;padding:10px 14px;font-family:monospace;line-height:1.5}}
.info-rules{{list-style:none;padding:0;margin:0}}
.info-rules li{{font-size:.85rem;color:#444;line-height:1.55;padding:5px 0 5px 16px;border-bottom:1px solid #f5f5f5;position:relative}}
.info-rules li:last-child{{border:none}}
.info-rules li::before{{content:"–";position:absolute;left:0;color:#bbb}}
.ft{{margin-top:48px;font-size:.68rem;color:#ccc}}
</style>
</head>
<body>
<h1>Creative KPIs</h1>

<div class="sl">Deliverables</div>
<table>
  <thead><tr><th class="ml"></th>{mh}</tr></thead>
  <tbody>{dr}</tbody>
</table>

<div class="sl">Time to First Response</div>
<table>
  <thead><tr><th class="ml"></th>{mh}</tr></thead>
  <tbody>{rr}</tbody>
</table>

<div class="ft">Last updated: {upd}</div>

<div class="ov" id="ov" onclick="close_()"></div>
<div class="pnl" id="pnl">
  <div class="ph">
    <div><div class="pt" id="pt"></div><div class="ps" id="ps"></div></div>
    <button class="px" onclick="close_()">✕</button>
  </div>
  <div class="pb" id="pb"></div>
</div>

<script>
const METRIC_INFO = {info_js};

function showInfo(key) {{
  const info = METRIC_INFO[key];
  if (!info) return;
  document.getElementById('pt').textContent = info.label;
  document.getElementById('ps').textContent = 'Definition & Rules';
  const rules = info.rules.map(r => `<li>${{r}}</li>`).join('');
  document.getElementById('pb').innerHTML = `
    <div class="info-section">
      <div class="info-label">What it means</div>
      <div class="info-text">${{info.definition}}</div>
    </div>
    <div class="info-section">
      <div class="info-label">How it's calculated</div>
      <div class="info-formula">${{info.formula}}</div>
    </div>
    <div class="info-section">
      <div class="info-label">Rules</div>
      <ul class="info-rules">${{rules}}</ul>
    </div>`;
  document.getElementById('ov').classList.add('on');
  document.getElementById('pnl').classList.add('open');
}}

function showDrill(el) {{
  const s = el.dataset.suffix || '';
  let d = {{}};
  try {{ d = JSON.parse(el.dataset.drill); }} catch(e) {{}}
  document.getElementById('pt').textContent = el.dataset.metric;
  document.getElementById('ps').textContent = el.dataset.month + ' ' + el.dataset.year;
  const entries = Object.entries(d);
  document.getElementById('pb').innerHTML = entries.length
    ? entries.map(([n,v]) => `<div class="dr"><span class="dn">${{n}}</span><span class="dv">${{v}}${{s}}</span></div>`).join('')
    : '<div class="nd">No breakdown available</div>';
  document.getElementById('ov').classList.add('on');
  document.getElementById('pnl').classList.add('open');
}}

function close_() {{
  document.getElementById('ov').classList.remove('on');
  document.getElementById('pnl').classList.remove('open');
}}
document.addEventListener('keydown', e => {{ if (e.key === 'Escape') close_(); }});
</script>
</body>
</html>"""

# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("═"*56)
    print("  Creative KPIs Dashboard Generator")
    print("═"*56)

    client = WebClient(token=SLACK_TOKEN)

    print("\n[1/5] Fetching Slack users...")
    users = get_all_users(client)
    print(f"  {len(users)} users")

    print(f"\n[2/5] Finding managers: {MANAGER_NAMES}")
    managers = find_managers(users, MANAGER_NAMES)
    if not managers:
        print("  WARNING: No managers found — response time tracking disabled")

    start_dt, end_dt = get_date_range()

    print("\n[3/5] Loading existing data.json...")
    existing = {}
    if os.path.exists("data.json"):
        try:
            existing = json.load(open("data.json")).get("metrics", {})
            print(f"  Found data for: {sorted(existing.keys())}")
        except Exception:
            print("  Could not parse data.json — starting fresh")
    else:
        print("  No data.json found — starting fresh")

    print("\n[4/5] Fetching and processing Slack data...")
    month_data   = process_slack(client, CHANNEL_ID, users, managers, start_dt, end_dt)
    new_metrics  = compute_metrics(month_data, managers)
    for m, d in sorted(new_metrics.items()):
        print(f"  {m}: {d['num_ds']} Ds | Cycles/D={d['cycles_per_d']} | "
              f"Replies/D={d['replies_per_d']} | Response/D={d['response_per_d']}")

    merged = {**existing, **new_metrics}

    print("\n[5/5] Writing output files...")
    html = generate_html(merged)
    with open("index.html","w") as f: f.write(html)
    print("  ✓ index.html")

    with open("data.json","w") as f:
        json.dump({"last_updated": datetime.now(tz=timezone.utc).isoformat(),
                   "metrics": merged, "targets": TARGETS}, f, indent=2)
    print("  ✓ data.json")

    print(f"\n{'═'*56}")
    print("  Done! Files written. GitHub Actions will commit & push.")
    print(f"{'═'*56}\n")


if __name__ == "__main__":
    main()

