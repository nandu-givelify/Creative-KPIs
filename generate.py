#!/usr/bin/env python3
"""
Creative KPIs Dashboard Generator
Reads Slack data for the last 3 months, computes KPI metrics,
and writes index.html + data.json to disk.
The GitHub Actions workflow commits and pushes those files.
"""

import json
import os
import re
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
#  DATE RANGE  — rolling last 3 months from today
# ═══════════════════════════════════════════════════════════════════════════════

def get_date_range():
    """Return (start, end) anchored to the 1st of the month 3 months ago.

    Always starts on the 1st so we capture complete calendar months and
    never overwrite a month's data with a partial fetch.

    Examples:
      Run on April 30  → start = Jan  1, end = April 30  (covers Jan–Apr fully)
      Run on May  15   → start = Feb  1, end = May  15   (covers Feb–May; Jan preserved in data.json)
      Run on Jan   5   → start = Oct  1 (prev year), end = Jan 5
    """
    today = datetime.now(tz=timezone.utc)
    m, y = today.month - 3, today.year
    if m <= 0:
        m += 12
        y -= 1
    start = datetime(y, m, 1, 0, 0, 0, tzinfo=timezone.utc)  # always 1st of the month
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

def _slack_call_with_retry(fn, label):
    """Call a Slack SDK function with up to 5 retries on rate-limit errors."""
    for attempt in range(5):
        try:
            return fn()
        except SlackApiError as e:
            if "ratelimited" in str(e).lower():
                wait = 15 * (2 ** attempt)   # 15s, 30s, 60s, 120s, 240s
                print(f"  Rate limited ({label}) — waiting {wait}s (attempt {attempt+1}/5)")
                time.sleep(wait)
            else:
                print(f"  Slack error ({label}): {e}")
                return None   # non-rate-limit error — caller handles None
    print(f"  Gave up on {label} after 5 rate-limit retries")
    return None


def fetch_history(client, channel_id, oldest, latest):
    msgs, cursor = [], None
    while True:
        r = _slack_call_with_retry(
            lambda: client.conversations_history(
                channel=channel_id, oldest=str(oldest),
                latest=str(latest), limit=200, cursor=cursor),
            "history")
        if r is None:
            break
        msgs.extend(r.get("messages", []))
        if not r.get("has_more"): break
        cursor = r.get("response_metadata", {}).get("next_cursor")
        if not cursor: break
        time.sleep(1)
    return msgs


def fetch_thread(client, channel_id, thread_ts):
    msgs, cursor = [], None
    while True:
        r = _slack_call_with_retry(
            lambda: client.conversations_replies(
                channel=channel_id, ts=thread_ts, limit=200, cursor=cursor),
            f"thread {thread_ts}")
        if r is None:
            break
        msgs.extend(r.get("messages", []))
        if not r.get("has_more"): break
        cursor = r.get("response_metadata", {}).get("next_cursor")
        if not cursor: break
        time.sleep(1.5)
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
#  TEXT HELPERS  (handle Slack bold/italic and rich-text blocks)
# ═══════════════════════════════════════════════════════════════════════════════

def normalize(text):
    """Strip Slack markdown markers and fix spacing around colons."""
    text = re.sub(r'[*_]', '', text or '')
    # "For review :" → "For review:"   (some mobile clients add a space)
    text = re.sub(r'(?i)(for\s+review|for\s+feedback)\s+:', r'\1:', text)
    return text


def extract_blocks_text(blocks):
    """Pull plain text + user mentions out of Slack rich-text blocks."""
    parts = []
    for block in (blocks or []):
        for el in block.get("elements", []):
            for sub in el.get("elements", []):
                if sub.get("type") == "text":
                    parts.append(sub.get("text", ""))
                elif sub.get("type") == "user":
                    parts.append(f"<@{sub.get('user_id', '')}>")
    return " ".join(parts)


def get_full_text(msg):
    """Combined normalized text from both text field and rich-text blocks."""
    return (normalize(msg.get("text") or "") + " " +
            normalize(extract_blocks_text(msg.get("blocks", [])))).strip()


def msg_has(msg, phrase):
    """True if phrase appears (case-insensitive) in the message's text or blocks."""
    return phrase.lower() in get_full_text(msg).lower()


def is_review(msg):    return msg_has(msg, "for review:")
def is_feedback(msg):  return msg_has(msg, "for feedback:")
def is_cycle_msg(msg): return is_review(msg) or is_feedback(msg)

def is_root(m):
    return m.get("thread_ts", m.get("ts")) == m.get("ts")

def ts_month(ts):
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m")

# ═══════════════════════════════════════════════════════════════════════════════
#  DATA PROCESSING
# ═══════════════════════════════════════════════════════════════════════════════

def collect_candidate_thread_ts(all_msgs):
    """
    Scan channel history to find thread_ts values for potential deliverable threads.

    Returns:
        confirmed_ts  — threads with a visible "For review:" message
        candidate_ts  — root messages with replies that aren't yet confirmed
                        (their non-broadcast replies may hide a "For review:")
    """
    confirmed_ts = set()
    roots_with_replies = set()

    for m in all_msgs:
        tts = m.get("thread_ts") or m.get("ts")
        if is_review(m):
            confirmed_ts.add(tts)
        if is_root(m) and int(m.get("reply_count", 0)) > 0:
            roots_with_replies.add(m["ts"])

    candidate_ts = roots_with_replies - confirmed_ts
    return confirmed_ts, candidate_ts


def process_deliverable_thread(thread, users, managers, month_data, start_ts, end_ts):
    """
    Analyze one confirmed deliverable thread and append per-designer entries to month_data.

    Deliverable rules per designer:
    - Deliverable  : their first "For review:" message in the thread.
    - Cycles       : all their other "For review:" / "For feedback:" messages
                     (before or after their deliverable).
    - Replies      : every other message, attributed as follows —
        • Before any designer's first "For review:" → retroactively assigned to
          the first designer who enters.
        • After designer[0] enters, before designer[1] → designer[0] only.
        • After designer[1] enters → all designers who have entered so far.
    - Response time: from each cycle message to the first manager reply,
                     in business hours (capped at 72 bh; over-cap = excluded).
    - Only designers whose first "For review:" falls within [start_ts, end_ts]
      are counted in this run's metrics.
    """
    if not thread:
        return

    mgr_ids = set(managers.keys())

    # ── Step 1: find each team member's first "For review:" index ───────────
    first_review_idx = {}  # uid → message index in thread
    for i, msg in enumerate(thread):
        uid = msg.get("user", "")
        if uid and uid not in mgr_ids and is_review(msg):
            if uid not in first_review_idx:
                first_review_idx[uid] = i

    if not first_review_idx:
        return  # No team member posted "For review:" — not a deliverable thread

    # ── Step 2: keep only designers whose deliverable is within the date window ─
    first_review_idx = {
        uid: idx for uid, idx in first_review_idx.items()
        if start_ts <= float(thread[idx]["ts"]) <= end_ts
    }
    if not first_review_idx:
        return

    # ── Step 3: ordered entry list (chronological by first "For review:") ───
    entry_order = sorted(first_review_idx.keys(), key=lambda uid: first_review_idx[uid])

    # ── Step 4: collect each designer's cycle messages ───────────────────────
    # Cycles = all their "For review:" / "For feedback:" except their deliverable
    designer_cycles = {uid: [] for uid in first_review_idx}
    for i, msg in enumerate(thread):
        uid = msg.get("user", "")
        if uid in first_review_idx and is_cycle_msg(msg) and i != first_review_idx[uid]:
            designer_cycles[uid].append(msg)

    # ── Step 5: compute reply attribution per message ────────────────────────
    reply_attribution = []  # parallel to thread; each entry = list of designer UIDs

    for i, msg in enumerate(thread):
        uid = msg.get("user", "")

        # Designer's deliverable message — not a reply for anyone
        if uid in first_review_idx and first_review_idx[uid] == i:
            reply_attribution.append([])
            continue

        # Designer's cycle message — not a reply for anyone
        if uid in first_review_idx and is_cycle_msg(msg):
            reply_attribution.append([])
            continue

        # Regular reply — who gets credit?
        # "entered" = designers whose first "For review:" came before this message
        entered = [d for d in entry_order if first_review_idx[d] < i]

        if not entered:
            reply_attribution.append(None)   # pre-entry placeholder
        else:
            reply_attribution.append(list(entered))

    # Resolve pre-entry placeholders → first designer to enter
    first_designer = entry_order[0]
    reply_attribution = [
        [first_designer] if x is None else x
        for x in reply_attribution
    ]

    # ── Step 6: count replies per designer ───────────────────────────────────
    designer_reply_count = {uid: 0 for uid in first_review_idx}
    for attribution in reply_attribution:
        for uid in attribution:
            if uid in designer_reply_count:
                designer_reply_count[uid] += 1

    # ── Step 7: compute response times for each designer's cycles ────────────
    mgr_id_list = list(mgr_ids)

    for uid in first_review_idx:
        deliv_idx = first_review_idx[uid]
        deliv_msg = thread[deliv_idx]
        month = ts_month(deliv_msg["ts"])
        pinfo = users.get(uid, {})
        pname = pinfo.get("display_name") or pinfo.get("name") or "Unknown"

        cycle_data = []
        for cyc_msg in designer_cycles[uid]:
            cts = float(cyc_msg["ts"])
            best_ts, best_mgr = None, None
            for m in thread:
                mts = float(m["ts"])
                if mts > cts and m.get("user") in mgr_ids:
                    if best_ts is None or mts < best_ts:
                        best_ts, best_mgr = mts, m["user"]

            resp_time = None
            if best_ts and best_mgr:
                bh = business_hours_between(cts, best_ts, managers[best_mgr]["tz"])
                if bh <= NO_RESPONSE_THRESHOLD_BH:
                    resp_time = bh

            full_txt = get_full_text(cyc_msg)
            tagged = [mid for mid in mgr_id_list if f"<@{mid}>" in full_txt]
            cycle_data.append({
                "ts":                     cyc_msg["ts"],
                "tagged":                 tagged,
                "response_time_hours":    resp_time,
                "responding_manager_id":  best_mgr if resp_time is not None else None,
            })

        month_data.setdefault(month, []).append({
            "root_ts":     thread[0]["ts"],
            "month":       month,
            "poster_id":   uid,
            "poster_name": pname,
            "cycle_count": len(designer_cycles[uid]),
            "reply_count": designer_reply_count[uid],
            "cycles":      cycle_data,
        })


def process_slack(client, channel_id, users, managers, start_dt, end_dt):
    start_ts = start_dt.timestamp()
    end_ts   = end_dt.timestamp()

    print(f"\n  Date range: {start_dt.date()} → {end_dt.date()}")
    all_msgs = fetch_history(client, channel_id, start_ts, end_ts)
    print(f"  Total channel messages in window: {len(all_msgs)}")

    # ── Find candidate and confirmed deliverable threads ─────────────────────
    confirmed_ts, candidate_ts = collect_candidate_thread_ts(all_msgs)
    print(f"  Confirmed deliverable threads (visible 'For review:'): {len(confirmed_ts)}")
    print(f"  Candidate threads to scan (roots with replies, no visible 'For review:'): {len(candidate_ts)}")

    # ── Fetch candidate threads; promote those containing a hidden "For review:" ─
    thread_cache = {}
    for rts in candidate_ts:
        thread = fetch_thread(client, channel_id, rts)
        thread_cache[rts] = thread
        for msg in thread[1:]:   # skip root — already checked in history
            if is_review(msg):
                confirmed_ts.add(rts)
                break

    print(f"  Total confirmed deliverable threads after full scan: {len(confirmed_ts)}")

    # ── Process every confirmed deliverable thread ────────────────────────────
    month_data = {}
    for rts in confirmed_ts:
        thread = thread_cache.get(rts) or fetch_thread(client, channel_id, rts)
        process_deliverable_thread(thread, users, managers, month_data, start_ts, end_ts)

    total_ds = sum(len(v) for v in month_data.values())
    print(f"  Total deliverable entries found: {total_ds}")

    # ── DIAGNOSTIC: print every deliverable so we can spot gaps in the log ───
    print("\n  DIAGNOSTIC — all deliverable entries by month:")
    for month in sorted(month_data.keys()):
        entries = month_data[month]
        print(f"  {month}: {len(entries)} entries")
        for d in entries:
            root_dt = datetime.fromtimestamp(float(d['root_ts']), tz=timezone.utc).strftime("%Y-%m-%d")
            deliv_dt = datetime.fromtimestamp(
                float(d['root_ts']), tz=timezone.utc).strftime("%Y-%m-%d")
            # Show thread root date and poster — helps cross-check with Slack
            print(f"    [{root_dt}] {d['poster_name']}  "
                  f"(thread_ts={d['root_ts']}, cycles={d['cycle_count']}, replies={d['reply_count']})")

    # ── DIAGNOSTIC: scan history for ALL 'For review:' messages to catch anything missed ─
    print(f"\n  DIAGNOSTIC — confirmed_ts size: {len(confirmed_ts)}")
    print("\n  DIAGNOSTIC — every 'For review:' message visible in channel history:")
    review_msgs = [m for m in all_msgs if is_review(m)]
    print(f"  Found {len(review_msgs)} 'For review:' messages in history:")

    # Build set of root_ts values from deliverables so we can flag missing ones
    found_root_ts = set()
    for entries in month_data.values():
        for d in entries:
            found_root_ts.add(d["root_ts"])

    for m in sorted(review_msgs, key=lambda x: float(x.get("ts", 0))):
        uid   = m.get("user", "")
        uname = users.get(uid, {}).get("display_name", uid)
        ts    = m.get("ts", "")
        dt    = datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        tts   = m.get("thread_ts") or ts
        root  = is_root(m)
        sub   = m.get("subtype") or "—"
        txt   = get_full_text(m)[:80]
        in_confirmed = tts in confirmed_ts
        produced_d   = tts in found_root_ts
        flag = "" if produced_d else " ◄ NO DELIVERABLE"
        if not in_confirmed: flag += " [NOT IN confirmed_ts!]"
        print(f"    [{dt}] {uname:<20} root={root} confirmed={in_confirmed} deliverable={produced_d}{flag}")
        print(f"           thread_ts={tts}  \"{txt}\"")

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
            pd[p] = pd.get(p, 0) + 1
            pc[p] = pc.get(p, 0) + d["cycle_count"]
            pr[p] = pr.get(p, 0) + d["reply_count"]

        tc = sum(d["cycle_count"] for d in deliverables)
        tr = sum(d["reply_count"] for d in deliverables)

        mgr_times = {}
        for d in deliverables:
            for c in d["cycles"]:
                if c["response_time_hours"] is not None:
                    mid   = c["responding_manager_id"]
                    label = managers.get(mid, {}).get("manager_label", mid)
                    mgr_times.setdefault(label, []).append(c["response_time_hours"])

        all_t    = [t for ts in mgr_times.values() for t in ts]
        avg_resp = round(sum(all_t)/len(all_t), 1) if all_t else None
        mgr_avgs = {k: round(sum(v)/len(v), 1) for k, v in mgr_times.items()}

        sd = lambda d: dict(sorted(d.items(), key=lambda x: -x[1]))
        sa = lambda d: dict(sorted(d.items(), key=lambda x:  x[1]))

        # Ds/Person divides by the number of people who actually posted that month.
        # Drill-down shows only active posters (no zeros for non-posters).
        full_team_ds = {name: pd.get(name, 0) for name in TEAM_MEMBERS}
        for name, count in pd.items():
            if name not in full_team_ds:
                full_team_ds[name] = count
        active_posters = len(pd)  # only people who posted at least 1 deliverable

        result[month] = {
            "num_ds":         n,
            "ds_per_person":  round(n / active_posters, 2) if active_posters else 0,
            "cycles_per_d":   round(tc/n, 2),
            "replies_per_d":  round(tr/n, 2),
            "response_per_d": avg_resp,
            "drill": {
                "num_ds":         sd(full_team_ds),
                "ds_per_person":  sd(full_team_ds),
                "cycles_per_d":   sd({k: round(pc[k]/pd[k], 2) for k in pd}),
                "replies_per_d":  sd({k: round(pr[k]/pd[k], 2) for k in pd}),
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
            "A thread qualifies as a deliverable thread if any message in it \u2014 root or reply \u2014 contains \u201cFor review:\u201d",
            "Each team member who posts \u201cFor review:\u201d in that thread gets credited with 1 deliverable",
            "The deliverable date is the timestamp of their first \u201cFor review:\u201d message",
            "If two designers both post \u201cFor review:\u201d in the same thread, each gets their own separate deliverable",
            "Messages from managers (Joe, Gabe, Alexa) are never counted as deliverables",
        ],
    },
    "ds_per_person": {
        "label": "Deliverables / Person",
        "definition": "How many deliverables each active team member submitted on average in that month.",
        "formula": "Total deliverables \u00f7 number of people who posted at least one deliverable",
        "rules": [
            "Only counts people who actually submitted a deliverable that month \u2014 inactive members are excluded from the denominator",
            "This reflects the average workload of those who were actively delivering",
            "Click any monthly value to see the per-person breakdown",
        ],
    },
    "cycles_per_d": {
        "label": "Cycles / Deliverable",
        "definition": "On average, how many additional review rounds a deliverable goes through after the initial submission. Lower is better \u2014 fewer cycles means faster approvals.",
        "formula": "Total cycles \u00f7 Total deliverables",
        "rules": [
            "A cycle = any \u201cFor review:\u201d or \u201cFor feedback:\u201d message posted by the designer in that thread, other than their first \u201cFor review:\u201d (the deliverable submission itself)",
            "Cycles before the deliverable message also count (e.g. \u201cFor feedback:\u201d posted earlier in the same thread)",
            "Click any monthly value to see cycles per person",
        ],
    },
    "replies_per_d": {
        "label": "Replies / Deliverable",
        "definition": "Average number of discussion messages attributed to each deliverable, excluding the deliverable and cycle messages themselves.",
        "formula": "Total attributed replies \u00f7 Total deliverables",
        "rules": [
            "Messages before any designer enters the thread are retroactively counted for the first designer who posts \u201cFor review:\u201d",
            "Messages between the first and second designer\u2019s entry count for the first designer only",
            "Once multiple designers are involved, all subsequent replies count for each of them",
            "Click any monthly value to see replies per person",
        ],
    },
    "response_per_d": {
        "label": "Avg. Response Time",
        "definition": "How quickly a manager first responds to a review or feedback cycle, measured in business hours. Lower is better.",
        "formula": "Average business hours from each cycle message to the first manager reply",
        "rules": [
            "Only Mon\u2013Fri, 8am\u20135pm in the responding manager\u2019s timezone counts",
            "Weekends are excluded",
            "Any manager (Joe, Gabe, or Alexa) can respond \u2014 the first one to reply after a cycle message gets credit",
            "If no manager responds within 72 business hours, that cycle is marked \u201cNo Response\u201d and excluded from the average",
            "Click any monthly value to see the average response time per manager",
        ],
    },
}

def fmt(val, key):
    if val is None: return None
    if key == "num_ds":         return str(int(val))
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
            ym    = f"{year}-{mk}"
            md    = metrics.get(ym, {})
            val   = md.get(key)
            drill = md.get("drill", {}).get(key, {})
            disp  = fmt(val, key)
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
.dr{{display:flex;justify-content:space-between;align-items:center;padding:13px 0;border-bottom:1px solid #f8f8f8}}
.dr:last-child{{border:none}}
.dn{{font-size:.88rem;color:#444}}
.dv{{font-size:.88rem;font-weight:600;color:#111}}
.nd{{color:#bbb;font-size:.82rem;padding:20px 0;text-align:center}}
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
    month_data  = process_slack(client, CHANNEL_ID, users, managers, start_dt, end_dt)
    new_metrics = compute_metrics(month_data, managers)
    for m, d in sorted(new_metrics.items()):
        print(f"  {m}: {d['num_ds']} Ds | Cycles/D={d['cycles_per_d']} | "
              f"Replies/D={d['replies_per_d']} | Response/D={d['response_per_d']}")

    merged = {**existing, **new_metrics}

    print("\n[5/5] Writing output files...")
    html = generate_html(merged)
    with open("index.html", "w") as f: f.write(html)
    print("  ✓ index.html")

    with open("data.json", "w") as f:
        json.dump({"last_updated": datetime.now(tz=timezone.utc).isoformat(),
                   "metrics": merged, "targets": TARGETS}, f, indent=2)
    print("  ✓ data.json")

    print(f"\n{'═'*56}")
    print("  Done! Files written. GitHub Actions will commit & push.")
    print(f"{'═'*56}\n")


if __name__ == "__main__":
    main()
