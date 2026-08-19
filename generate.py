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
SLACK_WORKSPACE = "givelify"   # used for constructing Slack thread URLs
MANAGER_NAMES = ["Joe", "Gabe", "Alexa"]

# Designer groups — used for the Separated view and drill-down rosters.
PRODUCT_DESIGNERS = [
    "Evan Brown", "Saba Talat", "Alex", "Krystyna", "Nandu",
    "Olexii Lysenko", "Spencer Arney", "Andrew Kallemeyn",
]
MARKETING_DESIGNERS = [
    "Nata", "Carlos Miras", "Dan Howard", "Jacob Blaze", "Anastasia",
]
# Full roster (combined) — every member appears in the drill-down.
# Ds/Person divides by the number of active posters that month, not a fixed number.
TEAM_MEMBERS = PRODUCT_DESIGNERS + MARKETING_DESIGNERS

TARGETS = {
    "num_ds":          48,
    "ds_per_person":   4,
    "cycles_per_d":    None,   # TBD
    "replies_per_d":   None,
    "task_days_per_d": None,   # TBD
    "response_per_d":  None,
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
    """Return (start, end) for the Slack fetch.

    Normal mode: rolling window starting the 1st of the month, 5 months back.
    Backfill mode: reads START_DATE / END_DATE environment variables (YYYY-MM-DD),
                   set via workflow_dispatch inputs in GitHub Actions.

    Examples (normal):
      Run on July 14  → start = Feb  1, end = July 14
      Run on Mar   5  → start = Oct  1 (prev year), end = Mar 5
    """
    start_env = os.environ.get("START_DATE", "").strip()
    end_env   = os.environ.get("END_DATE",   "").strip()

    if start_env and end_env:
        start = datetime.strptime(start_env, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end   = datetime.strptime(end_env,   "%Y-%m-%d").replace(
                    hour=23, minute=59, second=59, tzinfo=timezone.utc)
        print(f"  (Backfill mode: using START_DATE={start_env}  END_DATE={end_env})")
        return start, end

    today = datetime.now(tz=timezone.utc)
    m, y = today.month - 5, today.year
    if m <= 0:
        m += 12
        y -= 1
    start = datetime(y, m, 1, 0, 0, 0, tzinfo=timezone.utc)  # 1st of month, 5 months back
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
    """Call a Slack SDK function with up to 5 retries on rate-limit or transient errors."""
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
                return None   # non-rate-limit API error — skip this call
        except Exception as e:
            # Catch timeouts, connection resets, and other transient network errors
            wait = 10 * (2 ** attempt)   # 10s, 20s, 40s, 80s, 160s
            print(f"  Network error ({label}): {type(e).__name__}: {e} — waiting {wait}s (attempt {attempt+1}/5)")
            time.sleep(wait)
    print(f"  Gave up on {label} after 5 retries")
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

def business_days_between(start_ts, end_ts):
    """Count Mon–Fri days between two timestamps (UTC dates, weekends excluded)."""
    s = datetime.fromtimestamp(float(start_ts), tz=timezone.utc).date()
    e = datetime.fromtimestamp(float(end_ts),   tz=timezone.utc).date()
    if e <= s:
        return 0.0
    total_days = (e - s).days
    full_weeks, remainder = divmod(total_days, 7)
    bdays = full_weeks * 5
    start_dow = s.weekday()   # Mon=0 … Sun=6
    for i in range(remainder):
        if (start_dow + i) % 7 < 5:
            bdays += 1
    return float(bdays)


def make_slack_url(thread_ts):
    """Construct a direct Slack link for a thread."""
    ts_clean = thread_ts.replace(".", "")
    return f"https://{SLACK_WORKSPACE}.slack.com/archives/{CHANNEL_ID}/p{ts_clean}"


LONGER_GAP_THRESHOLD = 5  # business days between any two consecutive messages

def compute_max_gap_bdays(thread):
    """Largest business-day gap between any two consecutive messages in a thread."""
    timestamps = sorted(float(m["ts"]) for m in thread)
    if len(timestamps) < 2:
        return None
    max_gap = 0.0
    for i in range(1, len(timestamps)):
        gap = business_days_between(timestamps[i-1], timestamps[i])
        if gap > max_gap:
            max_gap = gap
    return max_gap if max_gap > 0 else None


def compute_signal(cycle_count, manager_wait, designer_wait, reply_count, max_gap=None):
    """Return a short diagnostic label for a deliverable based on its patterns."""
    if cycle_count >= 3:                                           return "High cycles"
    if max_gap is not None and max_gap >= LONGER_GAP_THRESHOLD:   return "Longer Gap"
    if manager_wait is not None and manager_wait > 2:              return "Slow feedback"
    if designer_wait is not None and designer_wait > 2:            return "Slow pickup"
    if reply_count >= 8 and cycle_count <= 1:                      return "Long discussion"
    return "On track"


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

def extract_deliverable_type(msg):
    """Extract the label after 'For review:' or 'For feedback:' — e.g. 'UI', 'Discovery'."""
    text = get_full_text(msg)
    for trigger in ["for review:", "for feedback:"]:
        idx = text.lower().find(trigger)
        if idx != -1:
            after = text[idx + len(trigger):].strip()
            first_part = after.split('\n')[0].strip()
            # Grab first 3 words max (enough for "UI Redesign", "Fab5 Marketing Banner", etc.)
            words = first_part.split()[:3]
            label = " ".join(words)
            return label[:35] if label else ""
    return ""

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
        confirmed_ts  — threads with a visible "For review:" or "For feedback:" message
        candidate_ts  — root messages with replies that aren't yet confirmed
                        (their non-broadcast replies may hide a "For review:"/"For feedback:")
    """
    confirmed_ts = set()
    roots_with_replies = set()

    for m in all_msgs:
        tts = m.get("thread_ts") or m.get("ts")
        if is_cycle_msg(m):   # "For review:" OR "For feedback:"
            confirmed_ts.add(tts)
        if is_root(m) and int(m.get("reply_count", 0)) > 0:
            roots_with_replies.add(m["ts"])

    candidate_ts = roots_with_replies - confirmed_ts
    return confirmed_ts, candidate_ts


def process_deliverable_thread(thread, users, managers, month_data, start_ts, end_ts):
    """
    Analyze one confirmed deliverable thread and append per-designer entries to month_data.

    Deliverable rules per designer:
    - Deliverable  : their first "For review:" OR "For feedback:" message in the thread.
    - Cycles       : all their other "For review:" / "For feedback:" messages
                     (before or after their deliverable).
    - Replies      : every other message, attributed as follows —
        • Before any designer's first review/feedback → retroactively assigned to
          the first designer who enters.
        • After designer[0] enters, before designer[1] → designer[0] only.
        • After designer[1] enters → all designers who have entered so far.
        • Deliverable and cycle messages (from any designer) are never counted as replies.
    - Response time: from each cycle message to the first manager reply,
                     in business hours (capped at 72 bh; over-cap = excluded).
    - Only designers whose first review/feedback falls within [start_ts, end_ts]
      are counted in this run's metrics.
    """
    if not thread:
        return

    mgr_ids = set(managers.keys())

    # ── Step 1: find each team member's first "For review:" OR "For feedback:" ─
    first_deliv_idx = {}  # uid → message index of their deliverable in thread
    for i, msg in enumerate(thread):
        uid = msg.get("user", "")
        if uid and uid not in mgr_ids and is_cycle_msg(msg):
            if uid not in first_deliv_idx:
                first_deliv_idx[uid] = i

    if not first_deliv_idx:
        return  # No team member posted a review/feedback — not a deliverable thread

    # ── Step 2: keep only designers whose deliverable is within the date window ─
    first_deliv_idx = {
        uid: idx for uid, idx in first_deliv_idx.items()
        if start_ts <= float(thread[idx]["ts"]) <= end_ts
    }
    if not first_deliv_idx:
        return

    # ── Step 3: ordered entry list (chronological by deliverable message) ────
    entry_order = sorted(first_deliv_idx.keys(), key=lambda uid: first_deliv_idx[uid])

    # ── Step 4: collect each designer's cycle messages ───────────────────────
    # Cycles = all their "For review:" / "For feedback:" except their deliverable
    designer_cycles = {uid: [] for uid in first_deliv_idx}
    for i, msg in enumerate(thread):
        uid = msg.get("user", "")
        if uid in first_deliv_idx and is_cycle_msg(msg) and i != first_deliv_idx[uid]:
            designer_cycles[uid].append(msg)

    # ── Step 5: compute reply attribution per message ────────────────────────
    # Deliverable and cycle messages (from any designer) are never counted as replies.
    # Regular messages are attributed to all designers who have entered by that point.
    reply_attribution = []  # parallel to thread; each entry = list of designer UIDs

    for i, msg in enumerate(thread):
        uid = msg.get("user", "")

        # Designer's deliverable message — not a reply for anyone
        if uid in first_deliv_idx and first_deliv_idx[uid] == i:
            reply_attribution.append([])
            continue

        # Any designer's cycle message — not a reply for anyone
        if uid in first_deliv_idx and is_cycle_msg(msg):
            reply_attribution.append([])
            continue

        # Regular reply — who gets credit?
        # "entered" = designers whose deliverable came before this message
        entered = [d for d in entry_order if first_deliv_idx[d] < i]

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
    designer_reply_count = {uid: 0 for uid in first_deliv_idx}
    for attribution in reply_attribution:
        for uid in attribution:
            if uid in designer_reply_count:
                designer_reply_count[uid] += 1

    # ── Step 7: compute response times for each designer's cycles ────────────
    mgr_id_list = list(mgr_ids)

    for uid in first_deliv_idx:
        deliv_idx = first_deliv_idx[uid]
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

        # Business days (Mon–Fri, weekends excluded) from this designer's deliverable
        # to the last message in the thread
        last_thread_ts = max(float(m["ts"]) for m in thread)
        task_days = business_days_between(float(deliv_msg["ts"]), last_thread_ts)

        # ── Phase times ──────────────────────────────────────────────────────
        # Phase 1 (manager wait): how long until a manager replied after each
        #   designer action (deliverable + each cycle).
        # Phase 2 (designer pickup): how long the designer took to post their
        #   next cycle after receiving manager feedback.
        all_mgr_ts  = sorted(float(m["ts"]) for m in thread if m.get("user") in mgr_ids)
        all_cyc_ts  = sorted(float(c["ts"]) for c in designer_cycles[uid])
        deliv_ts_f  = float(deliv_msg["ts"])

        mgr_wait_list, des_wait_list = [], []

        # After deliverable
        nm = next((ts for ts in all_mgr_ts if ts > deliv_ts_f), None)
        if nm:
            mgr_wait_list.append(business_days_between(deliv_ts_f, nm))

        # After each cycle
        for cyc_ts in all_cyc_ts:
            nm = next((ts for ts in all_mgr_ts if ts > cyc_ts), None)
            if nm:
                mgr_wait_list.append(business_days_between(cyc_ts, nm))
            pm = max((ts for ts in all_mgr_ts if ts < cyc_ts), default=None)
            if pm:
                des_wait_list.append(business_days_between(pm, cyc_ts))

        avg_mgr_wait = round(sum(mgr_wait_list) / len(mgr_wait_list), 1) if mgr_wait_list else None
        avg_des_wait = round(sum(des_wait_list) / len(des_wait_list), 1) if des_wait_list else None
        max_gap      = compute_max_gap_bdays(thread)

        signal    = compute_signal(len(designer_cycles[uid]), avg_mgr_wait, avg_des_wait,
                                   designer_reply_count[uid], max_gap)
        slack_url  = make_slack_url(thread[0]["ts"])
        deliv_type = extract_deliverable_type(deliv_msg)

        month_data.setdefault(month, []).append({
            "root_ts":             thread[0]["ts"],
            "month":               month,
            "poster_id":           uid,
            "poster_name":         pname,
            "deliverable_type":    deliv_type,
            "cycle_count":         len(designer_cycles[uid]),
            "reply_count":         designer_reply_count[uid],
            "task_days":           task_days,
            "manager_wait_bdays":  avg_mgr_wait,
            "designer_wait_bdays": avg_des_wait,
            "max_gap_bdays":       max_gap,
            "signal":              signal,
            "slack_url":           slack_url,
            "cycles":              cycle_data,
        })


def process_slack(client, channel_id, users, managers, start_dt, end_dt):
    start_ts = start_dt.timestamp()
    end_ts   = end_dt.timestamp()

    print(f"\n  Date range: {start_dt.date()} → {end_dt.date()}")
    all_msgs = fetch_history(client, channel_id, start_ts, end_ts)
    print(f"  Total channel messages in window: {len(all_msgs)}")

    # ── Find candidate and confirmed deliverable threads ─────────────────────
    confirmed_ts, candidate_ts = collect_candidate_thread_ts(all_msgs)
    print(f"  Confirmed deliverable threads (visible review/feedback): {len(confirmed_ts)}")
    print(f"  Candidate threads to scan (roots with replies, no visible review/feedback): {len(candidate_ts)}")

    # ── Fetch candidate threads; promote those containing "For review:" or "For feedback:" ─
    thread_cache = {}
    for rts in candidate_ts:
        thread = fetch_thread(client, channel_id, rts)
        thread_cache[rts] = thread
        for msg in thread[1:]:   # skip root — already checked in history
            if is_cycle_msg(msg):
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

    return month_data


def compute_metrics(month_data, managers, roster=None):
    """
    Compute monthly KPI metrics from raw deliverable data.

    roster: optional list of designer names to include (for Product / Marketing views).
            When set, only deliverables from those designers are counted, and the
            drill-down shows all roster members (with 0 for inactive ones).
            When None, uses TEAM_MEMBERS and counts everyone.
    """
    roster_set   = set(roster) if roster is not None else None
    roster_list  = list(roster) if roster is not None else TEAM_MEMBERS

    result = {}
    for month, all_deliverables in month_data.items():
        deliverables = (
            [d for d in all_deliverables if d["poster_name"] in roster_set]
            if roster_set is not None else all_deliverables
        )

        n = len(deliverables)
        if n == 0:
            result[month] = {"num_ds":0,"ds_per_person":0,"cycles_per_d":0,
                             "replies_per_d":0,"response_per_d":None,"drill":{}}
            continue

        pd, pc, pr, ptd = {}, {}, {}, {}
        for d in deliverables:
            p = d["poster_name"]
            pd[p]  = pd.get(p, 0)  + 1
            pc[p]  = pc.get(p, 0)  + d["cycle_count"]
            pr[p]  = pr.get(p, 0)  + d["reply_count"]
            ptd[p] = ptd.get(p, 0) + d.get("task_days", 0)

        tc = sum(d["cycle_count"]        for d in deliverables)
        tr = sum(d["reply_count"]        for d in deliverables)
        tt = sum(d.get("task_days", 0)   for d in deliverables)

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

        # Drill-down: show all roster members (0 for inactive), plus any
        # unexpected names that showed up in the data.
        full_team_ds = {name: pd.get(name, 0) for name in roster_list}
        for name, count in pd.items():
            if name not in full_team_ds:
                full_team_ds[name] = count
        active_posters = len(pd)

        result[month] = {
            "num_ds":          n,
            "ds_per_person":   round(n / active_posters, 2) if active_posters else 0,
            "cycles_per_d":    round(tc/n, 2),
            "replies_per_d":   round(tr/n, 2),
            "task_days_per_d": round(tt/n, 1),
            "response_per_d":  avg_resp,
            "drill": {
                "num_ds":          sd(full_team_ds),
                "ds_per_person":   sd(full_team_ds),
                "cycles_per_d":    sd({k: round(pc[k]/pd[k],  2) for k in pd}),
                "replies_per_d":   sd({k: round(pr[k]/pd[k],  2) for k in pd}),
                "task_days_per_d": sd({k: round(ptd[k]/pd[k], 1) for k in pd}),
                "response_per_d":  sa(mgr_avgs),
            }
        }

        # ── Thread details (for per-thread drill-down) ────────────────────────
        thread_details = {}
        for d in deliverables:
            p = d["poster_name"]
            thread_details.setdefault(p, []).append({
                "deliverable_type":    d.get("deliverable_type", ""),
                "task_days":           d.get("task_days", 0),
                "cycle_count":         d["cycle_count"],
                "manager_wait_bdays":  d.get("manager_wait_bdays"),
                "designer_wait_bdays": d.get("designer_wait_bdays"),
                "max_gap_bdays":       d.get("max_gap_bdays"),
                "signal":              d.get("signal", "On track"),
                "slack_url":           d.get("slack_url", ""),
            })

        # ── Monthly insight ───────────────────────────────────────────────────
        all_signals = [d.get("signal", "On track") for d in deliverables]
        flagged     = [s for s in all_signals if s != "On track"]
        most_common = max(set(flagged), key=flagged.count) if flagged else None
        slowest     = max(deliverables, key=lambda d: d.get("task_days", 0))
        # Signal breakdown: count of each signal label (excluding "On track")
        signal_counts = {}
        for s in all_signals:
            signal_counts[s] = signal_counts.get(s, 0) + 1
        monthly_insight = {
            "total":              n,
            "flagged_count":      len(flagged),
            "most_common_signal": most_common,
            "signal_breakdown":   signal_counts,
            "slowest_days":       slowest.get("task_days", 0),
            "slowest_url":        slowest.get("slack_url", ""),
        }

        result[month]["thread_details"]  = thread_details
        result[month]["monthly_insight"] = monthly_insight
    return result

# ═══════════════════════════════════════════════════════════════════════════════
#  HTML GENERATION
# ═══════════════════════════════════════════════════════════════════════════════

METRIC_DEFS = [
    {"key":"num_ds",          "label":"# Deliverables",            "section":"deliverables"},
    {"key":"ds_per_person",   "label":"Deliverables / Person",     "section":"deliverables"},
    {"key":"cycles_per_d",    "label":"Cycles / Deliverable",      "section":"deliverables"},
    {"key":"replies_per_d",   "label":"Replies / Deliverable",     "section":"deliverables"},
    {"key":"task_days_per_d", "label":"Avg. Days to Complete",     "section":"deliverables"},
    {"key":"response_per_d",  "label":"Avg. Response Time",        "section":"response"},
]

# Info panel content — shown when a metric title is clicked
METRIC_INFO = {
    "num_ds": {
        "label": "# Deliverables",
        "definition": "Total number of creative pieces submitted for review by the team in a given month.",
        "formula": "Count of qualifying Slack threads posted that month.",
        "rules": [
            "A thread qualifies as a deliverable thread if any message in it \u2014 root or reply \u2014 contains \u201cFor review:\u201d or \u201cFor feedback:\u201d",
            "Each team member who posts \u201cFor review:\u201d or \u201cFor feedback:\u201d in that thread gets credited with 1 deliverable",
            "The deliverable date is the timestamp of their first such message (whichever comes first)",
            "If two designers both post in the same thread, each gets their own separate deliverable",
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
    "task_days_per_d": {
        "label": "Avg. Days to Complete",
        "definition": "On average, how many business days elapsed from when a designer first submitted a deliverable to the last message in that thread. Lower is better \u2014 it means work moved through faster.",
        "formula": "Average of (last thread message date \u2212 designer\u2019s first \u201cFor review:\u201d or \u201cFor feedback:\u201d date), weekends excluded, across all deliverables",
        "rules": [
            "Start date: the timestamp of the designer\u2019s first \u201cFor review:\u201d or \u201cFor feedback:\u201d message in the thread",
            "End date: the timestamp of the very last message in that thread (from anyone)",
            "Measured in business days \u2014 Saturdays and Sundays are excluded",
            "If a thread has no replies after the deliverable, that deliverable counts as 0 days",
            "Click any monthly value to see the average per person",
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
    if key == "num_ds":          return str(int(val))
    if key == "response_per_d":  return f"{val}h"
    if key == "task_days_per_d": return f"{val}d"
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
            suffix = "h" if key == "response_per_d" else "d" if key == "task_days_per_d" else ""

            if disp is None or not md:
                cells += '<td class="mc"><span class="empty">—</span></td>'
            else:
                dj = json.dumps(drill).replace('"','&quot;')
                cells += (
                    f'<td class="mc"><span class="mv click"'
                    f' data-metric-key="{key}"'
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


def generate_html(metrics_combined, metrics_product, metrics_marketing, year=2026):
    mh = "".join(f'<th class="mh">{m}</th>' for m in MONTHS)

    # Combined view rows
    dr_c = build_rows(metrics_combined,  "deliverables", year)
    rr_c = build_rows(metrics_combined,  "response",     year)

    # Separated view rows
    dr_p = build_rows(metrics_product,   "deliverables", year)
    rr_p = build_rows(metrics_product,   "response",     year)
    dr_m = build_rows(metrics_marketing, "deliverables", year)
    rr_m = build_rows(metrics_marketing, "response",     year)

    upd     = datetime.now(tz=timezone.utc).strftime("%B %d, %Y")
    info_js = json.dumps(METRIC_INFO)

    # JS data blobs for thread breakdown and insights
    thread_details_js  = json.dumps({ym: v.get("thread_details", {})
                                      for ym, v in metrics_combined.items()})
    insights_combined_js  = json.dumps({ym: v.get("monthly_insight")
                                         for ym, v in metrics_combined.items() if v.get("monthly_insight")})
    insights_product_js   = json.dumps({ym: v.get("monthly_insight")
                                         for ym, v in metrics_product.items() if v.get("monthly_insight")})
    insights_marketing_js = json.dumps({ym: v.get("monthly_insight")
                                         for ym, v in metrics_marketing.items() if v.get("monthly_insight")})

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Creative KPIs</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Helvetica Neue',Arial,sans-serif;background:#fff;color:#111;padding:52px 64px}}
.hdr{{display:flex;align-items:center;justify-content:space-between;margin-bottom:52px}}
h1{{font-size:2.6rem;font-weight:300;letter-spacing:-.5px}}
.vnav{{display:flex;gap:4px}}
.nbtn{{background:none;border:1px solid #e0e0e0;border-radius:6px;cursor:pointer;font-size:.8rem;font-weight:500;color:#888;padding:7px 18px;transition:all .15s;letter-spacing:.2px}}
.nbtn.active{{background:#111;border-color:#111;color:#fff}}
.nbtn:hover:not(.active){{border-color:#aaa;color:#333}}
.sl{{font-size:10.5px;font-weight:600;letter-spacing:2px;text-transform:uppercase;color:#bbb;margin-bottom:10px}}
.grp-hdr{{font-size:1.25rem;font-weight:500;color:#111;margin:8px 0 28px;padding-bottom:14px;border-bottom:1.5px solid #111}}
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
.sig{{display:inline-block;font-size:.68rem;font-weight:600;letter-spacing:.4px;padding:2px 8px;border-radius:4px;white-space:nowrap}}
.sig-hc{{background:#fff0f0;color:#c0392b}}
.sig-lg{{background:#f3e8ff;color:#7c3aed}}
.sig-sf{{background:#fff8e1;color:#b45309}}
.sig-sp{{background:#fff8e1;color:#b45309}}
.sig-ld{{background:#f0f4ff;color:#3a56b0}}
.sig-ot{{background:#f0faf0;color:#1a7a3a}}
.th-row{{padding:12px 0;border-bottom:1px solid #f5f5f5}}
.th-row:last-child{{border:none}}
.th-meta{{font-size:.8rem;color:#666;margin-top:4px;display:flex;gap:12px;flex-wrap:wrap;align-items:center}}
.th-link{{font-size:.78rem;color:#0057d9;text-decoration:none;margin-left:auto;flex-shrink:0}}
.th-link:hover{{text-decoration:underline}}
.des-block{{margin-bottom:24px}}
.des-hdr{{font-size:.82rem;font-weight:700;color:#111;margin-bottom:8px}}
.des-sub{{font-size:.72rem;color:#aaa;font-weight:400;margin-left:6px}}
.th-table{{width:100%;border-collapse:collapse;font-size:.82rem}}
.th-table th{{font-size:.68rem;font-weight:600;letter-spacing:.8px;text-transform:uppercase;color:#bbb;padding:4px 8px 4px 0;border-bottom:1px solid #f0f0f0;text-align:left}}
.th-table td{{padding:8px 8px 8px 0;border-bottom:1px solid #f8f8f8;color:#333;vertical-align:middle}}
.th-table tr:last-child td{{border:none}}
.th-table .td-type{{font-weight:500;color:#111;max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.th-table .td-num{{text-align:center;color:#555}}
.th-table .td-link{{text-align:right}}
.th-table .td-link a{{color:#0057d9;text-decoration:none;font-size:.8rem}}
.th-table .td-type a{{color:#111;text-decoration:none;font-weight:500}}
.th-table .td-type a:hover{{color:#0057d9;text-decoration:underline}}
.leg{{margin-top:28px;padding-top:18px;border-top:1px solid #f0f0f0}}
.leg-title{{font-size:.68rem;font-weight:700;letter-spacing:1.2px;text-transform:uppercase;color:#bbb;margin-bottom:10px}}
.leg-row{{display:flex;gap:8px;align-items:flex-start;margin-bottom:7px;font-size:.78rem;color:#555;line-height:1.45}}
.leg-row .sig{{flex-shrink:0;margin-top:1px}}
.ic-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:16px;margin-bottom:52px}}
.ic{{background:#fafafa;border:1px solid #f0f0f0;border-radius:10px;padding:20px 22px}}
.ic-month{{font-size:.72rem;font-weight:600;letter-spacing:1.5px;text-transform:uppercase;color:#aaa;margin-bottom:10px}}
.ic-stat{{font-size:1.1rem;font-weight:500;color:#111;margin-bottom:4px}}
.ic-detail{{font-size:.8rem;color:#666;margin-bottom:4px}}
.ic-link{{font-size:.78rem;color:#0057d9;text-decoration:none}}
.ic-link:hover{{text-decoration:underline}}
.grp-ins{{margin-top:40px}}
.ft{{margin-top:48px;font-size:.68rem;color:#ccc}}
</style>
</head>
<body>

<div class="hdr">
  <h1>Creative KPIs</h1>
  <div class="vnav">
    <button class="nbtn active" id="btn-combined"  onclick="showView('combined')">Combined</button>
    <button class="nbtn"        id="btn-separated" onclick="showView('separated')">Separated</button>
  </div>
</div>

<!-- ═══ COMBINED VIEW ═══ -->
<div id="view-combined">
  <div class="sl">Deliverables</div>
  <table>
    <thead><tr><th class="ml"></th>{mh}</tr></thead>
    <tbody>{dr_c}</tbody>
  </table>

  <div class="sl">Time to First Response</div>
  <table>
    <thead><tr><th class="ml"></th>{mh}</tr></thead>
    <tbody>{rr_c}</tbody>
  </table>

  <div class="sl" style="margin-top:8px">Monthly Insights</div>
  <div class="ic-grid" id="ins-combined"></div>
</div>

<!-- ═══ SEPARATED VIEW ═══ -->
<div id="view-separated" style="display:none">

  <div class="grp-hdr">Product Deliverables</div>

  <div class="sl">Deliverables</div>
  <table>
    <thead><tr><th class="ml"></th>{mh}</tr></thead>
    <tbody>{dr_p}</tbody>
  </table>

  <div class="sl">Time to First Response</div>
  <table>
    <thead><tr><th class="ml"></th>{mh}</tr></thead>
    <tbody>{rr_p}</tbody>
  </table>

  <div class="sl" style="margin-top:8px">Product Insights</div>
  <div class="ic-grid" id="ins-product"></div>

  <div class="grp-hdr">Marketing Deliverables</div>

  <div class="sl">Deliverables</div>
  <table>
    <thead><tr><th class="ml"></th>{mh}</tr></thead>
    <tbody>{dr_m}</tbody>
  </table>

  <div class="sl">Time to First Response</div>
  <table>
    <thead><tr><th class="ml"></th>{mh}</tr></thead>
    <tbody>{rr_m}</tbody>
  </table>

  <div class="sl" style="margin-top:8px">Marketing Insights</div>
  <div class="ic-grid" id="ins-marketing"></div>

</div>

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
const THREAD_DETAILS    = {thread_details_js};
const INSIGHTS_COMBINED = {insights_combined_js};
const INSIGHTS_PRODUCT  = {insights_product_js};
const INSIGHTS_MARKETING= {insights_marketing_js};
const MONTH_KEYS_MAP    = {{"JAN":"01","FEB":"02","MAR":"03","APR":"04","MAY":"05","JUN":"06","JUL":"07","AUG":"08","SEP":"09","OCT":"10","NOV":"11","DEC":"12"}};

function showView(v) {{
  document.getElementById('view-combined').style.display  = v === 'combined'  ? '' : 'none';
  document.getElementById('view-separated').style.display = v === 'separated' ? '' : 'none';
  document.getElementById('btn-combined').classList.toggle('active',  v === 'combined');
  document.getElementById('btn-separated').classList.toggle('active', v === 'separated');
  if (v === 'combined')  {{ renderInsights(INSIGHTS_COMBINED,  'ins-combined');  }}
  if (v === 'separated') {{
    renderInsights(INSIGHTS_PRODUCT,   'ins-product');
    renderInsights(INSIGHTS_MARKETING, 'ins-marketing');
  }}
}}

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

function sigClass(s) {{
  if (s === 'High cycles')     return 'sig sig-hc';
  if (s === 'Longer Gap')      return 'sig sig-lg';
  if (s === 'Slow feedback')   return 'sig sig-sf';
  if (s === 'Slow pickup')     return 'sig sig-sp';
  if (s === 'Long discussion') return 'sig sig-ld';
  return 'sig sig-ot';
}}

function renderInsights(data, containerId) {{
  const el = document.getElementById(containerId);
  if (!el) return;
  const months = Object.keys(data).sort().reverse().slice(0, 6);
  if (!months.length) {{ el.innerHTML = '<div class="nd">No data yet</div>'; return; }}
  const SIG_ORDER = ['High cycles','Slow feedback','Slow pickup','Long discussion','On track'];
  el.innerHTML = months.map(ym => {{
    const d = data[ym];
    if (!d) return '';
    const [y, m] = ym.split('-');
    const mName = {{"01":"Jan","02":"Feb","03":"Mar","04":"Apr","05":"May","06":"Jun","07":"Jul","08":"Aug","09":"Sep","10":"Oct","11":"Nov","12":"Dec"}}[m] + ' ' + y;
    // Signal breakdown lines — sorted by SIG_ORDER, all signals shown
    const breakdown = d.signal_breakdown || {{}};
    const bLines = SIG_ORDER
      .filter(s => breakdown[s])
      .map(s => `<div class="ic-detail">${{breakdown[s]}} — ${{s}}</div>`)
      .join('');
    const onTrack = breakdown['On track'] || 0;
    const flagged = d.total - onTrack;
    const summaryLine = flagged > 0
      ? `${{flagged}} of ${{d.total}} flagged`
      : `All ${{d.total}} on track`;
    return `<div class="ic">
      <div class="ic-month">${{mName}}</div>
      <div class="ic-stat">${{d.total}} deliverable${{d.total!==1?'s':''}}</div>
      <div class="ic-detail" style="margin-bottom:8px;color:#888">${{summaryLine}}</div>
      ${{bLines}}
    </div>`;
  }}).join('');
}}

function showDrill(el) {{
  const metricKey = el.dataset.metricKey || '';
  const s = el.dataset.suffix || '';
  const month = el.dataset.month;
  const year  = el.dataset.year;
  const mk    = MONTH_KEYS_MAP[month] || '';
  const ym    = year + '-' + mk;

  document.getElementById('pt').textContent = el.dataset.metric;
  document.getElementById('ps').textContent = month + ' ' + year;
  document.getElementById('ov').classList.add('on');
  document.getElementById('pnl').classList.add('open');

  if (metricKey === 'task_days_per_d') {{
    const byDesigner = THREAD_DETAILS[ym] || {{}};
    const entries = Object.entries(byDesigner);
    if (!entries.length) {{
      document.getElementById('pb').innerHTML = '<div class="nd">No thread data</div>';
      return;
    }}
    document.getElementById('pb').innerHTML = entries
      .sort((a,b) => {{
        const avgA = a[1].reduce((s,t)=>s+t.task_days,0)/a[1].length;
        const avgB = b[1].reduce((s,t)=>s+t.task_days,0)/b[1].length;
        return avgB - avgA;
      }})
      .map(([name, threads]) => {{
        const avg = Math.round(threads.reduce((s,t)=>s+t.task_days,0)/threads.length);
        const n   = threads.length;
        const tableRows = threads
          .sort((a,b) => b.task_days - a.task_days)
          .map(t => {{
            const typeLabel = t.deliverable_type || '—';
            const typeCell  = t.slack_url
              ? `<a href="${{t.slack_url}}" target="_blank" title="${{typeLabel}}">${{typeLabel}}</a>`
              : `<span title="${{typeLabel}}">${{typeLabel}}</span>`;
            const mgr = t.manager_wait_bdays != null ? `${{Math.round(t.manager_wait_bdays)}}d` : '—';
            const des = t.designer_wait_bdays != null ? `${{Math.round(t.designer_wait_bdays)}}d` : '—';
            const gap = t.max_gap_bdays     != null ? `${{Math.round(t.max_gap_bdays)}}d`     : '—';
            return `<tr>
              <td class="td-type">${{typeCell}}</td>
              <td><span class="${{sigClass(t.signal)}}">${{t.signal}}</span></td>
              <td class="td-num">${{t.cycle_count}}</td>
              <td class="td-num">${{mgr}}</td>
              <td class="td-num">${{des}}</td>
              <td class="td-num">${{gap}}</td>
            </tr>`;
          }}).join('');
        return `<div class="des-block">
          <div class="des-hdr">${{name}}<span class="des-sub">${{n}} deliverable${{n!==1?'s':''}} · AVG ${{avg}}D</span></div>
          <table class="th-table">
            <thead><tr>
              <th>Type</th><th>Signal</th>
              <th style="text-align:center">Cycles</th>
              <th style="text-align:center">Mgr</th>
              <th style="text-align:center">Des</th>
              <th style="text-align:center">Gap</th>
            </tr></thead>
            <tbody>${{tableRows}}</tbody>
          </table>
        </div>`;
      }}).join('') + `<div class="leg">
      <div class="leg-title">Signals</div>
      <div class="leg-row"><span class="sig sig-hc">High cycles</span>3+ revision rounds</div>
      <div class="leg-row"><span class="sig sig-lg">Longer Gap</span>5+ business days between any two messages</div>
      <div class="leg-row"><span class="sig sig-sf">Slow feedback</span>Manager took &gt;2 days to respond after designer action</div>
      <div class="leg-row"><span class="sig sig-sp">Slow pickup</span>Designer took &gt;2 days to act after manager feedback</div>
      <div class="leg-row"><span class="sig sig-ld">Long discussion</span>8+ replies with ≤1 revision cycle</div>
      <div class="leg-row"><span class="sig sig-ot">On track</span>No issues detected</div>
      <div class="leg-title" style="margin-top:14px">Columns</div>
      <div class="leg-row"><strong>Cycles</strong> — Number of additional "For review" / "For feedback" rounds after the first</div>
      <div class="leg-row"><strong>Mgr</strong> — Avg business days for manager to respond after each designer action</div>
      <div class="leg-row"><strong>Des</strong> — Avg business days for designer to pick up after manager feedback</div>
      <div class="leg-row"><strong>Gap</strong> — Longest stretch (business days) between any two consecutive messages in the thread</div>
    </div>`;
    return;
  }}

  let d = {{}};
  try {{ d = JSON.parse(el.dataset.drill); }} catch(e) {{}}
  const entries = Object.entries(d);
  document.getElementById('pb').innerHTML = entries.length
    ? entries.map(([n,v]) => `<div class="dr"><span class="dn">${{n}}</span><span class="dv">${{v}}${{s}}</span></div>`).join('')
    : '<div class="nd">No breakdown available</div>';
}}

function close_() {{
  document.getElementById('ov').classList.remove('on');
  document.getElementById('pnl').classList.remove('open');
}}
renderInsights(INSIGHTS_COMBINED, 'ins-combined');
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
    existing_c, existing_p, existing_m = {}, {}, {}
    if os.path.exists("data.json"):
        try:
            saved = json.load(open("data.json"))
            existing_c = saved.get("metrics",           {})
            existing_p = saved.get("metrics_product",   {})
            existing_m = saved.get("metrics_marketing", {})
            print(f"  Found combined data for: {sorted(existing_c.keys())}")
        except Exception:
            print("  Could not parse data.json — starting fresh")
    else:
        print("  No data.json found — starting fresh")

    print("\n[4/5] Fetching and processing Slack data...")
    month_data = process_slack(client, CHANNEL_ID, users, managers, start_dt, end_dt)

    new_c = compute_metrics(month_data, managers)
    new_p = compute_metrics(month_data, managers, roster=PRODUCT_DESIGNERS)
    new_m = compute_metrics(month_data, managers, roster=MARKETING_DESIGNERS)

    for m, d in sorted(new_c.items()):
        print(f"  {m}: {d['num_ds']} Ds (combined) | "
              f"product={new_p.get(m,{}).get('num_ds',0)} | "
              f"marketing={new_m.get(m,{}).get('num_ds',0)}")

    merged_c = {**existing_c, **new_c}
    merged_p = {**existing_p, **new_p}
    merged_m = {**existing_m, **new_m}

    print("\n[5/5] Writing output files...")
    html = generate_html(merged_c, merged_p, merged_m)
    with open("index.html", "w") as f: f.write(html)
    print("  ✓ index.html")

    with open("data.json", "w") as f:
        json.dump({
            "last_updated":      datetime.now(tz=timezone.utc).isoformat(),
            "metrics":           merged_c,
            "metrics_product":   merged_p,
            "metrics_marketing": merged_m,
            "targets":           TARGETS,
        }, f, indent=2)
    print("  ✓ data.json")

    print(f"\n{'═'*56}")
    print("  Done! Files written. GitHub Actions will commit & push.")
    print(f"{'═'*56}\n")


if __name__ == "__main__":
    import traceback, sys
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(1)
