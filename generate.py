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
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")  # optional — enables AI signal summaries
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

# Override working hours for specific people (display_name → (bh_start, bh_end) in local tz)
# Default is BH_START–BH_END (8–17) in their Slack profile timezone
REVIEWER_HOURS_OVERRIDE = {
    "Nandu": (15, 22),   # 3:30pm–10:30pm IST (approximated to hour boundaries)
}

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


# ── US federal holidays (for gap calculation) ────────────────────────────────
import calendar as _cal

def _us_federal_holidays(year):
    """Return a set of date objects for US federal holidays in the given year."""
    from datetime import date as _date

    def nth_weekday(y, mo, wd, n):
        d = _date(y, mo, 1)
        d += timedelta(days=(wd - d.weekday()) % 7)
        return d + timedelta(weeks=n - 1)

    def last_weekday(y, mo, wd):
        last = _date(y, mo, _cal.monthrange(y, mo)[1])
        return last - timedelta(days=(last.weekday() - wd) % 7)

    def observe(d):
        if d.weekday() == 6: return d + timedelta(days=1)
        if d.weekday() == 5: return d - timedelta(days=1)
        return d

    from datetime import date as D
    h = set()
    h.add(observe(D(year, 1,  1)))   # New Year's Day
    h.add(observe(D(year, 6, 19)))   # Juneteenth
    h.add(observe(D(year, 7,  4)))   # Independence Day
    h.add(observe(D(year, 11, 11)))  # Veterans Day
    h.add(observe(D(year, 12, 25)))  # Christmas
    h.add(nth_weekday(year, 1, 0, 3))   # MLK Day       — 3rd Mon Jan
    h.add(nth_weekday(year, 2, 0, 3))   # Presidents    — 3rd Mon Feb
    h.add(last_weekday(year, 5, 0))     # Memorial Day  — last Mon May
    h.add(nth_weekday(year, 9, 0, 1))   # Labor Day     — 1st Mon Sep
    h.add(nth_weekday(year, 10, 0, 2))  # Columbus Day  — 2nd Mon Oct
    h.add(nth_weekday(year, 11, 3, 4))  # Thanksgiving  — 4th Thu Nov
    return h

_HOLIDAY_CACHE = {}
def _get_holidays(year):
    if year not in _HOLIDAY_CACHE:
        _HOLIDAY_CACHE[year] = _us_federal_holidays(year)
    return _HOLIDAY_CACHE[year]

def business_days_gap(start_ts, end_ts):
    """Business days between two timestamps, excluding weekends AND US federal holidays."""
    from datetime import date as _date
    s = datetime.fromtimestamp(float(start_ts), tz=timezone.utc).date()
    e = datetime.fromtimestamp(float(end_ts),   tz=timezone.utc).date()
    if e <= s: return 0.0
    count, cur = 0.0, s
    while cur < e:
        if cur.weekday() < 5 and cur not in _get_holidays(cur.year):
            count += 1
        cur += timedelta(days=1)
    return count


LONGER_GAP_THRESHOLD  = 5   # business days (excl. weekends + US holidays)
HIGH_CYCLES_THRESHOLD = 6   # additional review/feedback rounds after first submission

def compute_max_gap_bdays(thread):
    """Largest holiday-aware business-day gap between consecutive messages in a thread."""
    timestamps = sorted(float(m["ts"]) for m in thread)
    if len(timestamps) < 2:
        return None
    max_gap = 0.0
    for i in range(1, len(timestamps)):
        gap = business_days_gap(timestamps[i-1], timestamps[i])
        if gap > max_gap:
            max_gap = gap
    return max_gap if max_gap > 0 else None


LONG_DISCUSSION_REPLIES_PER_CYCLE = 5  # replies within a single cycle to flag Long discussion

def compute_signal(cycle_count, reviewer_wait, designer_wait, reply_count, max_gap=None, max_replies_per_cycle=None):
    """Return the single most prominent signal, chosen by highest underlying value."""
    candidates = []
    if cycle_count >= HIGH_CYCLES_THRESHOLD:
        candidates.append(("High rework", cycle_count))
    if max_gap is not None and max_gap >= LONGER_GAP_THRESHOLD:
        candidates.append(("Slow pickup", max_gap))
    if reviewer_wait is not None and reviewer_wait > 2:
        candidates.append(("Late feedback", reviewer_wait))
    # Long discussion: any single cycle had 5+ replies (feedback required negotiation)
    if max_replies_per_cycle is not None and max_replies_per_cycle >= LONG_DISCUSSION_REPLIES_PER_CYCLE:
        candidates.append(("Long discussion", max_replies_per_cycle))
    if not candidates:
        return "On track"
    return max(candidates, key=lambda x: x[1])[0]


def business_hours_between(start_ts, end_ts, tz_str, bh_start=None, bh_end=None):
    if bh_start is None: bh_start = BH_START
    if bh_end   is None: bh_end   = BH_END
    try:    tz = ZoneInfo(tz_str)
    except: tz = ZoneInfo("America/New_York")
    s = datetime.fromtimestamp(float(start_ts), tz=tz)
    e = datetime.fromtimestamp(float(end_ts),   tz=tz)
    if e <= s: return 0.0
    total, cur = 0.0, s
    while cur < e:
        if cur.weekday() < 5:
            open_  = cur.replace(hour=bh_start, minute=0, second=0, microsecond=0)
            close_ = cur.replace(hour=bh_end,   minute=0, second=0, microsecond=0)
            ws, we = max(cur, open_), min(e, close_)
            if ws < we:
                total += (we - ws).total_seconds() / 3600.0
        cur = (cur + timedelta(days=1)).replace(hour=bh_start, minute=0, second=0, microsecond=0)
    return round(total, 2)

# ═══════════════════════════════════════════════════════════════════════════════
#  TEXT HELPERS  (handle Slack bold/italic and rich-text blocks)
# ═══════════════════════════════════════════════════════════════════════════════

def normalize(text):
    """Strip Slack markdown markers, URLs, and fix spacing around colons."""
    text = text or ''
    text = re.sub(r'[*_]', '', text)
    # Slack URL with display text anywhere: <http://url|display> → keep display text
    text = re.sub(r'<https?://[^|>]+\|([^>]+)>', r'\1', text)
    # Slack bare URL anywhere: <http://url> → remove
    text = re.sub(r'<https?://[^>]+>', '', text)
    # Plain-text URLs anywhere (http:// or https:// not in brackets) → remove
    text = re.sub(r'https?://\S+', '', text)
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


def get_tagged_users(text, exclude_ids=None):
    """Return list of user IDs @mentioned in text, excluding exclude_ids."""
    import re
    exclude = set(exclude_ids or [])
    return [uid for uid in re.findall(r'<@([A-Z0-9]+)>', text) if uid not in exclude]


def msg_has(msg, phrase):
    """True if phrase appears (case-insensitive) in the message's text or blocks."""
    return phrase.lower() in get_full_text(msg).lower()


def is_review(msg):    return msg_has(msg, "for review:")
def is_feedback(msg):  return msg_has(msg, "for feedback:")
def is_cycle_msg(msg): return is_review(msg) or is_feedback(msg)

def extract_deliverable_type(msg):
    """Extract the label after 'For review:' / 'For feedback:', always returns non-empty string."""
    text = get_full_text(msg)
    found_trigger = None
    for trigger in ["for review:", "for feedback:"]:
        idx = text.lower().find(trigger)
        if idx != -1:
            found_trigger = "Review" if "review" in trigger else "Feedback"
            after = text[idx + len(trigger):].strip()
            # Take first non-empty line, first 4 words
            for line in after.split('\n'):
                line = line.strip()
                if line:
                    label = " ".join(line.split()[:4]).strip()
                    if label:
                        return label[:40]
            # Nothing after the colon — use the trigger word
            return found_trigger
    # No trigger found — use first 40 chars of message, or generic fallback
    snippet = " ".join(text.split()[:5]).strip()
    return snippet[:40] if snippet else "—"

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


GEMINI_URL = ("https://generativelanguage.googleapis.com/v1beta/"
              "models/gemini-3.1-flash-lite:generateContent?key={key}")

_gemini_call_count = 0

_SIGNAL_CONTEXT = {
    "High rework":     "the designer went through 7+ revision rounds — something drove repeated changes",
    "Slow pickup":     "the designer took 5+ business days to respond — something slowed or blocked them",
    "Long discussion": "there were 8+ messages with ≤1 revision cycle — extended back-and-forth without resolution",
    "Late feedback":   "the reviewer took 2+ business days to respond — something delayed their review",
}

def generate_signal_summary(thread, signal, deliverable_type, users, mgr_ids):
    """Call Gemini Flash to get a one-line summary + root-cause reasons for a flagged thread.
    Returns a dict: {"summary": str|None, "reasons": str|None}
    """
    global _gemini_call_count
    if not GEMINI_API_KEY or signal == "On track":
        return None

    lines = []
    for m in thread[:25]:
        uid  = m.get("user", "")
        name = users.get(uid, {}).get("display_name", "?")
        text = get_full_text(m).strip()
        if text:
            role = "Manager" if uid in mgr_ids else "Designer"
            lines.append(f"[{role}: {name}] {text[:180]}")

    transcript = "\n".join(lines)
    signal_ctx = _SIGNAL_CONTEXT.get(signal, "")
    prompt = (
        f'Analyze this Slack design review thread.\n'
        f'Deliverable: "{deliverable_type}"\n'
        f'Signal: {signal} — {signal_ctx}\n\n'
        f'Note: The reviewer for each round is whoever the designer @tagged in their submission message — '
        f'this may be a manager, another designer, or any team member. '
        f'If a non-manager responded and the manager did not, that is expected — do not flag manager as bottleneck.\n\n'
        f'Thread:\n{transcript}\n\n'
        f'Return EXACTLY this format (two lines, nothing else):\n'
        f'Summary: [One sentence, max 12 words, plain simple language. '
        f'Explain the specific underlying cause — do NOT restate what the signal already says. '
        f'Be precise: "Stakeholder changed heart placement direction mid-review" not "Design direction shifted repeatedly".]\n'
        f'Issues: [Up to 2 root causes of WHY the signal happened, comma-separated, ordered by importance. '
        f'2-3 words each. Label if reasonably evidenced in the thread — you do not need certainty, just clear indication. '
        f'If the signal is fully explained by itself (e.g. manager simply missed a message), write "None". '
        f'Do NOT invent issues not evidenced in the thread.\n'
        f'Rules for each label:\n'
        f'- "Scope Changed": only if scope was explicitly expanded or reduced AFTER work began, with evidence.\n'
        f'- "Direction Changed": only if the SAME person reversed or significantly changed their OWN direction between rounds. Also applies when a manager requested changes to image usage, cropping, or layout after initial submission.\n'
        f'- "Conflicting Input": only if TWO OR MORE reviewers gave opposing directions IN THE SAME ROUND. Multiple people giving feedback does not qualify.\n'
        f'- "Unclear Requirements": only if the thread shows the designer misunderstood requirements because they were ambiguous upfront — not just because feedback arrived.\n'
        f'- "Copy Alignment": use whenever copy and design were not in sync — including debates over copy tone, wording, CTA text, or copy placement that caused rework or prolonged review. Apply even if there is also a visual/UI issue.\n'
        f'- "Missing Pattern": no existing design system component or established pattern existed for what was needed. Also applies when team debated whether to create new visual styles (colors, layouts) versus reusing existing ones — indicating a gap in the design system.\n'
        f'- "Priority Changed": designer paused, deprioritized, or delayed the work due to competing delivery commitments or being pulled to other tasks.\n'
        f'- "Brand Constraint": brand guidelines directly limited or changed the design direction.\n'
        f'- "Asset Dependency": work was blocked waiting for assets from another team or person.\n'
        f'- "Technical Limit": a platform or engineering constraint changed what was designable.\n'
        f'- "Accessibility Gap": accessibility requirements caused a design change.\n'
        f'- "Missed Usecase": an unexpected user scenario or flow was not accounted for during design, discovered during review.\n'
        f'- "Design Oversight": a visible detail already present in the design was missed by the designer before submission (not a missing scenario — something observable that was overlooked). Also applies when feedback was not correctly incorporated and required a sync.\n'
        f'If none fit, write "None".]\n\n'
        f'Good example (Direction Changed):\n'
        f'Summary: Stakeholder reversed heart placement direction after approving it.\n'
        f'Issues: Direction Changed\n\n'
        f'Good example (two issues):\n'
        f'Summary: Copy tone debated across rounds while CTA placement also shifted.\n'
        f'Issues: Copy Alignment, Direction Changed\n\n'
        f'Good example (Copy Alignment for copy debate):\n'
        f'Summary: Team debated motivational vs clarity copy for badge states across rounds.\n'
        f'Issues: Copy Alignment\n\n'
        f'Good example (Missing Pattern for color/style gap):\n'
        f'Summary: Team had no established palette guidance, debated new vs existing colors.\n'
        f'Issues: Missing Pattern\n\n'
        f'Good example (Priority Changed):\n'
        f'Summary: Designer paused work to handle competing delivery commitments.\n'
        f'Issues: Priority Changed\n\n'
        f'Good example (no issue beyond signal):\n'
        f'Summary: Manager missed the Slack submission and responded after follow-up.\n'
        f'Issues: None\n\n'
        f'Bad Issues — never use these:\n'
        f'"Conflicting Input" when only one person reviewed.\n'
        f'"Scope Changed" when scope was decomposed or planned, not changed mid-work.\n'
        f'"Unclear Requirements" when the brief was clear but scope was large.\n'
        f'"Direction Changed" when a manager gave normal iterative feedback across rounds.\n'
        f'Any label that restates what the signal already captures.'
    )

    url  = GEMINI_URL.format(key=GEMINI_API_KEY)
    body = {"contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"maxOutputTokens": 300, "temperature": 0.3}}

    for attempt in range(3):
        try:
            r = requests.post(url, json=body, timeout=30)
            if r.status_code == 429:
                wait = 30 * (attempt + 1)
                print(f"  Gemini rate-limited — waiting {wait}s")
                time.sleep(wait)
                continue
            if r.status_code != 200:
                print(f"  Gemini HTTP {r.status_code}: {r.text[:300]}")
                time.sleep(5)
                continue
            data = r.json()
            _gemini_call_count += 1
            # Print full response for first 3 calls to verify structure
            if _gemini_call_count <= 3:
                print(f"  Gemini DEBUG call #{_gemini_call_count} raw: {r.text[:600]}")
            candidates = data.get("candidates", [])
            if not candidates:
                feedback = data.get("promptFeedback", {})
                print(f"  Gemini no candidates — promptFeedback: {feedback}")
                return None
            content = candidates[0].get("content", {})
            parts   = content.get("parts", [])
            if not parts:
                finish = candidates[0].get("finishReason", "unknown")
                print(f"  Gemini empty parts — finishReason: {finish}")
                return None
            raw_text = parts[0].get("text", "").strip()
            if not raw_text:
                print(f"  Gemini empty text after strip — raw_text repr: {repr(raw_text[:100])}")
                return None
            # Parse "Summary: ...\nIssues: ..." format
            summary, issue = None, None
            for line in raw_text.splitlines():
                line = line.strip()
                if line.lower().startswith("summary:"):
                    summary = line[len("summary:"):].strip().strip('"\'') or None
                elif line.lower().startswith("issues:"):
                    v = line[len("issues:"):].strip().strip('"\'')
                    if v and v.lower() != "none":
                        issue = v
            if summary or issue:
                return {"summary": summary, "issue": issue}
            # Fallback: treat entire response as summary
            return {"summary": raw_text[:150], "issue": None}
        except Exception as e:
            print(f"  Gemini error (attempt {attempt+1}): {type(e).__name__}: {e}")
            time.sleep(5)
    print("  Gemini gave up after 3 attempts")
    return None


def process_deliverable_thread(thread, users, managers, month_data, start_ts, end_ts,
                               ai_summaries=None, reviewer_hours=None):
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
    _reviewer_hours = reviewer_hours or {}

    for uid in first_deliv_idx:
        deliv_idx = first_deliv_idx[uid]
        deliv_msg = thread[deliv_idx]
        month = ts_month(deliv_msg["ts"])
        pinfo = users.get(uid, {})
        pname = pinfo.get("display_name") or pinfo.get("name") or "Unknown"

        cycle_data = []
        for cyc_msg in designer_cycles[uid]:
            cts = float(cyc_msg["ts"])
            full_txt = get_full_text(cyc_msg)
            # Tagged reviewers: anyone @mentioned in the cycle opener except the designer
            tagged_ids = get_tagged_users(full_txt, exclude_ids={uid})
            tagged_set = set(tagged_ids)

            # Find first response from any tagged person
            first_rev_ts, first_rev_id = None, None
            if tagged_set:
                for m in thread:
                    mts = float(m["ts"])
                    if mts > cts and m.get("user") in tagged_set:
                        first_rev_ts, first_rev_id = mts, m["user"]
                        break  # thread is chronological; first match wins

            resp_time = None
            if first_rev_ts and first_rev_id:
                rev_tz  = users.get(first_rev_id, {}).get("tz", "America/New_York")
                rev_bh  = _reviewer_hours.get(first_rev_id, (BH_START, BH_END))
                bh = business_hours_between(cts, first_rev_ts, rev_tz, rev_bh[0], rev_bh[1])
                if bh <= NO_RESPONSE_THRESHOLD_BH:
                    resp_time = bh

            cycle_data.append({
                "ts":                      cyc_msg["ts"],
                "tagged_reviewer_ids":     tagged_ids,
                "response_time_hours":     resp_time,
                "responding_reviewer_id":  first_rev_id if resp_time is not None else None,
            })

        # Business days (Mon–Fri, weekends excluded) from this designer's deliverable
        # to the last message in the thread
        last_thread_ts = max(float(m["ts"]) for m in thread)
        task_days = business_days_between(float(deliv_msg["ts"]), last_thread_ts)

        # ── Phase times ──────────────────────────────────────────────────────
        # Phase 1 (reviewer wait): how long until a tagged reviewer replied after each
        #   designer action (deliverable + each cycle).
        # Phase 2 (designer pickup): how long the designer took to post their
        #   next cycle after receiving reviewer feedback.
        all_cyc_ts  = sorted(float(c["ts"]) for c in designer_cycles[uid])
        deliv_ts_f  = float(deliv_msg["ts"])

        reviewer_wait_list, des_wait_list = [], []
        reviewer_response_ts = []   # timestamps of reviewer responses (for designer-wait calc)

        def _first_reviewer_response(after_ts, tagged_set):
            """Return timestamp of first tagged-reviewer response after after_ts, or None."""
            if not tagged_set:
                return None
            for m in thread:
                mts = float(m["ts"])
                if mts > after_ts and m.get("user") in tagged_set:
                    return mts
            return None

        # After initial deliverable
        deliv_tagged = set(get_tagged_users(get_full_text(deliv_msg), exclude_ids={uid}))
        nr = _first_reviewer_response(deliv_ts_f, deliv_tagged)
        if nr:
            reviewer_wait_list.append(business_days_between(deliv_ts_f, nr))
            reviewer_response_ts.append(nr)

        # After each cycle
        for cyc_msg in designer_cycles[uid]:
            cyc_ts = float(cyc_msg["ts"])
            cyc_tagged = set(get_tagged_users(get_full_text(cyc_msg), exclude_ids={uid}))
            nr = _first_reviewer_response(cyc_ts, cyc_tagged)
            if nr:
                reviewer_wait_list.append(business_days_between(cyc_ts, nr))
                reviewer_response_ts.append(nr)
            # Designer wait: time from last reviewer response to this cycle
            pm = max((ts for ts in reviewer_response_ts if ts < cyc_ts), default=None)
            if pm:
                des_wait_list.append(business_days_between(pm, cyc_ts))

        avg_reviewer_wait = round(sum(reviewer_wait_list) / len(reviewer_wait_list), 1) if reviewer_wait_list else None
        avg_des_wait = round(sum(des_wait_list) / len(des_wait_list), 1) if des_wait_list else None
        max_gap      = compute_max_gap_bdays(thread)

        # Compute max replies within any single cycle (between consecutive designer submissions)
        cycle_boundaries = sorted([float(thread[deliv_idx]["ts"])] +
                                  [float(c["ts"]) for c in designer_cycles[uid]])
        max_replies_per_cycle = None
        if len(cycle_boundaries) >= 1:
            # Count replies in each window: [boundary[i], boundary[i+1]) for i in 0..n-2,
            # plus the window after the last cycle boundary.
            windows = list(zip(cycle_boundaries, cycle_boundaries[1:])) + [(cycle_boundaries[-1], float('inf'))]
            counts = []
            for w_start, w_end in windows:
                n = sum(
                    1 for m in thread
                    if w_start < float(m["ts"]) < w_end
                    and not (m.get("user") == uid and is_cycle_msg(m))
                    and not (m.get("user") == uid and float(m["ts"]) == w_start)
                )
                counts.append(n)
            if counts:
                max_replies_per_cycle = max(counts)

        signal    = compute_signal(len(designer_cycles[uid]), avg_reviewer_wait, avg_des_wait,
                                   designer_reply_count[uid], max_gap,
                                   max_replies_per_cycle=max_replies_per_cycle)
        slack_url  = make_slack_url(thread[0]["ts"])
        deliv_type = extract_deliverable_type(deliv_msg)

        # AI insight — check cache first, then call Gemini for flagged threads
        summary_key  = f"{thread[0]['ts']}:{uid}:{thread[-1]['ts']}"
        ai_summary   = None
        ai_issue     = None
        if ai_summaries is not None:
            cached = ai_summaries.get(summary_key, "MISSING")
            # Re-run if: never cached, failed, old string format, or old dict without 'issue' key
            needs_run = (
                cached == "MISSING" or
                cached is None or
                isinstance(cached, str) or
                (isinstance(cached, dict) and "issue" not in cached)
            )
            if not needs_run:
                ai_summary = cached.get("summary")
                ai_issue   = cached.get("issue")
            # Run Gemini
            if needs_run and signal != "On track":
                print(f"    Gemini: summarising [{signal}] for {pname}…")
                result = generate_signal_summary(thread, signal, deliv_type, users, mgr_ids)
                if result:
                    ai_summary = result.get("summary")
                    ai_issue   = result.get("issue")
                ai_summaries[summary_key] = result  # cache (even if None)
                time.sleep(5)  # 5s between calls to stay within 15 RPM free-tier limit

        month_data.setdefault(month, []).append({
            "root_ts":             thread[0]["ts"],
            "month":               month,
            "poster_id":           uid,
            "poster_name":         pname,
            "deliverable_type":    deliv_type,
            "cycle_count":         len(designer_cycles[uid]),
            "reply_count":         designer_reply_count[uid],
            "task_days":           task_days,
            "reviewer_wait_bdays": avg_reviewer_wait,
            "designer_wait_bdays": avg_des_wait,
            "max_gap_bdays":       max_gap,
            "signal":              signal,
            "slack_url":           slack_url,
            "ai_summary":          ai_summary,
            "ai_issue":            ai_issue,
            "cycles":              cycle_data,
        })


def process_slack(client, channel_id, users, managers, start_dt, end_dt, ai_summaries=None,
                  reviewer_hours=None):
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
        process_deliverable_thread(thread, users, managers, month_data, start_ts, end_ts,
                                   ai_summaries=ai_summaries, reviewer_hours=reviewer_hours)

    total_ds = sum(len(v) for v in month_data.values())
    print(f"  Total deliverable entries found: {total_ds}")

    return month_data


def compute_metrics(month_data, managers, users=None, roster=None):
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
                    mid   = c.get("responding_reviewer_id")
                    label = managers.get(mid, {}).get("manager_label") or (users or {}).get(mid, {}).get("display_name") or (mid or "Unknown")
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
                "reply_count":         d.get("reply_count", 0),
                "reviewer_wait_bdays": d.get("reviewer_wait_bdays"),
                "max_gap_bdays":       d.get("max_gap_bdays"),
                "signal":              d.get("signal", "On track"),
                "slack_url":           d.get("slack_url", ""),
                "ai_summary":          d.get("ai_summary"),
                "ai_issue":            d.get("ai_issue"),
            })

        # ── Monthly insight ───────────────────────────────────────────────────
        all_signals = [d.get("signal", "On track") for d in deliverables]
        flagged     = [s for s in all_signals if s != "On track"]
        most_common = max(set(flagged), key=flagged.count) if flagged else None
        slowest     = max(deliverables, key=lambda d: d.get("task_days", 0))
        # Signal breakdown
        signal_counts = {}
        for s in all_signals:
            signal_counts[s] = signal_counts.get(s, 0) + 1
        # Avg days: all vs flagged vs on-track
        flagged_days   = [d.get("task_days", 0) for d in deliverables if d.get("signal") != "On track"]
        ontrack_days   = [d.get("task_days", 0) for d in deliverables if d.get("signal") == "On track"]
        avg_days_all     = round(tt / n, 1)
        avg_days_flagged = round(sum(flagged_days) / len(flagged_days), 1) if flagged_days else None
        avg_days_ontrack = round(sum(ontrack_days) / len(ontrack_days), 1) if ontrack_days else None
        # Aggregate AI issue labels for dashboard
        issue_counts = {}
        for d in deliverables:
            raw = d.get("ai_issue") or ""
            for label in raw.split(","):
                label = label.strip().strip(".")
                if label and label.lower() != "none":
                    issue_counts[label] = issue_counts.get(label, 0) + 1
        top_issues = sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)
        monthly_insight = {
            "total":              n,
            "flagged_count":      len(flagged),
            "most_common_signal": most_common,
            "signal_breakdown":   signal_counts,
            "slowest_days":       slowest.get("task_days", 0),
            "slowest_url":        slowest.get("slack_url", ""),
            "avg_days_all":       avg_days_all,
            "avg_days_flagged":   avg_days_flagged,
            "avg_days_ontrack":   avg_days_ontrack,
            "top_issues":         top_issues,
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
            "Only Mon\u2013Fri, 8am\u20135pm in the responding reviewer\u2019s timezone counts",
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

    def safe_js(obj):
        """JSON-encode obj and escape sequences that would break an inline <script> block."""
        return (json.dumps(obj)
                .replace("</", "<\\/")   # prevents </script> from ending the block
                .replace("<!--", "<\\!--"))

    # JS data blobs for thread breakdown and insights
    thread_details_js  = safe_js({ym: v.get("thread_details", {})
                                   for ym, v in metrics_combined.items()})
    insights_combined_js  = safe_js({ym: v.get("monthly_insight")
                                      for ym, v in metrics_combined.items() if v.get("monthly_insight")})
    insights_product_js   = safe_js({ym: v.get("monthly_insight")
                                      for ym, v in metrics_product.items() if v.get("monthly_insight")})
    insights_marketing_js = safe_js({ym: v.get("monthly_insight")
                                      for ym, v in metrics_marketing.items() if v.get("monthly_insight")})

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Creative KPIs</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
:root{{
  --bg:#ffffff;--fg:#09090b;
  --muted:#f4f4f5;--muted-fg:#777;
  --border:#e4e4e7;
  --primary:#18181b;--primary-fg:#fafafa;
  --ring:#777;
  --radius:0.375rem;
}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Inter',system-ui,sans-serif;background:var(--bg);color:var(--fg);padding:52px 64px;font-size:.875rem;line-height:1.5}}
.hdr{{display:flex;align-items:center;justify-content:space-between;margin-bottom:52px}}
h1{{font-size:1.875rem;font-weight:600;letter-spacing:-.025em;color:var(--fg)}}
.vnav{{display:flex;gap:4px}}
.nbtn{{background:none;border:1px solid var(--border);border-radius:var(--radius);cursor:pointer;font-size:.8rem;font-weight:500;color:var(--muted-fg);padding:7px 18px;transition:all .15s}}
.nbtn.active{{background:var(--primary);border-color:var(--primary);color:var(--primary-fg)}}
.nbtn:hover:not(.active){{border-color:var(--ring);color:var(--fg)}}
.sl{{font-size:.75rem;font-weight:600;color:var(--muted-fg);margin-bottom:10px}}
.grp-hdr{{font-size:1.125rem;font-weight:600;color:var(--fg);margin:8px 0 28px;padding-bottom:14px;border-bottom:1.5px solid var(--primary)}}
table{{width:100%;border-collapse:collapse;margin-bottom:52px}}
th,td{{padding:18px 10px;border-bottom:1px solid var(--border);vertical-align:middle}}
.mh{{font-size:.6875rem;font-weight:500;letter-spacing:.05em;text-transform:uppercase;color:var(--muted-fg);text-align:center;min-width:62px}}
.ml{{min-width:200px;padding-right:28px}}
.mn{{font-size:1rem;font-weight:600;color:var(--fg);margin-bottom:4px}}
.mn.click-title{{cursor:pointer;display:inline-block}}
.mn.click-title:hover{{color:#0057d9;text-decoration:underline;text-decoration-style:dotted;text-decoration-color:#0057d9}}
.mt{{font-size:.75rem;color:var(--muted-fg)}}
.mc{{text-align:center}}
.mv{{font-size:.9375rem;font-weight:500;color:var(--fg)}}
.mv.click{{cursor:pointer;text-decoration:underline;text-decoration-style:dotted;text-decoration-color:#d4d4d8;transition:color .15s,text-decoration-color .15s}}
.mv.click:hover{{color:#0057d9;text-decoration-color:#0057d9}}
.empty{{color:#d4d4d8;font-size:.8rem}}
.ov{{display:none;position:fixed;inset:0;background:rgba(0,0,0,.3);z-index:100}}
.ov.on{{display:flex;align-items:flex-end;justify-content:center;padding-bottom:0}}
.pnl{{width:max-content;min-width:520px;max-width:92vw;max-height:45vh;background:var(--bg);border-radius:12px 12px 0 0;box-shadow:0 -4px 24px rgba(0,0,0,.1);display:flex;flex-direction:column;overflow:hidden;transform:translateY(100%);transition:transform .25s ease}}
.ov.on .pnl{{transform:translateY(0)}}
.ph{{padding:16px 24px 12px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:flex-start;flex-shrink:0}}
.pt{{font-size:.9375rem;font-weight:600;color:var(--fg)}}
.ps{{font-size:.75rem;color:var(--muted-fg);margin-top:4px}}
.px{{background:none;border:none;cursor:pointer;color:var(--ring);font-size:1.1rem;padding:0;margin-left:10px;line-height:1;flex-shrink:0}}
.px:hover{{color:var(--fg)}}
.pb{{flex:1;overflow-y:auto;padding:0 28px 28px}}
.dr{{display:flex;justify-content:space-between;align-items:center;padding:13px 0;border-bottom:1px solid var(--border)}}
.dr:last-child{{border:none}}
.dn{{font-size:.875rem;color:#777}}
.dv{{font-size:.875rem;font-weight:600;color:var(--fg)}}
.nd{{color:var(--ring);font-size:.8rem;padding:20px 0;text-align:center}}
.info-section{{margin-bottom:24px}}
.info-label{{font-size:.75rem;font-weight:600;color:var(--muted-fg);margin-bottom:8px}}
.info-text{{font-size:.875rem;color:var(--fg);line-height:1.6}}
.info-formula{{font-size:.875rem;color:var(--fg);background:var(--muted);border-radius:var(--radius);padding:10px 14px;font-family:'JetBrains Mono',monospace;line-height:1.5}}
.info-rules{{list-style:none;padding:0;margin:0}}
.info-rules li{{font-size:.85rem;color:#777;line-height:1.55;padding:5px 0 5px 16px;border-bottom:1px solid var(--border);position:relative}}
.info-rules li:last-child{{border:none}}
.info-rules li::before{{content:"–";position:absolute;left:0;color:var(--ring)}}
.sig{{display:inline;font-size:.8rem;font-weight:600;white-space:nowrap}}
.sig-err{{color:#dc2626}}
.sig-ot{{color:#16a34a}}
.cell-alert{{color:#dc2626!important;font-weight:700}}
.pf{{padding:10px 28px 10px;border-bottom:1px solid var(--border);flex-shrink:0;display:none;gap:8px;align-items:center}}
.pf.on{{display:flex}}
.pf-lbl{{font-size:.75rem;color:var(--muted-fg);margin-right:4px}}
.pf{{display:none;align-items:center;gap:6px}}
.pf.on{{display:flex}}
.gf-sel{{font-size:.75rem;border:1px solid var(--border);border-radius:var(--radius);padding:4px 8px;color:var(--fg);background:var(--bg);cursor:pointer;outline:none}}
.th-row{{padding:12px 0;border-bottom:1px solid var(--border)}}
.th-row:last-child{{border:none}}
.th-meta{{font-size:.8rem;color:var(--muted-fg);margin-top:4px;display:flex;gap:12px;flex-wrap:wrap;align-items:center}}
.th-link{{font-size:.78rem;color:#2563eb;text-decoration:none;margin-left:auto;flex-shrink:0}}
.th-link:hover{{text-decoration:underline}}
.des-block{{margin-bottom:24px;padding-top:16px}}
.des-hdr{{font-size:.8rem;font-weight:700;color:var(--fg);margin-bottom:8px}}
.des-sub{{font-size:.7rem;color:var(--muted-fg);font-weight:400;margin-left:6px}}
.th-table{{width:100%;border-collapse:collapse;font-size:.8rem;table-layout:fixed}}
.th-table th{{font-size:.75rem;font-weight:600;color:var(--muted-fg);padding:14px 8px 4px 0;border-bottom:1px solid var(--border);text-align:left;position:sticky;top:0;background:var(--bg);z-index:1}}
.th-table td{{padding:8px 8px 8px 0;border-bottom:1px solid var(--border);color:#09090b;vertical-align:middle}}
.th-table tr:last-child td{{border:none}}
.th-table .td-type{{font-weight:500;color:var(--fg);max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.th-table .td-name{{font-weight:500;color:#777;white-space:nowrap}}
.th-table .td-num{{text-align:center;color:#777}}
.th-table .td-link{{text-align:right}}
.th-table .td-link a{{color:#2563eb;text-decoration:none;font-size:.8rem}}
.th-table .td-type a{{color:var(--fg);text-decoration:none;font-weight:500}}
.th-table .td-type a:hover{{color:#2563eb;text-decoration:underline}}
.th-row-click:hover td{{background:var(--muted)}}
.ai-why{{font-size:.72rem;color:var(--muted-fg);font-style:italic;margin-top:3px;line-height:1.4}}
.td-ai{{max-width:300px;min-width:160px}}
.td-reason{{max-width:280px;min-width:140px}}
.th-ai,.th-reason{{text-align:left!important}}
.ai-why-col{{font-size:.72rem;color:var(--muted-fg);font-style:italic;line-height:1.4;display:block}}
.ai-empty{{color:#d4d4d8}}
.col-deliv{{width:22%}}.col-sig{{width:11%}}.col-num{{width:6%}}.col-ai{{width:22%}}.col-issue{{width:17%}}
.pnl2{{position:fixed;bottom:0;left:50%;transform:translateX(-50%) translateY(100%);width:max-content;min-width:520px;max-width:92vw;max-height:45vh;background:var(--bg);border-radius:12px 12px 0 0;box-shadow:0 -4px 24px rgba(0,0,0,.1);display:flex;flex-direction:column;overflow:hidden;transition:transform .25s ease;z-index:200}}
.pnl2.open{{transform:translateX(-50%) translateY(0)}}
.pnl.pushed{{transform:translateY(-28px) scale(0.96);transition:transform .25s ease}}
.ov2{{display:none;position:fixed;inset:0;z-index:190;background:rgba(0,0,0,.15)}}
.ov2.on{{display:block}}
.dlg-ov{{display:none;position:fixed;inset:0;z-index:200;background:rgba(0,0,0,.4)}}
.dlg-ov.on{{display:block}}
.dlg{{display:none;position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);width:90vw;max-width:960px;max-height:85vh;background:var(--bg);border-radius:0.5rem;box-shadow:0 8px 30px rgba(0,0,0,.12),0 0 0 1px rgba(0,0,0,.05);z-index:201;flex-direction:column;overflow:hidden}}
.dlg.open{{display:flex}}
.dlg-hdr{{padding:20px 20px 14px;border-bottom:1px solid var(--border);display:flex;align-items:flex-start;justify-content:space-between;flex-shrink:0;gap:12px}}
.dlg-title{{font-size:.9375rem;font-weight:600;color:var(--fg)}}
.dlg-sub{{font-size:.75rem;color:var(--muted-fg);margin-top:2px}}
.dlg-pb{{flex:1;overflow-y:auto;padding:0 20px 20px}}
.sp{{position:fixed;top:0;right:0;height:100vh;width:380px;background:var(--bg);box-shadow:-4px 0 24px rgba(0,0,0,.08);transform:translateX(100%);transition:transform .28s ease;z-index:150;display:flex;flex-direction:column;overflow:hidden}}
.sp.open{{transform:translateX(0)}}
.sp-ov{{display:none;position:fixed;inset:0;z-index:140;background:rgba(0,0,0,.15)}}
.sp-ov.on{{display:block}}
.sp-ph{{padding:20px 20px 14px;border-bottom:1px solid var(--border);display:flex;align-items:flex-start;justify-content:space-between;flex-shrink:0}}
.sp-pb{{flex:1;overflow-y:auto;padding:20px}}
.sp-title{{font-size:.9375rem;font-weight:600;color:var(--fg)}}
.sp-sub{{font-size:.75rem;color:var(--muted-fg);margin-top:2px}}
.sig-table{{width:100%;border-collapse:collapse;font-size:.8rem;margin-top:8px}}
.sig-table th{{font-size:.7rem;font-weight:600;color:var(--muted-fg);padding:8px 6px 4px 0;border-bottom:1px solid var(--border);text-align:left}}
.sig-table td{{padding:8px 6px 8px 0;border-bottom:1px solid var(--border);vertical-align:top}}
.sig-table tr:last-child td{{border-bottom:none}}
.reason-text{{font-size:.72rem;color:#777;line-height:1.5}}
.reason-ds{{color:#2563eb;font-weight:600}}
.dash-grid{{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:20px}}
.dash-card{{background:var(--muted);border-radius:var(--radius);padding:14px 18px}}
.dash-label{{font-size:.7rem;font-weight:600;color:var(--muted-fg);margin-bottom:8px}}
.dash-big{{font-size:1.5rem;font-weight:700;color:var(--fg);line-height:1}}
.dash-sub{{font-size:.75rem;color:var(--muted-fg);margin-top:4px}}
.dash-row{{display:flex;justify-content:space-between;align-items:baseline;padding:5px 0;border-bottom:1px solid var(--border);font-size:.8rem}}
.dash-row:last-child{{border:none}}
.dash-section{{margin-bottom:18px}}
.dash-section-title{{font-size:.7rem;font-weight:600;color:var(--muted-fg);margin-bottom:10px}}
.leg{{margin-top:28px;padding-top:18px;border-top:1px solid var(--border)}}
.leg-title{{font-size:.7rem;font-weight:600;color:var(--muted-fg);margin-bottom:10px}}
.leg-row{{display:flex;gap:8px;align-items:flex-start;margin-bottom:7px;font-size:.78rem;color:#777;line-height:1.45}}
.leg-row .sig{{flex-shrink:0;margin-top:1px}}
.ic-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:16px;margin-bottom:52px}}
.ic{{background:var(--muted);border:1px solid var(--border);border-radius:var(--radius);padding:20px 22px}}
.ic-month{{font-size:.6875rem;font-weight:600;letter-spacing:.05em;text-transform:uppercase;color:var(--muted-fg);margin-bottom:10px}}
.ic-stat{{font-size:1.1rem;font-weight:500;color:var(--fg);margin-bottom:4px}}
.ic-detail{{font-size:.8rem;color:#777;margin-bottom:4px}}
.ic-link{{font-size:.78rem;color:#2563eb;text-decoration:none}}
.ic-link:hover{{text-decoration:underline}}
.grp-ins{{margin-top:40px}}
.ft{{margin-top:48px;font-size:.6875rem;color:var(--muted-fg)}}
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

</div>

<!-- ═══ SEPARATED VIEW ═══ -->
<div id="view-separated" style="display:none">

  <div class="sl">Product Deliverables</div>
  <table>
    <thead><tr><th class="ml"></th>{mh}</tr></thead>
    <tbody>{dr_p}</tbody>
  </table>

  <div class="sl">Time to First Response</div>
  <table>
    <thead><tr><th class="ml"></th>{mh}</tr></thead>
    <tbody>{rr_p}</tbody>
  </table>


  <div class="sl" style="margin-top:20px">Marketing Deliverables</div>
  <table>
    <thead><tr><th class="ml"></th>{mh}</tr></thead>
    <tbody>{dr_m}</tbody>
  </table>

  <div class="sl">Time to First Response</div>
  <table>
    <thead><tr><th class="ml"></th>{mh}</tr></thead>
    <tbody>{rr_m}</tbody>
  </table>


</div>

<div class="ft">Last updated: {upd}</div>

<div class="ov" id="ov" onclick="close_()">
  <div class="pnl" id="pnl" onclick="event.stopPropagation()">
    <div class="ph">
      <div><div class="pt" id="pt"></div><div class="ps" id="ps"></div></div>
      <div style="display:flex;align-items:center;gap:10px;flex-shrink:0">
        <div class="pf" id="pf" style="display:none">
          <span class="pf-lbl">Group by</span>
          <select class="gf-sel" id="gf-sel" onchange="setGroupBy(this.value)">
            <option value="signal">Signal</option>
            <option value="designer">Designer</option>
            <option value="issue">Issue</option>
            <option value="none">Days to Complete</option>
          </select>
        </div>
        <button class="px" onclick="close_()">✕</button>
      </div>
    </div>
    <div class="pb" id="pb"></div>
  </div>
</div>

<div class="ov2" id="ov2" onclick="closePanel2()">
</div>
<div class="pnl2" id="pnl2" onclick="event.stopPropagation()">
  <div class="ph">
    <div><div class="pt" id="pt2"></div><div class="ps" id="ps2"></div></div>
    <button class="px" onclick="closePanel2()">✕</button>
  </div>
  <div class="pb" id="pb2"></div>
</div>

<div class="dlg-ov" id="dlg-ov" onclick="closeDialog()"></div>
<div class="dlg" id="dlg" onclick="event.stopPropagation()">
  <div class="dlg-hdr">
    <div><div class="dlg-title" id="dlg-title"></div><div class="dlg-sub" id="dlg-sub"></div></div>
    <div style="display:flex;align-items:center;gap:10px;flex-shrink:0">
      <div id="dlg-gf" style="display:none;align-items:center;gap:6px">
        <span class="pf-lbl">Group by</span>
        <select class="gf-sel" id="dlg-gf-sel" onchange="setGroupBy(this.value)">
          <option value="signal">Signal</option>
          <option value="designer">Designer</option>
          <option value="issue">Issue</option>
          <option value="none">Days to Complete</option>
        </select>
      </div>
      <button class="px" onclick="closeDialog()">✕</button>
    </div>
  </div>
  <div class="dlg-pb" id="dlg-pb"></div>
</div>

<div class="sp-ov" id="sp-ov" onclick="closeSidePanel()"></div>
<div class="sp" id="sp" onclick="event.stopPropagation()">
  <div class="sp-ph">
    <div><div class="sp-title" id="sp-title"></div><div class="sp-sub" id="sp-sub"></div></div>
    <button class="px" onclick="closeSidePanel()">✕</button>
  </div>
  <div class="sp-pb" id="sp-pb"></div>
</div>

<script>
const METRIC_INFO = {info_js};
const THREAD_DETAILS    = {thread_details_js};
const INSIGHTS_COMBINED = {insights_combined_js};
const INSIGHTS_PRODUCT  = {insights_product_js};
const INSIGHTS_MARKETING= {insights_marketing_js};
const MONTH_KEYS_MAP    = {{"JAN":"01","FEB":"02","MAR":"03","APR":"04","MAY":"05","JUN":"06","JUL":"07","AUG":"08","SEP":"09","OCT":"10","NOV":"11","DEC":"12"}};
let _activeView = 'combined';

function showView(v) {{
  _activeView = v;
  document.getElementById('view-combined').style.display  = v === 'combined'  ? '' : 'none';
  document.getElementById('view-separated').style.display = v === 'separated' ? '' : 'none';
  document.getElementById('btn-combined').classList.toggle('active',  v === 'combined');
  document.getElementById('btn-separated').classList.toggle('active', v === 'separated');
}}

function showInfo(key) {{
  const info = METRIC_INFO[key];
  if (!info) return;
  document.getElementById('pt').textContent = info.label;
  document.getElementById('ps').textContent = 'Definition & Rules';
  document.getElementById('pf').style.display = 'none';
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
}}

function sigClass(s) {{
  return s === 'On track' ? 'sig sig-ot' : 'sig sig-err';
}}

function renderInsights(data, containerId) {{
  const el = document.getElementById(containerId);
  if (!el) return;
  const months = Object.keys(data).sort().reverse().slice(0, 6);
  if (!months.length) {{ el.innerHTML = '<div class="nd">No data yet</div>'; return; }}
  const SIG_ORDER = ['High rework','Slow pickup','Long discussion','Late feedback','On track'];
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

// ── Drill-down state ────────────────────────────────────────────────────────
let _drillEntries = {{}};
let _groupBy = 'signal';
let _drillTarget = 'pb';

function openDialog(title, subtitle, html, showGroupBy) {{
  document.getElementById('dlg-title').textContent = title;
  document.getElementById('dlg-sub').textContent = subtitle;
  document.getElementById('dlg-pb').innerHTML = html;
  document.getElementById('dlg-gf').style.display = showGroupBy ? 'flex' : 'none';
  document.getElementById('dlg-ov').classList.add('on');
  document.getElementById('dlg').classList.add('open');
}}
function closeDialog() {{
  document.getElementById('dlg-ov').classList.remove('on');
  document.getElementById('dlg').classList.remove('open');
  _drillTarget = 'pb';
}}

function setGroupBy(mode) {{
  _groupBy = mode;
  renderDrillContent();
  document.getElementById('pb').scrollTop = 0;
}}

function drillRow(t, showDesigner) {{
  const lbl  = t.deliverable_type || '—';
  const rev  = t.reviewer_wait_bdays != null ? Math.round(t.reviewer_wait_bdays)+'d' : '—';
  const gap  = t.max_gap_bdays     != null ? Math.round(t.max_gap_bdays)+'d'     : '—';
  const days = Math.round(t.task_days)+'d';
  const rc   = t.click_url ? `onclick="window.open('${{t.click_url}}','_blank')" style="cursor:pointer"` : '';
  const sig  = t.signal;
  const isCyc = sig==='High rework', isGap = sig==='Slow pickup',
        isRep = sig==='Long discussion', isMgr = sig==='Late feedback';
  const nameEl = showDesigner
    ? `<div style="font-size:.7rem;color:#71717a;margin-top:2px">${{t.designer}}</div>` : '';
  const aiCell = t.ai_summary ? `<span class="ai-why-col">${{t.ai_summary}}</span>` : '';
  let issueHtml = '';
  if (t.ai_issue) {{
    issueHtml = t.ai_issue.split(',').map(r => r.trim()).filter(Boolean)
      .map(r => `<span class="reason-text">${{r}}</span>`).join('<br>');
  }}
  return `<tr ${{rc}} class="th-row-click">
    <td class="td-type" title="${{lbl}}">${{lbl}}${{nameEl}}</td>
    <td><span class="${{sigClass(sig)}}">${{sig}}</span></td>
    <td class="td-num ${{isCyc?'cell-alert':''}}">${{t.cycle_count}}</td>
    <td class="td-num ${{isRep?'cell-alert':''}}">${{t.reply_count??'—'}}</td>
    <td class="td-num ${{isMgr?'cell-alert':''}}">${{rev}}</td>
    <td class="td-num ${{isGap?'cell-alert':''}}">${{gap}}</td>
    <td class="td-num">${{days}}</td>
    <td class="td-ai">${{aiCell}}</td>
    <td class="td-reason">${{issueHtml}}</td>
  </tr>`;
}}

function drillHead(showDesigner) {{
  return `<colgroup>
    ${{showDesigner ? '<col class="col-deliv">' : '<col style="width:28%">'}}
    <col class="col-sig"><col class="col-num"><col class="col-num"><col class="col-num"><col class="col-num"><col class="col-num">
    <col class="col-ai"><col class="col-issue">
  </colgroup>
  <thead><tr>
    <th>Deliverable</th><th>Signal</th>
    <th style="text-align:center">Cycles</th><th style="text-align:center">Replies</th>
    <th style="text-align:center">Feedback</th><th style="text-align:center">Gap</th>
    <th style="text-align:center">Days</th><th class="th-ai">AI Summary</th>
    <th class="th-reason">Issue</th>
  </tr></thead>`;
}}

function drillLegend() {{
  return `<div class="leg">
    <div class="leg-title">Signals — highest raw value wins when multiple apply</div>
    <div class="leg-row"><span class="sig sig-err">High rework</span>7+ revision rounds after first submission</div>
    <div class="leg-row"><span class="sig sig-err">Slow pickup</span>Longest gap ≥5 working days (excl. weekends &amp; US holidays)</div>
    <div class="leg-row"><span class="sig sig-err">Late feedback</span>Reviewer avg &gt;2 days to respond</div>
    <div class="leg-row"><span class="sig sig-err">Long discussion</span>8+ replies with ≤1 cycle</div>
    <div class="leg-row"><span class="sig sig-ot">On track</span>No issues detected</div>
    <div class="leg-title" style="margin-top:14px">Columns</div>
    <div class="leg-row"><strong>Cycles</strong> — Extra "For review" / "For feedback" rounds after first</div>
    <div class="leg-row"><strong>Replies</strong> — Discussion messages attributed to this deliverable</div>
    <div class="leg-row"><strong>Feedback</strong> — Avg business days for tagged reviewer to respond (avg across rounds)</div>
    <div class="leg-row"><strong>Gap</strong> — Longest silent stretch between any two consecutive messages</div>
    <div class="leg-row"><strong>Days</strong> — Total business days from first submission to last message</div>
    <div class="leg-row"><strong>AI Summary</strong> — One-line root cause from Gemini, specific to thread content</div>
    <div class="leg-row"><strong>Issue</strong> — Root-cause label from Gemini</div>
  </div>`;
}}

function renderDrillContent() {{
  const entries = _drillEntries;
  const flat = [];
  for (const [name, threads] of Object.entries(entries))
    for (const t of threads) flat.push({{...t, designer: name, click_url: t.slack_url}});

  let html = '';
  if (_groupBy === 'none') {{
    flat.sort((a,b) => b.task_days - a.task_days);
    html = `<table class="th-table">${{drillHead(true)}}<tbody>${{flat.map(t=>drillRow(t,true)).join('')}}</tbody></table>`;

  }} else if (_groupBy === 'designer') {{
    const byDes = {{}};
    for (const t of flat) {{ byDes[t.designer]=byDes[t.designer]||[]; byDes[t.designer].push(t); }}
    html = Object.entries(byDes)
      .sort((a,b)=>{{
        const f=x=>x.reduce((s,t)=>s+t.task_days,0)/x.length;
        return f(b[1])-f(a[1]);
      }})
      .map(([name,threads])=>{{
        const avg=Math.round(threads.reduce((s,t)=>s+t.task_days,0)/threads.length), n=threads.length;
        threads.sort((a,b)=>b.task_days-a.task_days);
        return `<div class="des-block">
          <div class="des-hdr">${{name}}<span class="des-sub">${{n}} deliverable${{n!==1?'s':''}} · AVG ${{avg}}D</span></div>
          <table class="th-table">${{drillHead(false)}}<tbody>${{threads.map(t=>drillRow(t,false)).join('')}}</tbody></table>
        </div>`;
      }}).join('');

  }} else if (_groupBy === 'signal') {{
    const SIG_ORDER = ['High rework','Slow pickup','Long discussion','Late feedback','On track'];
    const bySig = {{}};
    for (const t of flat) {{ bySig[t.signal]=bySig[t.signal]||[]; bySig[t.signal].push(t); }}
    html = SIG_ORDER.filter(s=>bySig[s]).map(sig=>{{
      const threads=bySig[sig]; threads.sort((a,b)=>b.task_days-a.task_days);
      return `<div class="des-block">
        <div class="des-hdr"><span class="${{sigClass(sig)}}">${{sig}}</span><span class="des-sub">${{threads.length}} deliverable${{threads.length!==1?'s':''}}</span></div>
        <table class="th-table">${{drillHead(true)}}<tbody>${{threads.map(t=>drillRow(t,true)).join('')}}</tbody></table>
      </div>`;
    }}).join('');
  }} else if (_groupBy === 'issue') {{
    const byIssue = {{}};
    for (const t of flat) {{
      const labels = (t.ai_issue||'On track').split(',').map(s=>s.trim()).filter(Boolean);
      labels.forEach(lbl => {{ byIssue[lbl]=byIssue[lbl]||[]; byIssue[lbl].push(t); }});
    }}
    html = Object.entries(byIssue)
      .sort((a,b) => b[1].length - a[1].length)
      .map(([lbl, threads]) => {{
        threads.sort((a,b) => b.task_days - a.task_days);
        return `<div class="des-block">
          <div class="des-hdr">${{lbl}}<span class="des-sub">${{threads.length}} deliverable${{threads.length!==1?'s':''}}</span></div>
          <table class="th-table">${{drillHead(true)}}<tbody>${{threads.map(t=>drillRow(t,true)).join('')}}</tbody></table>
        </div>`;
      }}).join('');
  }}
  document.getElementById(_drillTarget).innerHTML = html + drillLegend();
}}

function buildThreadTable(threads) {{
  if (!threads.length) return '<div class="nd">No deliverables</div>';
  const sorted = [...threads].sort((a,b) => b.task_days - a.task_days);
  return `<table class="th-table">${{drillHead(true)}}<tbody>${{sorted.map(t=>drillRow(t,true)).join('')}}</tbody></table>${{drillLegend()}}`;
}}

function openPanel2Filter(ym, filterType, el) {{
  const filterValue = el.dataset.filter;
  const threadDetails = THREAD_DETAILS[ym] || {{}};
  const allThreads = Object.entries(threadDetails).flatMap(([name, ts]) =>
    ts.map(t => ({{...t, designer: name, click_url: t.slack_url}})));
  let filtered;
  if (filterType === 'signal') {{
    filtered = allThreads.filter(t => t.signal === filterValue);
  }} else {{
    filtered = allThreads.filter(t =>
      (t.ai_issue||'').split(',').map(s=>s.trim()).includes(filterValue));
  }}
  const count = filtered.length;
  const subtitle = count + ' deliverable' + (count !== 1 ? 's' : '');
  openDialog(filterValue, subtitle, buildThreadTable(filtered), false);
}}

function showDrill(el) {{
  const metricKey = el.dataset.metricKey || '';
  const s   = el.dataset.suffix || '';
  const mon = el.dataset.month;
  const yr  = el.dataset.year;
  const ym  = yr + '-' + (MONTH_KEYS_MAP[mon]||'');

  document.getElementById('pt').textContent = el.dataset.metric;
  document.getElementById('ps').textContent = mon + ' ' + yr;

  if (metricKey === 'task_days_per_d') {{
    _drillEntries = THREAD_DETAILS[ym] || {{}};
    _groupBy = 'signal';
    _drillTarget = 'dlg-pb';
    const dlgSel = document.getElementById('dlg-gf-sel');
    if (dlgSel) dlgSel.value = 'signal';
    if (!Object.keys(_drillEntries).length) {{
      openDialog('Avg Days to Complete', mon+' '+yr, '<div class="nd">No thread data</div>', false);
      return;
    }}
    openDialog('Avg Days to Complete', mon+' '+yr, '', true);
    renderDrillContent();
    return;
  }}

  document.getElementById('ov').classList.add('on');

  if (metricKey === 'num_ds') {{
    const threads  = THREAD_DETAILS[ym] || {{}};
    const insight  = (INSIGHTS_COMBINED[ym] || INSIGHTS_PRODUCT[ym] || INSIGHTS_MARKETING[ym] || {{}});
    const SIG_OFF  = ['High rework','Slow pickup','Long discussion','Late feedback'];

    const allThreads = Object.entries(threads).flatMap(([name, ts]) => ts.map(t => ({{...t, designer: name, click_url: t.slack_url}})));
    const total      = allThreads.length || insight.total || 0;
    const flaggedCnt = insight.flagged_count || 0;
    const onTrackCnt = total - flaggedCnt;
    const offPct     = total ? Math.round(flaggedCnt/total*100) : 0;
    const onPct      = 100 - offPct;
    const sigBreak   = insight.signal_breakdown || {{}};
    const avgAll     = insight.avg_days_all;
    const avgFlag    = insight.avg_days_flagged;
    const avgOk      = insight.avg_days_ontrack;
    const topIssues  = insight.top_issues || [];

    const fmtAvg = avg => avg != null ? avg+'d to complete' : '—';

    // Signal rows — off-track only, all clickable
    const sigRows = SIG_OFF.filter(s => sigBreak[s]).map(s =>
      `<div class="dash-row" style="cursor:pointer" onclick="openPanel2Filter('${{ym}}','signal',this)" data-filter="${{s}}">` +
      `<span class="sig sig-err">${{s}}</span><span style="font-weight:600">${{sigBreak[s]}}</span></div>`
    ).join('');

    // Top issues — clickable, with type count in header
    const issueTypeCount = topIssues.length;
    const issueRows = issueTypeCount
      ? topIssues.map(([r,c]) =>
          `<div class="dash-row" style="cursor:pointer" onclick="openPanel2Filter('${{ym}}','issue',this)" data-filter="${{r.replace(/"/g,'&quot;')}}">` +
          `<span style="color:#09090b">${{r}}</span><span style="font-weight:600;color:#09090b">${{c}}</span></div>`
        ).join('')
      : '<div style="font-size:.78rem;color:#71717a">Run workflow to generate AI issues</div>';

    const html = `
      <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-bottom:16px">
        <div style="background:#f4f4f5;border-radius:6px;padding:10px 8px">
          <div style="font-size:.75rem;color:#777;margin-bottom:4px">Total</div>
          <div style="font-size:1.5rem;font-weight:700;color:#09090b;line-height:1">${{total}}</div>
          <div style="font-size:.68rem;color:#777;margin-top:4px">${{fmtAvg(avgAll)}}</div>
        </div>
        <div style="background:#f0faf2;border-radius:6px;padding:10px 8px;border:1px solid #c3e6cb">
          <div style="font-size:.75rem;color:#777;margin-bottom:4px">On track</div>
          <div style="font-size:1.5rem;font-weight:700;color:#16a34a;line-height:1">${{onTrackCnt}} <span style="font-size:.95rem;font-weight:500;color:#16a34a">(${{onPct}}%)</span></div>
          <div style="font-size:.68rem;color:#777;margin-top:4px">${{fmtAvg(avgOk)}}</div>
        </div>
        <div style="background:#fff3f3;border-radius:6px;padding:10px 8px;border:1px solid #f5c6c6;position:relative">
          <div style="font-size:.75rem;color:#777;margin-bottom:4px">Off track</div>
          <div style="font-size:1.5rem;font-weight:700;color:#dc2626;line-height:1">${{flaggedCnt}} <span style="font-size:.95rem;font-weight:500;color:#dc2626">(${{offPct}}%)</span></div>
          <div style="font-size:.68rem;color:#777;margin-top:4px">${{fmtAvg(avgFlag)}}</div>
          ${{sigRows ? `<div style="position:absolute;bottom:-10px;left:50%;transform:translateX(-50%);width:0;height:0;border-left:8px solid transparent;border-right:8px solid transparent;border-top:10px solid #f5c6c6"></div>` : ''}}
        </div>
      </div>
      ${{sigRows ? `<div style="margin-bottom:20px;border-top:2px solid #f5c6c6;padding-top:10px">
        <div style="font-size:.75rem;font-weight:600;color:#777;margin-bottom:6px">Off-track breakdown</div>
        ${{sigRows}}
      </div>` : ''}}
      <div style="padding-top:4px;border-top:1px solid #e4e4e7;margin-top:4px">
        <div style="font-size:.75rem;font-weight:600;color:#777;margin-bottom:6px;padding-top:12px">
          Top issues${{issueTypeCount ? ' · '+issueTypeCount+' type'+(issueTypeCount!==1?'s':'') : ''}}
        </div>
        ${{issueRows}}
      </div>`;
    openSidePanel(el.dataset.metric, mon+' '+yr, html);
    document.getElementById('ov').classList.remove('on');
    return;
  }}

  document.getElementById('pf').style.display = 'none';

  if (metricKey === 'ds_per_person') {{
    // Build per-designer signal breakdown table in side panel
    const SIG_ORDER = ['High rework','Slow pickup','Long discussion','Late feedback','On track'];
    const threads = THREAD_DETAILS[ym] || {{}};
    const rows = Object.entries(threads).map(([name, ts]) => {{
      const counts = {{}};
      ts.forEach(t => {{ counts[t.signal] = (counts[t.signal]||0)+1; }});
      const total = ts.length;
      const flagged = total - (counts['On track']||0);
      return {{name, counts, total, flagged}};
    }}).sort((a,b) => b.flagged - a.flagged || b.total - a.total);
    if (!rows.length) {{
      openSidePanel(el.dataset.metric, mon+' '+yr, '<div class="nd">No data</div>');
      document.getElementById('ov').classList.remove('on');
      return;
    }}
    const sigCols = SIG_ORDER;
    const thStyle = `text-align:center;border-left:1px solid #eee`;
    const tdStyle = `text-align:center;border-left:1px solid #eee`;
    const thead = `<tr><th>Designer</th><th style="text-align:center">Total</th>${{sigCols.map(s=>`<th style="${{thStyle}}">${{s}}</th>`).join('')}}</tr>`;
    const tbody = rows.map(r => {{
      const sigCells = sigCols.map(s => {{
        const v = r.counts[s]||0;
        const cls = s==='On track'?'sig-ot':'sig-err';
        return `<td style="${{tdStyle}}">${{v ? `<span class="sig ${{cls}}">${{v}}</span>` : ''}}</td>`;
      }}).join('');
      return `<tr><td style="font-weight:500">${{r.name}}</td><td style="text-align:center;font-weight:600">${{r.total}}</td>${{sigCells}}</tr>`;
    }}).join('');
    const html = `<table class="sig-table"><thead>${{thead}}</thead><tbody>${{tbody}}</tbody></table>`;
    openSidePanel(el.dataset.metric, mon+' '+yr, html);
    document.getElementById('ov').classList.remove('on');
    return;
  }}

  // Generic drill — show breakdown list in side panel
  let d = {{}};
  try {{ d = JSON.parse(el.dataset.drill); }} catch(e) {{}}
  const entries = Object.entries(d);
  const html = entries.length
    ? entries.map(([n,v]) => `<div class="dr"><span class="dn">${{n}}</span><span class="dv">${{v}}${{s}}</span></div>`).join('')
    : '<div class="nd">No breakdown available</div>';
  openSidePanel(el.dataset.metric, mon+' '+yr, html);
  document.getElementById('ov').classList.remove('on');
}}

function close_() {{
  document.getElementById('ov').classList.remove('on');
  document.getElementById('pf').style.display = 'none';
}}
function openPanel2(title, subtitle, html) {{
  document.getElementById('pt2').textContent = title;
  document.getElementById('ps2').textContent = subtitle;
  document.getElementById('pb2').innerHTML = html;
  document.getElementById('ov2').classList.add('on');
  document.getElementById('pnl2').classList.add('open');
  document.getElementById('pnl').classList.add('pushed');
}}
function closePanel2() {{
  document.getElementById('ov2').classList.remove('on');
  document.getElementById('pnl2').classList.remove('open');
  document.getElementById('pnl').classList.remove('pushed');
}}
function openSidePanel(title, subtitle, html) {{
  document.getElementById('sp-title').textContent = title;
  document.getElementById('sp-sub').textContent   = subtitle;
  document.getElementById('sp-pb').innerHTML      = html;
  document.getElementById('sp-ov').classList.add('on');
  document.getElementById('sp').classList.add('open');
}}
function closeSidePanel() {{
  document.getElementById('sp-ov').classList.remove('on');
  document.getElementById('sp').classList.remove('open');
}}
document.addEventListener('keydown', e => {{ if (e.key === 'Escape') {{ closeSidePanel(); closePanel2(); close_(); }} }});
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

    # Build per-user business-hours map from Slack profile tz + overrides
    reviewer_hours = {}
    for uid, uinfo in users.items():
        dname = uinfo.get("display_name", "")
        if dname in REVIEWER_HOURS_OVERRIDE:
            reviewer_hours[uid] = REVIEWER_HOURS_OVERRIDE[dname]
        else:
            reviewer_hours[uid] = (BH_START, BH_END)

    print(f"\n[2/5] Finding managers: {MANAGER_NAMES}")
    managers = find_managers(users, MANAGER_NAMES)
    if not managers:
        print("  WARNING: No managers found — response time tracking disabled")

    start_dt, end_dt = get_date_range()

    print("\n[3/5] Loading existing data.json...")
    existing_c, existing_p, existing_m, ai_summaries = {}, {}, {}, {}
    if os.path.exists("data.json"):
        try:
            saved = json.load(open("data.json"))
            existing_c   = saved.get("metrics",           {})
            existing_p   = saved.get("metrics_product",   {})
            existing_m   = saved.get("metrics_marketing", {})
            ai_summaries = saved.get("ai_summaries",      {})
            print(f"  Found combined data for: {sorted(existing_c.keys())}")
            print(f"  Cached AI summaries: {len(ai_summaries)}")
        except Exception:
            print("  Could not parse data.json — starting fresh")
    else:
        print("  No data.json found — starting fresh")

    if GEMINI_API_KEY:
        print("  Gemini AI summaries: enabled")
    else:
        print("  Gemini AI summaries: disabled (set GEMINI_API_KEY secret to enable)")

    print("\n[4/5] Fetching and processing Slack data...")
    month_data = process_slack(client, CHANNEL_ID, users, managers, start_dt, end_dt,
                               ai_summaries=ai_summaries, reviewer_hours=reviewer_hours)

    new_c = compute_metrics(month_data, managers, users=users)
    new_p = compute_metrics(month_data, managers, users=users, roster=PRODUCT_DESIGNERS)
    new_m = compute_metrics(month_data, managers, users=users, roster=MARKETING_DESIGNERS)

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
            "ai_summaries":      ai_summaries,
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
