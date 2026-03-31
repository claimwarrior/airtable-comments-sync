#!/usr/bin/env python3
"""
Sync ALL Airtable comments (Claims + CRM tables) into Supabase "Claim Comments".

Safe to run repeatedly — deduplicates against existing comments.
Creates new Team Members for unknown comment authors.

Usage:
  # Dry run (no writes):
  SUPABASE_SERVICE_KEY='eyJ...' python3 sync_airtable_comments.py

  # Actually write:
  DRY_RUN=false SUPABASE_SERVICE_KEY='eyJ...' python3 sync_airtable_comments.py
"""

import logging
import os
import sys
import time
import requests
from datetime import datetime, timezone

# ──────────────────────────────────────────────
# Logging (flushes immediately for Render)
# ──────────────────────────────────────────────
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("sync")

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
AIRTABLE_TOKEN = os.environ.get("AIRTABLE_TOKEN", "")
AIRTABLE_BASE_ID = "appsLyOvRhV9VTXMY"

# Two Airtable tables to sync
TABLES = [
    {
        "name": "Claims",
        "table_id": "tblfMC5GgoplRez3F",
        "field": "Claim Number",
        "map_by": "airtable_record_id",
    },
    {
        "name": "CRM",
        "table_id": "tbljCMaiYjh6kR0kW",
        "field": "Claim",
        "map_by": "claim_number",
    },
]

# Supabase production
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://upbbqaqnegncoetxuhwk.supabase.co")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

REQUEST_DELAY = 0.2
DRY_RUN = os.environ.get("DRY_RUN", "true").lower() == "true"
BATCH_SIZE = 50


def normalize_timestamp(ts):
    if not ts:
        return ""
    try:
        if "T" in ts:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(ts.replace("+00", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S+00")
    except Exception:
        return ts


# ──────────────────────────────────────────────
# Airtable helpers
# ──────────────────────────────────────────────
def airtable_headers():
    return {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}


def fetch_all_records(table_id, field_name):
    records = []
    offset = None
    page = 0

    while True:
        page += 1
        params = {"pageSize": 100, "fields[]": field_name}
        if offset:
            params["offset"] = offset

        resp = requests.get(
            f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table_id}",
            headers=airtable_headers(),
            params=params,
        )
        resp.raise_for_status()
        data = resp.json()

        batch = data.get("records", [])
        records.extend(batch)
        log.info(f"  Page {page}: {len(batch)} records (total: {len(records)})")

        offset = data.get("offset")
        if not offset:
            break
        time.sleep(REQUEST_DELAY)

    return records


def fetch_comments(table_id, record_id, max_retries=3):
    comments = []
    offset = None

    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset

        for attempt in range(max_retries):
            try:
                resp = requests.get(
                    f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table_id}/{record_id}/comments",
                    headers=airtable_headers(),
                    params=params,
                    timeout=30,
                )
                if resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", 30))
                    log.warning(f"Rate limited, waiting {wait}s...")
                    time.sleep(wait)
                    continue
                if resp.status_code >= 500:
                    wait = 2 ** (attempt + 1)
                    log.warning(f"Server error {resp.status_code}, retry in {wait}s...")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                break
            except requests.exceptions.Timeout:
                wait = 2 ** (attempt + 1)
                log.warning(f"Timeout on {record_id}, retry in {wait}s...")
                time.sleep(wait)
        else:
            log.warning(f"Skipping {record_id} after {max_retries} retries")
            return comments

        data = resp.json()
        batch = data.get("comments", [])
        comments.extend(batch)

        offset = data.get("offset")
        if not offset:
            break
        time.sleep(REQUEST_DELAY)

    return comments


# ──────────────────────────────────────────────
# Supabase helpers
# ──────────────────────────────────────────────
def supabase_headers():
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }


def supabase_get(table, params=None):
    all_rows = []
    offset = 0
    page_size = 1000

    while True:
        headers = supabase_headers()
        headers["Range"] = f"{offset}-{offset + page_size - 1}"

        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers=headers,
            params=params or {},
        )
        resp.raise_for_status()
        rows = resp.json()
        all_rows.extend(rows)

        if len(rows) < page_size:
            break
        offset += page_size

    return all_rows


def supabase_post(table, rows):
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=supabase_headers(),
        json=rows,
    )
    if resp.status_code >= 400:
        log.error(f"Inserting into {table}: {resp.status_code} {resp.text}")
        return None
    return resp.json()


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
def main():
    if not AIRTABLE_TOKEN:
        log.error("AIRTABLE_TOKEN not set")
        sys.exit(1)

    if not SUPABASE_SERVICE_KEY:
        log.error("SUPABASE_SERVICE_KEY not set")
        sys.exit(1)

    if DRY_RUN:
        log.info("=" * 50)
        log.info("DRY RUN MODE - no writes to Supabase")
        log.info("=" * 50)

    # ── Step 1: Load Supabase claim mappings ──
    log.info("[1/6] Loading Supabase claim mappings...")

    sb_claims_at = supabase_get(
        "Claims",
        {"select": "whalesync_postgres_id,airtable_record_id", "airtable_record_id": "not.is.null"},
    )
    claim_by_airtable_id = {}
    for c in sb_claims_at:
        arid = c.get("airtable_record_id")
        if arid:
            claim_by_airtable_id[arid] = c["whalesync_postgres_id"]

    claim_by_number = {}
    sb_claims_cn = supabase_get(
        "Claims",
        {"select": "whalesync_postgres_id,claim_number", "claim_number": "not.is.null"},
    )
    for c in sb_claims_cn:
        cn = c.get("claim_number")
        if cn:
            claim_by_number[str(cn).strip()] = c["whalesync_postgres_id"]

    log.info(f"  Claims by airtable_record_id: {len(claim_by_airtable_id)}")
    log.info(f"  Claims by Claim Number: {len(claim_by_number)}")

    # ── Step 2: Load existing team members ──
    log.info("[2/6] Loading existing team members...")
    sb_team = supabase_get("Team Members", {"select": "whalesync_postgres_id,Name,Email"})
    team_by_email = {}
    team_by_name = {}
    for tm in sb_team:
        email = (tm.get("Email") or "").strip().lower()
        name = (tm.get("Name") or "").strip().lower()
        if email:
            team_by_email[email] = tm["whalesync_postgres_id"]
        if name:
            team_by_name[name] = tm["whalesync_postgres_id"]
    log.info(f"  Team members: {len(sb_team)} ({len(team_by_email)} with email)")

    EMAIL_ALIASES = {
        "claims@myclaimwarriors.com": "sjerome@spjadjusting.com",
        "jody@myclaimwarriors.com": "jodymisko@gmail.com",
    }
    for alias, primary in EMAIL_ALIASES.items():
        if primary.lower() in team_by_email and alias.lower() not in team_by_email:
            team_by_email[alias.lower()] = team_by_email[primary.lower()]
            log.info(f"  + Alias: {alias} -> {primary}")

    # ── Step 3: Fetch comments from ALL Airtable tables ──
    log.info("[3/6] Fetching comments from Airtable...")
    all_comments = []
    total_records = 0
    records_with_comments = 0

    for table in TABLES:
        log.info(f"  --- {table['name']} table (ID: {table['table_id']}) ---")
        records = fetch_all_records(table["table_id"], table["field"])
        total_records += len(records)
        log.info(f"  Total records: {len(records)}")

        for i, rec in enumerate(records):
            record_id = rec["id"]
            field_value = str(rec.get("fields", {}).get(table["field"], "")).strip()

            if table["map_by"] == "airtable_record_id":
                if record_id not in claim_by_airtable_id:
                    continue
            else:
                if not field_value or field_value not in claim_by_number:
                    continue

            comments = fetch_comments(table["table_id"], record_id)
            if comments:
                records_with_comments += 1
                for c in comments:
                    all_comments.append((table["name"], table["map_by"], record_id, field_value, c))

            if (i + 1) % 50 == 0 or i == len(records) - 1:
                log.info(f"  Processed {i + 1}/{len(records)} - {len(all_comments)} comments so far")

            time.sleep(REQUEST_DELAY)

    log.info(f"  Total comments found: {len(all_comments)} across {records_with_comments} records")

    if not all_comments:
        log.info("No comments to migrate. Done!")
        return

    # ── Step 4: Resolve authors ──
    log.info("[4/6] Resolving comment authors...")
    unique_authors = {}
    for _, _, _, _, comment in all_comments:
        author = comment.get("author", {})
        author_id = author.get("id", "")
        if author_id and author_id not in unique_authors:
            unique_authors[author_id] = {
                "email": (author.get("email") or "").strip(),
                "name": (author.get("name") or "").strip(),
            }

    log.info(f"  Unique authors: {len(unique_authors)}")

    author_to_tm = {}
    new_team_members = []

    for author_id, info in unique_authors.items():
        email_lower = info["email"].lower()
        name_lower = info["name"].lower()

        if email_lower and email_lower in team_by_email:
            author_to_tm[author_id] = team_by_email[email_lower]
            log.info(f"  Matched by email: {info['name']} <{info['email']}>")
        elif name_lower and name_lower in team_by_name:
            author_to_tm[author_id] = team_by_name[name_lower]
            log.info(f"  Matched by name: {info['name']}")
        else:
            log.info(f"  + New team member needed: {info['name']} <{info['email']}>")
            new_team_members.append((author_id, info))

    if new_team_members and not DRY_RUN:
        log.info(f"  Creating {len(new_team_members)} new team members...")
        for author_id, info in new_team_members:
            row = {"Name": info["name"] or None, "Email": info["email"] or None}
            result = supabase_post("Team Members", row)
            if result and isinstance(result, list) and len(result) > 0:
                tm_id = result[0]["whalesync_postgres_id"]
                author_to_tm[author_id] = tm_id
                if info["email"]:
                    team_by_email[info["email"].lower()] = tm_id
                if info["name"]:
                    team_by_name[info["name"].lower()] = tm_id
                log.info(f"  Created: {info['name']} -> {tm_id}")
            else:
                log.error(f"  FAILED to create team member for {info['name']}")
    elif new_team_members and DRY_RUN:
        log.info(f"  [DRY RUN] Would create {len(new_team_members)} new team members")
        for author_id, info in new_team_members:
            author_to_tm[author_id] = f"NEW-{author_id}"

    # ── Step 5: Dedup against existing comments ──
    log.info("[5/6] Loading existing comments for dedup...")
    existing_comments = supabase_get("Claim Comments", {"select": "claim_id,created_at,comment"})
    existing_keys = set()
    for ec in existing_comments:
        key = (ec["claim_id"], normalize_timestamp(ec["created_at"]), (ec["comment"] or "")[:100])
        existing_keys.add(key)
    log.info(f"  Existing comments: {len(existing_comments)} ({len(existing_keys)} unique dedup keys)")

    # ── Step 6: Insert new comments ──
    log.info("[6/6] Inserting new comments...")

    inserted = 0
    skipped_no_claim = 0
    skipped_no_author = 0
    skipped_duplicate = 0
    batch = []

    for source, map_by, record_id, field_value, comment in all_comments:
        if map_by == "airtable_record_id":
            claim_id = claim_by_airtable_id.get(record_id)
        else:
            claim_id = claim_by_number.get(field_value) if field_value else None

        if not claim_id:
            skipped_no_claim += 1
            continue

        author_id = comment.get("author", {}).get("id", "")
        team_member_id = author_to_tm.get(author_id)
        if not team_member_id:
            skipped_no_author += 1
            continue

        dedup_key = (claim_id, normalize_timestamp(comment["createdTime"]), (comment["text"] or "")[:100])
        if dedup_key in existing_keys:
            skipped_duplicate += 1
            continue

        batch.append({
            "claim_id": claim_id,
            "team_member_id": team_member_id,
            "created_at": comment["createdTime"],
            "comment": comment["text"],
            "mentioned_team_member_ids": [],
            "edited": comment.get("lastUpdatedTime") is not None,
            "deleted": False,
            "replaced_by_id": None,
        })

        if len(batch) >= BATCH_SIZE:
            if not DRY_RUN:
                result = supabase_post("Claim Comments", batch)
                if result:
                    inserted += len(batch)
                    log.info(f"  Inserted batch: {inserted} total")
            else:
                inserted += len(batch)
            batch = []

    if batch:
        if not DRY_RUN:
            result = supabase_post("Claim Comments", batch)
            if result:
                inserted += len(batch)
        else:
            inserted += len(batch)

    # ── Summary ──
    log.info("=" * 50)
    log.info("SYNC SUMMARY")
    log.info("=" * 50)
    log.info(f"  Airtable records scanned:  {total_records}")
    log.info(f"  Records with comments:     {records_with_comments}")
    log.info(f"  Total comments found:      {len(all_comments)}")
    log.info(f"  Comments inserted:         {inserted}")
    log.info(f"  Skipped (no claim match):  {skipped_no_claim}")
    log.info(f"  Skipped (no author match): {skipped_no_author}")
    log.info(f"  Skipped (duplicate):       {skipped_duplicate}")
    log.info(f"  New team members created:  {len(new_team_members)}")
    if DRY_RUN:
        log.info("DRY RUN - nothing was written to Supabase")
    log.info("=" * 50)


if __name__ == "__main__":
    main()
