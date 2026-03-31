#!/usr/bin/env python3
"""
Sync ALL Airtable comments (Claims + CRM tables) into Supabase "Claim Comments".

Safe to run repeatedly — deduplicates against existing comments.
Creates new Team Members for unknown comment authors.

Usage:
  # Dry run (no writes):
  SUPABASE_SERVICE_KEY='eyJ...' python3 scripts/sync_airtable_comments.py

  # Actually write:
  DRY_RUN=false SUPABASE_SERVICE_KEY='eyJ...' python3 scripts/sync_airtable_comments.py
"""

import os
import sys
import time
import requests
from datetime import datetime, timezone

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
        "field": "Claim Number",       # Field to fetch from Airtable
        "map_by": "airtable_record_id", # How to map to Supabase claims
    },
    {
        "name": "CRM",
        "table_id": "tbljCMaiYjh6kR0kW",
        "field": "Claim",               # CRM uses "Claim" field (contains claim number)
        "map_by": "claim_number",        # Map by claim number, not airtable_record_id
    },
]

# Supabase production
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://upbbqaqnegncoetxuhwk.supabase.co")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

# Rate limiting
REQUEST_DELAY = 0.2  # 5 req/sec

# Dry run mode
DRY_RUN = os.environ.get("DRY_RUN", "true").lower() == "true"

BATCH_SIZE = 50


def normalize_timestamp(ts):
    """Normalize a timestamp string to 'YYYY-MM-DD HH:MM:SS+00' for consistent dedup comparison."""
    if not ts:
        return ""
    try:
        # Handle Airtable format: 2024-09-09T23:26:41.000Z
        if "T" in ts:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        else:
            # Handle Supabase format: 2024-09-09 23:26:41+00
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
    """Fetch all records from an Airtable table (paginated)."""
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
        print(f"    Page {page}: {len(batch)} records (total: {len(records)})")

        offset = data.get("offset")
        if not offset:
            break
        time.sleep(REQUEST_DELAY)

    return records


def fetch_comments(table_id, record_id, max_retries=3):
    """Fetch all comments for a single Airtable record (paginated, with retries)."""
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
                    print(f"      Rate limited, waiting {wait}s...")
                    time.sleep(wait)
                    continue
                if resp.status_code >= 500:
                    wait = 2 ** (attempt + 1)
                    print(f"      Server error {resp.status_code}, retry in {wait}s...")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                break
            except requests.exceptions.Timeout:
                wait = 2 ** (attempt + 1)
                print(f"      Timeout on {record_id}, retry in {wait}s...")
                time.sleep(wait)
        else:
            print(f"      WARN: Skipping {record_id} after {max_retries} retries")
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
    """GET from Supabase REST API with pagination."""
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
    """INSERT rows into Supabase table."""
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=supabase_headers(),
        json=rows,
    )
    if resp.status_code >= 400:
        print(f"    ERROR inserting into {table}: {resp.status_code} {resp.text}")
        return None
    return resp.json()


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
def main():
    if not SUPABASE_SERVICE_KEY:
        print("ERROR: Set SUPABASE_SERVICE_KEY environment variable.")
        print("  export SUPABASE_SERVICE_KEY='eyJ...'")
        sys.exit(1)

    if DRY_RUN:
        print("=" * 60)
        print("  DRY RUN MODE — no writes to Supabase")
        print("  Set DRY_RUN=false to actually migrate")
        print("=" * 60)

    # ── Step 1: Load Supabase claim mappings ──
    print("\n[1/6] Loading Supabase claim mappings...")

    # Query 1: Claims with airtable_record_id (for Claims table mapping)
    sb_claims_at = supabase_get(
        "Claims",
        {"select": "whalesync_postgres_id,airtable_record_id", "airtable_record_id": "not.is.null"},
    )
    claim_by_airtable_id = {}
    for c in sb_claims_at:
        arid = c.get("airtable_record_id")
        if arid:
            claim_by_airtable_id[arid] = c["whalesync_postgres_id"]

    # Query 2: All claims with claim_number (for CRM table mapping)
    claim_by_number = {}
    sb_claims_cn = supabase_get(
        "Claims",
        {"select": "whalesync_postgres_id,claim_number", "claim_number": "not.is.null"},
    )
    for c in sb_claims_cn:
        cn = c.get("claim_number")
        if cn:
            claim_by_number[str(cn).strip()] = c["whalesync_postgres_id"]

    print(f"  Claims by airtable_record_id: {len(claim_by_airtable_id)}")
    print(f"  Claims by Claim Number: {len(claim_by_number)}")

    # ── Step 2: Load existing team members ──
    print("\n[2/6] Loading existing team members...")
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
    print(f"  Team members: {len(sb_team)} ({len(team_by_email)} with email)")

    # Add known Airtable email aliases → same team member
    # (some people use different emails in Airtable vs Supabase)
    EMAIL_ALIASES = {
        "claims@myclaimwarriors.com": "sjerome@spjadjusting.com",    # Sarath Jerome
        "jody@myclaimwarriors.com": "jodymisko@gmail.com",           # Jody Misko
    }
    for alias, primary in EMAIL_ALIASES.items():
        if primary.lower() in team_by_email and alias.lower() not in team_by_email:
            team_by_email[alias.lower()] = team_by_email[primary.lower()]
            print(f"  + Alias: {alias} → {primary}")

    # ── Step 3: Fetch comments from ALL Airtable tables ──
    print("\n[3/6] Fetching comments from Airtable...")
    # Each entry: (source, airtable_record_id, claim_field_value, comment_data)
    all_comments = []
    total_records = 0
    records_with_comments = 0

    for table in TABLES:
        print(f"\n  --- {table['name']} table (ID: {table['table_id']}) ---")
        records = fetch_all_records(table["table_id"], table["field"])
        total_records += len(records)
        print(f"    Total records: {len(records)}")

        for i, rec in enumerate(records):
            record_id = rec["id"]
            field_value = str(rec.get("fields", {}).get(table["field"], "")).strip()

            # Skip records that won't map to any Supabase claim
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
                print(f"    Processed {i + 1}/{len(records)} — {len(all_comments)} comments so far")

            time.sleep(REQUEST_DELAY)

    print(f"\n  Total comments found: {len(all_comments)} across {records_with_comments} records")

    if not all_comments:
        print("\nNo comments to migrate. Done!")
        return

    # ── Step 4: Resolve authors → team member IDs ──
    print("\n[4/6] Resolving comment authors...")
    unique_authors = {}
    for _, _, _, _, comment in all_comments:
        author = comment.get("author", {})
        author_id = author.get("id", "")
        if author_id and author_id not in unique_authors:
            unique_authors[author_id] = {
                "email": (author.get("email") or "").strip(),
                "name": (author.get("name") or "").strip(),
            }

    print(f"  Unique authors: {len(unique_authors)}")

    author_to_tm = {}
    new_team_members = []

    for author_id, info in unique_authors.items():
        email_lower = info["email"].lower()
        name_lower = info["name"].lower()

        if email_lower and email_lower in team_by_email:
            author_to_tm[author_id] = team_by_email[email_lower]
            print(f"  ✓ Matched by email: {info['name']} <{info['email']}>")
        elif name_lower and name_lower in team_by_name:
            author_to_tm[author_id] = team_by_name[name_lower]
            print(f"  ✓ Matched by name: {info['name']}")
        else:
            print(f"  + New team member needed: {info['name']} <{info['email']}>")
            new_team_members.append((author_id, info))

    # Create missing team members
    if new_team_members and not DRY_RUN:
        print(f"\n  Creating {len(new_team_members)} new team members...")
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
                print(f"    Created: {info['name']} → {tm_id}")
            else:
                print(f"    FAILED to create team member for {info['name']}")
    elif new_team_members and DRY_RUN:
        print(f"  [DRY RUN] Would create {len(new_team_members)} new team members")
        for author_id, info in new_team_members:
            author_to_tm[author_id] = f"NEW-{author_id}"

    # ── Step 5: Dedup against existing comments ──
    # Uses (claim_id, normalized_timestamp, comment[:100]) — intentionally excludes
    # team_member_id because old imports may have mapped authors differently.
    print("\n[5/6] Loading existing comments for dedup...")
    existing_comments = supabase_get("Claim Comments", {"select": "claim_id,created_at,comment"})
    existing_keys = set()
    for ec in existing_comments:
        key = (ec["claim_id"], normalize_timestamp(ec["created_at"]), (ec["comment"] or "")[:100])
        existing_keys.add(key)
    print(f"  Existing comments: {len(existing_comments)} ({len(existing_keys)} unique dedup keys)")

    # ── Step 6: Insert new comments ──
    print("\n[6/6] Inserting new comments...")

    inserted = 0
    skipped_no_claim = 0
    skipped_no_author = 0
    skipped_duplicate = 0
    batch = []

    for source, map_by, record_id, field_value, comment in all_comments:
        # Resolve claim_id
        if map_by == "airtable_record_id":
            claim_id = claim_by_airtable_id.get(record_id)
        else:
            claim_id = claim_by_number.get(field_value) if field_value else None

        if not claim_id:
            skipped_no_claim += 1
            continue

        # Resolve author
        author_id = comment.get("author", {}).get("id", "")
        team_member_id = author_to_tm.get(author_id)
        if not team_member_id:
            skipped_no_author += 1
            continue

        # Dedup (normalized timestamp, no team_member_id)
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
                    print(f"    Inserted batch: {inserted} total")
            else:
                inserted += len(batch)
            batch = []

    # Flush remaining
    if batch:
        if not DRY_RUN:
            result = supabase_post("Claim Comments", batch)
            if result:
                inserted += len(batch)
        else:
            inserted += len(batch)

    # ── Summary ──
    print("\n" + "=" * 60)
    print("  SYNC SUMMARY")
    print("=" * 60)
    print(f"  Airtable records scanned:    {total_records}")
    print(f"  Records with comments:       {records_with_comments}")
    print(f"  Total comments found:        {len(all_comments)}")
    print(f"  Comments inserted:           {inserted}")
    print(f"  Skipped (no claim match):    {skipped_no_claim}")
    print(f"  Skipped (no author match):   {skipped_no_author}")
    print(f"  Skipped (duplicate):         {skipped_duplicate}")
    print(f"  New team members created:    {len(new_team_members)}")
    if DRY_RUN:
        print(f"\n  ⚠ DRY RUN — nothing was written to Supabase")
        print(f"  Set DRY_RUN=false to execute for real")
    print("=" * 60)


if __name__ == "__main__":
    main()
