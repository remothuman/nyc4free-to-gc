import os
import json
import logging
import datetime as dt
import random
import time
from typing import Any, Dict, List

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build


# ---------------------------------------------------------------------
# CONFIG FROM ENV
# ---------------------------------------------------------------------

from dotenv import load_dotenv
load_dotenv()

GOOGLE_SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
GOOGLE_CALENDAR_ID = os.environ["GOOGLE_CALENDAR_ID"]

NYC_COLLECTION_ID = os.environ.get("NYC_COLLECTION_ID", "63de598a71ebc00f98284aaf")
NYC_CRUMB = os.environ.get("NYC_CRUMB")  # OPTIONAL
NYC_MONTHS_AHEAD = int(os.environ.get("NYC_MONTHS_AHEAD", "2"))

NYC_BASE_URL = "https://www.nycforfree.co"
NYC_API_URL = f"{NYC_BASE_URL}/api/open/GetItemsByMonth"

TIMEZONE = "America/New_York"
IMPORT_MARKER = "Imported from nycforfree.co"

# throttle a bit so we’re extra nice to Google
INSERT_SLEEP_SECONDS = 0.1  # 10 writes/sec


# ---------------------------------------------------------------------
# GOOGLE CALENDAR SERVICE (SERVICE ACCOUNT)
# ---------------------------------------------------------------------

def get_calendar_service():
    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/calendar"],
    )
    return build("calendar", "v3", credentials=creds)


# ---------------------------------------------------------------------
# NYC FOR FREE API
# ---------------------------------------------------------------------

def month_iter(start_date: dt.date, months_ahead: int):
    """Yield (year, month) from the month of start_date forward."""
    year = start_date.year
    month = start_date.month
    for _ in range(months_ahead + 1):  # include current month
        yield year, month
        month += 1
        if month > 12:
            month = 1
            year += 1


def fetch_month_items(year: int, month: int) -> List[Dict[str, Any]]:
    """
    Call the GetItemsByMonth API for a given year-month
    and return the list of items.
    """
    month_str = f"{month:02d}-{year}"
    params = {
        "month": month_str,
        "collectionId": NYC_COLLECTION_ID,
    }
    # crumb is optional
    if NYC_CRUMB:
        params["crumb"] = NYC_CRUMB

    logging.info(f"Fetching items for {month_str} with params={params}")
    resp = requests.get(NYC_API_URL, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    # For this Squarespace endpoint, the top-level is a list of event objects.
    if isinstance(data, list):
        return data

    logging.warning(f"Unexpected JSON structure for {month_str}: {data}")
    return []


def fetch_all_items() -> List[Dict[str, Any]]:
    """
    Fetch items for current month + NYC_MONTHS_AHEAD.
    De-duplicate by item['id'].
    """
    today = dt.date.today()
    all_items: List[Dict[str, Any]] = []

    for year, month in month_iter(today.replace(day=1), NYC_MONTHS_AHEAD):
        all_items.extend(fetch_month_items(year, month))

    seen_ids = set()
    unique_items = []
    for item in all_items:
        item_id = item.get("id")
        if not item_id:
            # as a fallback, dedupe by full JSON
            item_id = json.dumps(item, sort_keys=True)
        if item_id not in seen_ids:
            seen_ids.add(item_id)
            unique_items.append(item)

    logging.info(f"Total unique items fetched: {len(unique_items)}")
    return unique_items


# ---------------------------------------------------------------------
# HELPERS FOR BUILDING EVENTS
# ---------------------------------------------------------------------

def ms_to_datetime(ms: int) -> dt.datetime:
    """Convert milliseconds since epoch to a timezone-aware datetime in the local timezone."""
    return dt.datetime.fromtimestamp(ms / 1000.0, tz=dt.timezone(dt.timedelta(hours=-5)))  # Default to EST/EDT



def build_google_event_from_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map the Squarespace event JSON into a Google Calendar event.
    """
    # summary
    
    summary = item.get("title") or "NYC for FREE event"

    # location fields
    location_obj = item.get("location") or {}
    address_parts = []
    for key in ("addressTitle", "addressLine1", "addressLine2", "addressCountry"):
        val = location_obj.get(key)
        if val:
            address_parts.append(str(val))
    location = ", ".join(address_parts)

    # dates
    sc = item.get("structuredContent") or {}
    start_ms = sc.get("startDate") or item.get("startDate")
    end_ms = sc.get("endDate") or item.get("endDate")

    if start_ms is None:
        raise ValueError(f"No start date in item: {item}")

    # Convert timestamps to datetime objects
    start_dt = ms_to_datetime(int(start_ms))
    end_dt = ms_to_datetime(int(end_ms)) if end_ms is not None else start_dt + dt.timedelta(hours=1)

    # Check if it's an all-day event (time is midnight)
    if start_dt.hour == 0 and start_dt.minute == 0 and (end_dt.hour == 0 or end_dt.hour == 23) and end_dt.minute == 0:
        # All-day event
        start_field = {"date": start_dt.date().isoformat(), "timeZone": TIMEZONE}
        end_field = {"date": end_dt.date().isoformat(), "timeZone": TIMEZONE}
    else:
        # Timed event
        start_field = {"dateTime": start_dt.isoformat(), "timeZone": TIMEZONE}
        end_field = {"dateTime": end_dt.isoformat(), "timeZone": TIMEZONE}

    # Prepare description components
    excerpt = str(item.get("excerpt", "")).strip()
    
    # Format tags if they exist
    tags = item.get("tags") or []
    tags_str = f"Tags: {', '.join(tags)}" if tags else ""
    
    # Format author information if available
    author = item.get("author") or {}
    author_name = (
        author.get("displayName") or 
        f"{author.get('firstName', '').strip()} {author.get('lastName', '').strip()}".strip()
    )
    author_str = f"Listed by: {author_name}" if author_name else ""
    
    # Prepare source URL
    full_url = item.get("fullUrl")
    source_url = f"{NYC_BASE_URL.rstrip('/')}{full_url}" if full_url else ""
    
    # Prepare debug info
    debug_info = [
        # IMPORT_MARKER,
        # f"Source: {source_url}" if source_url else "",
        "Raw item JSON:\n" + json.dumps(item, indent=2)
    ]
    debug_info_str = "\n\n".join(filter(None, debug_info))

    # Format address if available
    address_line1 = location_obj.get('addressLine1', '').strip()
    address_line2 = location_obj.get('addressLine2', '').strip()
    address = '\n'.join(line for line in [address_line1, address_line2] if line)

    # Build the description using the template
    description = f"""
Full Information: {source_url}
{excerpt + "\n" if excerpt else ""}
Location: 
{address_line1}
{address_line2}
    
Tags: {tags_str}
Listed by: {author_name}

{debug_info_str}
    """.strip()

    return {
        "summary": summary,
        "location": location,
        "start": start_field,
        "end": end_field,
        "description": description,
    }


# ---------------------------------------------------------------------
# SYNC LOGIC
# ---------------------------------------------------------------------

def delete_existing_imported_events(service):
    """
    Delete future events on the calendar that we previously imported
    (we detect them via IMPORT_MARKER in the description).
    """
    now = dt.datetime.utcnow().isoformat() + "Z"

    page_token = None
    deleted = 0

    while True:
        events_result = (
            service.events()
            .list(
                calendarId=GOOGLE_CALENDAR_ID,
                timeMin=now,
                singleEvents=True,
                orderBy="startTime",
                pageToken=page_token,
            )
            .execute()
        )
        items = events_result.get("items", [])
        for ev in items:
            desc = (ev.get("description") or "").lower()
            if IMPORT_MARKER.lower() in desc:
                service.events().delete(
                    calendarId=GOOGLE_CALENDAR_ID,
                    eventId=ev["id"],
                ).execute()
                deleted += 1

        page_token = events_result.get("nextPageToken")
        if not page_token:
            break

    logging.info(f"Deleted {deleted} previously-imported events.")


def insert_events(service, events: List[Dict[str, Any]]):
    count = 0
    for e in events:
        service.events().insert(
            calendarId=GOOGLE_CALENDAR_ID,
            body=e,
        ).execute()
        count += 1
        # light throttling
        time.sleep(INSERT_SLEEP_SECONDS)
    logging.info(f"Inserted {count} events.")


def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting NYC for FREE calendar sync (service account mode)")

    # # jitter so we don’t always hit at the exact same second
    # delay = random.uniform(0, 300)  # up to 5 minutes
    # logging.info(f"Sleeping for {delay:.1f} seconds to add jitter…")
    # time.sleep(delay)

    service = get_calendar_service()

    logging.info("Deleting previously-imported future events…")
    delete_existing_imported_events(service)

    logging.info("Fetching events from NYC for FREE API…")
    items = fetch_all_items()

    logging.info("Building Google Calendar events…")
    events = []
    for item in items:
        try:
            events.append(build_google_event_from_item(item))
        except Exception as e:
            logging.warning(f"Skipping item due to error {e}: {item}")

    logging.info("Inserting events into Google Calendar…")
    insert_events(service, events)

    logging.info("Sync finished.")


if __name__ == "__main__":
    main()
