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

def ms_to_iso_date(ms: int) -> str:
    """
    Squarespace gives startDate/endDate as ms since epoch.
    We treat these as all-day events (date-only).
    """
    # Use UTC here; for all-day events we only care about the calendar date
    d = dt.datetime.utcfromtimestamp(ms / 1000.0).date()
    return d.isoformat()


def build_google_event_from_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map the Squarespace event JSON into a Google Calendar event.
    Based on your sample, structure is like:

    {
      "id": "...",
      "title": "...",
      "location": {
        "addressTitle": "...",
        "addressLine1": "...",
        "addressLine2": "...",
        ...
      },
      "structuredContent": {
        "_type": "CalendarEvent",
        "startDate": 1764021600931,
        "endDate": 1767585600931
      },
      "startDate": ... (duplicate),
      "endDate": ...,
      "fullUrl": "/events/...",
      "excerpt": "...",
      ...
    }
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

    start_date = ms_to_iso_date(int(start_ms))
    if end_ms is not None:
        end_date = ms_to_iso_date(int(end_ms))
    else:
        end_date = start_date

    # all-day event representation
    start_field = {"date": start_date, "timeZone": TIMEZONE}
    end_field = {"date": end_date, "timeZone": TIMEZONE}

    # description pieces
    description_parts = []

    excerpt = item.get("excerpt")
    if excerpt:
        description_parts.append(str(excerpt))

    # tags
    tags = item.get("tags") or []
    if tags:
        description_parts.append("Tags: " + ", ".join(tags))

    # author
    author = item.get("author") or {}
    author_name = author.get("displayName") or (
        (author.get("firstName") or "") + " " + (author.get("lastName") or "")
    ).strip()
    if author_name:
        description_parts.append(f"Listed by: {author_name}")

    # link to the original event page
    full_url = item.get("fullUrl")
    source_url = None
    if full_url:
        # fullUrl is like "/events/slug"
        source_url = NYC_BASE_URL.rstrip("/") + full_url

    # base description text
    desc = "\n\n".join(p for p in description_parts if p)

    # extra debugging + marker
    extra = [IMPORT_MARKER]
    if source_url:
        extra.append(f"Source: {source_url}")
    extra.append("Raw item JSON:\n" + json.dumps(item, indent=2))

    full_desc = (desc + "\n\n" + "\n\n".join(extra)).strip()

    event = {
        "summary": summary,
        "location": location,
        "start": start_field,
        "end": end_field,
        "description": full_desc,
    }

    return event


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

    # jitter so we don’t always hit at the exact same second
    delay = random.uniform(0, 300)  # up to 5 minutes
    logging.info(f"Sleeping for {delay:.1f} seconds to add jitter…")
    time.sleep(delay)

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
