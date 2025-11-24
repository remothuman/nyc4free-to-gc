import os
import json
import logging
import datetime as dt
from typing import Any, Dict, List
import requests

from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- CONFIG FROM ENV ---------------------------------------------------------

GOOGLE_SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
GOOGLE_CALENDAR_ID = os.environ["GOOGLE_CALENDAR_ID"]

NYC_COLLECTION_ID = os.environ.get("NYC_COLLECTION_ID", "63de598a71ebc00f98284aaf")
NYC_CRUMB = os.environ["NYC_CRUMB"]  # must be set
NYC_MONTHS_AHEAD = int(os.environ.get("NYC_MONTHS_AHEAD", "2"))

NYC_BASE_URL = "https://www.nycforfree.co"
NYC_API_URL = f"{NYC_BASE_URL}/api/open/GetItemsByMonth"

TIMEZONE = "America/New_York"
IMPORT_MARKER = "Imported from nycforfree.co"


# --- GOOGLE CALENDAR SERVICE (SERVICE ACCOUNT) -------------------------------

def get_calendar_service():
    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/calendar"],
    )
    service = build("calendar", "v3", credentials=creds)
    return service


# --- NYC FOR FREE API --------------------------------------------------------

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
        "crumb": NYC_CRUMB,
    }
    logging.info(f"Fetching items for {month_str}")
    resp = requests.get(NYC_API_URL, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    # We don't know if it's "items" or "Items" etc, so try a few.
    for key in ("items", "Items", "data", "Data"):
        if isinstance(data, dict) and key in data and isinstance(data[key], list):
            return data[key]

    # Fallback: if the JSON itself is a list
    if isinstance(data, list):
        return data

    logging.warning(f"Unexpected JSON structure for {month_str}: {data}")
    return []


def fetch_all_items() -> List[Dict[str, Any]]:
    """
    Fetch items for current month + NYC_MONTHS_AHEAD.
    De-duplicate by some stable field if present.
    """
    today = dt.date.today()
    all_items: List[Dict[str, Any]] = []

    for year, month in month_iter(today.replace(day=1), NYC_MONTHS_AHEAD):
        all_items.extend(fetch_month_items(year, month))

    # Deduplicate if items have some sort of id/slug
    seen = set()
    unique_items = []
    for item in all_items:
        key = item.get("id") or item.get("_id") or item.get("slug") or json.dumps(
            item, sort_keys=True
        )
        if key not in seen:
            seen.add(key)
            unique_items.append(item)

    logging.info(f"Total unique items fetched: {len(unique_items)}")
    return unique_items


# --- HELPER: INFER FIELDS FROM UNKNOWN JSON SHAPE ---------------------------

def _find_key(item: Dict[str, Any], substrings: List[str]) -> str | None:
    """
    Return first key where all substrings appear in the lowercase key.
    """
    lower_map = {k.lower(): k for k in item.keys()}
    for lower_key, orig_key in lower_map.items():
        if all(sub in lower_key for sub in substrings):
            return orig_key
    return None


def build_google_event_from_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Try to infer title, start, end, location, description from an arbitrary item.
    You may want to print(item.keys()) once locally to tighten this mapping.
    """

    # title / summary
    title_key = (
        _find_key(item, ["title"])
        or _find_key(item, ["name"])
        or _find_key(item, ["summary"])
    )
    summary = str(item.get(title_key, "NYC for FREE event"))

    # location
    location_key = (
        _find_key(item, ["location"])
        or _find_key(item, ["address"])
        or _find_key(item, ["venue"])
        or _find_key(item, ["place"])
    )
    location = str(item.get(location_key, "")) if location_key else ""

    # description-ish fields
    description_parts = []
    for key in ("description", "body", "excerpt", "details"):
        k = _find_key(item, [key])
        if k and item.get(k):
            description_parts.append(str(item[k]))

    # Sometimes there is a URL / link
    url_key = _find_key(item, ["url"]) or _find_key(item, ["link"])
    source_url = item.get(url_key)

    # start / end datetime-ish fields
    # We look for something like startDate, start, begin, etc.
    start_key = (
        _find_key(item, ["start", "date"])
        or _find_key(item, ["start"])
        or _find_key(item, ["begin"])
    )
    end_key = (
        _find_key(item, ["end", "date"])
        or _find_key(item, ["end"])
        or _find_key(item, ["finish"])
    )

    start_raw = item.get(start_key) if start_key else None
    end_raw = item.get(end_key) if end_key else None

    if not start_raw:
        raise ValueError(f"Cannot infer start time from item: {item}")

    # Convert into Google Calendar datetime/date fields
    start_field, end_field = _build_time_fields(start_raw, end_raw)

    # Base event
    event = {
        "summary": summary,
        "location": location,
        "start": start_field,
        "end": end_field,
    }

    # Build description
    desc = "\n\n".join([p for p in description_parts if p])
    extra = [IMPORT_MARKER]
    if source_url:
        extra.append(f"Source: {source_url}")
    # embed raw JSON for debugging if you want
    extra.append("Raw item JSON:\n" + json.dumps(item, indent=2))
    full_desc = (desc + "\n\n" + "\n\n".join(extra)).strip()
    event["description"] = full_desc

    return event


def _build_time_fields(start_raw: Any, end_raw: Any | None):
    """
    Map raw date/time strings into Google Calendar start/end dicts.
    Handles all-day vs dateTime heuristically.
    """

    def parse_maybe_date(s: str) -> tuple[bool, str]:
        s = s.strip()
        # All-day date like '2025-11-24'
        try:
            if len(s) == 10:
                dt.datetime.strptime(s, "%Y-%m-%d")
                return True, s
        except Exception:
            pass
        # Anything else: assume RFC3339-ish; let Calendar API validate
        return False, s

    start_is_date, start_val = parse_maybe_date(str(start_raw))
    if end_raw:
        end_is_date, end_val = parse_maybe_date(str(end_raw))
    else:
        end_is_date, end_val = start_is_date, start_val

    if start_is_date and end_is_date:
        # all-day event
        start_field = {"date": start_val, "timeZone": TIMEZONE}
        end_field = {"date": end_val, "timeZone": TIMEZONE}
    else:
        # timed event
        start_field = {"dateTime": start_val, "timeZone": TIMEZONE}
        end_field = {"dateTime": end_val, "timeZone": TIMEZONE}

    return start_field, end_field


# --- SYNC LOGIC -------------------------------------------------------------

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
    logging.info(f"Inserted {count} events.")


def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting NYC for FREE calendar sync (service account mode)")

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
