"""
NYC for Free Calendar Sync - Simple functional version
"""

import json
import logging
import os
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import pytz
import requests
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

from description_scraper import EventDescriptionScraper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
GOOGLE_CALENDAR_ID = os.environ["GOOGLE_CALENDAR_ID"]
NYC_COLLECTION_ID = os.getenv("NYC_COLLECTION_ID", "63de598a71ebc00f98284aaf")
NYC_CRUMB = os.getenv("NYC_CRUMB")
MONTHS_AHEAD = int(os.getenv("NYC_MONTHS_AHEAD", "4"))

NYC_BASE_URL = "https://www.nycforfree.co"
NYC_API_URL = f"{NYC_BASE_URL}/api/open/GetItemsByMonth"
TIMEZONE = "America/New_York"
# IMPORT_MARKER = "Imported from nycforfree.co"
INSERT_DELAY = 0.01
SCRAPED_DESCRIPTION_FIELD = "_scraped_description"


def get_calendar_service():
    """Create Google Calendar API service."""
    creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/calendar"],
    )
    return build("calendar", "v3", credentials=creds)


def fetch_events_for_month(year: int, month: int) -> List[Dict[str, Any]]:
    """Fetch events from NYC for Free API for a specific month."""
    month_str = f"{month:02d}-{year}"
    params = {
        "month": month_str,
        "collectionId": NYC_COLLECTION_ID,
    }
    if NYC_CRUMB:
        params["crumb"] = NYC_CRUMB

    logger.info(f"Fetching events for {month_str}")
    
    try:
        response = requests.get(NYC_API_URL, params=params, timeout=20)
        response.raise_for_status()
        data = response.json()
        
        if isinstance(data, list):
            return data
        
        logger.warning(f"Unexpected response format for {month_str}")
        return []
        
    except requests.RequestException as e:
        logger.error(f"Failed to fetch {month_str}: {e}")
        return []


def fetch_all_events() -> List[Dict[str, Any]]:
    """Fetch events for current month + MONTHS_AHEAD, deduplicated by ID."""
    today = date.today()
    all_events = []
    
    # Fetch events for each month
    for i in range(MONTHS_AHEAD + 1):
        year = today.year
        month = today.month + i
        
        # Handle year rollover
        while month > 12:
            month -= 12
            year += 1
        
        all_events.extend(fetch_events_for_month(year, month))
    
    # Deduplicate by ID
    seen_ids = set()
    unique_events = []
    
    for event in all_events:
        event_id = event.get("id")
        if not event_id:
            event_id = json.dumps(event, sort_keys=True)
        
        if event_id not in seen_ids:
            seen_ids.add(event_id)
            unique_events.append(event)
    
    logger.info(f"Fetched {len(unique_events)} unique events")
    return unique_events


def ms_to_datetime(milliseconds: int) -> datetime:
    """Convert milliseconds since epoch to timezone-aware datetime."""
    tz = pytz.timezone(TIMEZONE)
    return datetime.fromtimestamp(milliseconds / 1000.0, tz=tz)


def is_all_day(start_dt: datetime, end_dt: datetime) -> bool:
    """Check if event should be treated as all-day."""
    # Check midnight times
    if (start_dt.hour == 0 and start_dt.minute == 0 and 
        (end_dt.hour in (0, 23)) and end_dt.minute == 0):
        return True
    
    # Check ~24 hour duration
    duration = end_dt - start_dt
    return timedelta(hours=23) <= duration <= timedelta(hours=25)


def build_google_event(item: Dict[str, Any]) -> Dict[str, Any]:
    """Convert NYC for Free event to Google Calendar event format."""
    # Get title
    title = item.get("title") or "NYC for FREE event"
    
    # Build location string
    location_obj = item.get("location") or {}
    location_parts = []
    for key in ("addressTitle", "addressLine1", "addressLine2", "addressCountry"):
        value = location_obj.get(key, "")
        if value:
            location_parts.append(str(value).strip())
    location = ", ".join(location_parts)
    
    # Get start/end timestamps
    structured = item.get("structuredContent") or {}
    start_ms = structured.get("startDate") or item.get("startDate")
    end_ms = structured.get("endDate") or item.get("endDate")
    
    if not start_ms:
        raise ValueError(f"Missing start date for event: {item.get('id')}")
    
    # Convert to datetime
    try:
        start_dt = ms_to_datetime(int(start_ms))
        end_dt = ms_to_datetime(int(end_ms)) if end_ms else start_dt + timedelta(hours=1)
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid date format for event {item.get('id')}: {e}")
    
    # Build start/end fields
    if is_all_day(start_dt, end_dt):
        start_date = start_dt.date()
        end_date = end_dt.date()
        
        # Adjust if end is at midnight
        if end_dt.hour == 0 and end_dt.minute == 0:
            end_date = (end_dt - timedelta(days=1)).date()
        
        start_field = {"date": start_date.isoformat(), "timeZone": TIMEZONE}
        end_field = {"date": (end_date + timedelta(days=1)).isoformat(), "timeZone": TIMEZONE}
    else:
        start_field = {"dateTime": start_dt.isoformat(), "timeZone": TIMEZONE}
        end_field = {"dateTime": end_dt.isoformat(), "timeZone": TIMEZONE}
    
    # Build description
    scraped_description = item.get(SCRAPED_DESCRIPTION_FIELD, "").strip()
    excerpt = item.get("excerpt", "").strip()
    tags = item.get("tags") or []
    author = item.get("author") or {}
    
    author_name = author.get("displayName", "").strip()
    
    full_url = item.get("fullUrl", "")
    source_url = f"{NYC_BASE_URL.rstrip('/')}{full_url}" if full_url else ""
    
    address_line1 = location_obj.get("addressLine1", "").strip()
    address_line2 = location_obj.get("addressLine2", "").strip()
    
    description_parts = []
    
    if source_url:
        description_parts.append(f"Full Information: {source_url}")
        description_parts.append("\n")
    details_text = scraped_description or excerpt
    
    if address_line1 or address_line2:
        description_parts.append("\nLocation:")
        if address_line1:
            description_parts.append(f"\n{address_line1}")
        if address_line2:
            description_parts.append(f"\n{address_line2}")
        
        description_parts.append("\n")
    
    if details_text:
        description_parts.append("\nAbout:\n")
        description_parts.append(details_text)
        description_parts.append("\n")
    
        
    if tags:
        description_parts.append(f"\nTags: {', '.join(str(t) for t in tags)}")

        description_parts.append("\n")
    if author_name:
        description_parts.append(f"\nListed by: {author_name}")
    
    # description_parts.append(f"\n\nRaw item JSON:\n{json.dumps(item, indent=2)}")

    description = "".join(description_parts)
    
    return {
        "summary": title,
        "location": location,
        "start": start_field,
        "end": end_field,
        "description": description,
    }


def delete_all_events(service, calendar_id: str) -> int:
    """Delete all events from calendar."""
    deleted = 0
    page_token = None
    
    logger.info("Deleting all existing events...")
    
    while True:
        try:
            result = service.events().list(
                calendarId=calendar_id,
                singleEvents=True,
                pageToken=page_token,
                maxResults=250,
            ).execute()
            
            for event in result.get("items", []):
                service.events().delete(
                    calendarId=calendar_id,
                    eventId=event["id"],
                ).execute()
                deleted += 1
            
            page_token = result.get("nextPageToken")
            if not page_token:
                break
                
        except Exception as e:
            logger.error(f"Error deleting events: {e}")
            raise
    
    logger.info(f"Deleted {deleted} events")
    return deleted


def insert_events(service, calendar_id: str, events: List[Dict[str, Any]]) -> int:
    """Insert events into calendar with rate limiting."""
    inserted = 0
    
    logger.info(f"Inserting {len(events)} events...")
    
    for event in events:
        try:
            service.events().insert(
                calendarId=calendar_id,
                body=event,
            ).execute()
            inserted += 1
            time.sleep(INSERT_DELAY)
            
        except Exception as e:
            logger.warning(f"Failed to insert '{event.get('summary')}': {e}")
    
    logger.info(f"Inserted {inserted}/{len(events)} events")
    return inserted


def main():
    """Main sync function."""
    logger.info("Starting NYC for Free calendar sync")
    
    try:
        # Initialize services
        service = get_calendar_service()
        scraper = EventDescriptionScraper(base_url=NYC_BASE_URL)
        
        # Delete existing events
        delete_all_events(service, GOOGLE_CALENDAR_ID)
        
        # Fetch events from NYC for Free
        nyc_events = fetch_all_events()
        
        # Process and insert events one at a time
        logger.info(f"Processing and inserting {len(nyc_events)} events...")
        inserted = 0
        
        for i, event in enumerate(nyc_events, 1):
            try:
                # Scrape description for this event
                url = event.get("fullUrl")
                if url:
                    description = scraper.get_description(url)
                    if description:
                        event[SCRAPED_DESCRIPTION_FIELD] = description
                
                # Convert to Google Calendar format
                google_event = build_google_event(event)
                
                # Insert immediately
                service.events().insert(
                    calendarId=GOOGLE_CALENDAR_ID,
                    body=google_event,
                ).execute()
                inserted += 1
                
                if i % 10 == 0:
                    logger.info(f"Processed {i}/{len(nyc_events)} events")
                
                time.sleep(INSERT_DELAY)
                
            except Exception as e:
                logger.warning(f"Failed to process event {event.get('id')}: {e}")
        
        logger.info(f"Sync completed successfully. Inserted {inserted}/{len(nyc_events)} events")
        return 0
        
    except Exception as e:
        logger.error(f"Sync failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit(main())