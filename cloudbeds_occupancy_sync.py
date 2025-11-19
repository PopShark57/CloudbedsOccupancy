#!/usr/bin/env python3
"""
Scrape Cloudbeds occupancy via Airtop and update an Airtable record
that powers a Softr "Available Rooms / Occupancy" widget.

Requirements:
    pip install airtop requests

Environment variables (recommended):
    export AIRTOP_API_KEY="sk-xxx"
    export AIRTABLE_API_KEY="patXXX"  # or old style key

Then run:
    python cloudbeds_occupancy_sync.py
"""

import os
import asyncio
import json
import logging
import httpx
from typing import Dict, Any, Optional

import requests
from airtop import AsyncAirtop, SessionConfigV1, PageQueryConfig, PageQueryExperimentalConfig
from airtop.core.api_error import ApiError

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

# Airtop
AIRTOP_API_KEY = os.getenv("AIRTOP_API_KEY", "YOUR_API_KEY_HERE")
CLOUDBEDS_PROFILE_NAME = "Cloudbeds"
CLOUDBEDS_DASHBOARD_URL = "https://hotels.cloudbeds.com/connect/310986#/dashboard"

# Airtable (used as backend for Softr)
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "YOUR_AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = "appqstjfFNrdvbxIE"      # <-- replace with your base id
AIRTABLE_TABLE_NAME = "Occupancy"           # <-- replace with your table name
AIRTABLE_RECORD_ID = "recUbCZSPvOPj10Rn"    # <-- record Softr reads from

# Airtable field names
FIELD_AVAILABLE = "Available Units"
FIELD_BOOKED = "Booked Units"
FIELD_OOS = "Out of Service"
FIELD_BLOCKED = "Blocked Dates"

# LLM prompt for Airtop page_query
PROMPT = """
You are inspecting a Cloudbeds dashboard page.

Focus ONLY on the Occupancy widget on the left side:
- It has a donut chart showing a percentage like "28.57% Occupancy".
- Directly underneath the donut, there are four rows of text, each row starting with a number and then a label:
  - "<number> Available units"
  - "<number> Booked units"
  - "<number> Out of service"
  - "<number> Blocked dates"

Your task:
- Read those four numbers from those four rows (not from anywhere else on the page).
- Output them as JSON with this exact shape and nothing else.

Important rules:
- Do NOT use numbers from any other widgets or sections (for example, the Forecast section, room nights, arrivals, bookings, cancellations, etc.).
- If you cannot clearly see this Occupancy widget OR you cannot confidently read all four rows, respond with exactly this string and nothing else: NO_DATA
- Only return 0 for a field if the corresponding row in the Occupancy widget literally shows 0.
- If you are unsure about any of the four numbers, respond NO_DATA instead of guessing or defaulting to zeros.

Output format (no commentary, no extra keys):

{
  "available_units": <number>,
  "booked_units": <number>,
  "out_of_service": <number>,
  "blocked_dates": <number>
}
"""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# ---------------------------------------------------------------------------
# AIRTABLE BACKEND
# ---------------------------------------------------------------------------

def update_airtable(occupancy: Dict[str, Any]) -> None:
    """
    Update a single Airtable record with the values from the occupancy dict.

    The table should have at least these fields:
        - Available Units
        - Booked Units
        - Out of Service
        - Blocked Dates
    """
    if not AIRTABLE_API_KEY or AIRTABLE_API_KEY == "YOUR_AIRTABLE_API_KEY":
        raise RuntimeError("AIRTABLE_API_KEY is not set")

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}/{AIRTABLE_RECORD_ID}"

    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json",
    }

    payload = {
        "fields": {
            FIELD_AVAILABLE: occupancy.get("available_units"),
            FIELD_BOOKED: occupancy.get("booked_units"),
            FIELD_OOS: occupancy.get("out_of_service"),
            FIELD_BLOCKED: occupancy.get("blocked_dates"),
        }
    }

    logging.info("Updating Airtable: %s", payload["fields"])
    resp = requests.patch(url, headers=headers, json=payload)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        logging.error("Error updating Airtable: %s - %s", e, resp.text)
        raise

    logging.info("Airtable update OK.")


# ---------------------------------------------------------------------------
# AIRTOP / CLOUDBEDS SCRAPE
# ---------------------------------------------------------------------------

async def fetch_occupancy_from_cloudbeds() -> Optional[Dict[str, Any]]:
    """
    Use Airtop to open the Cloudbeds dashboard and ask the LLM to read
    the Occupancy widget, returning a dict like:

        {
          "available_units": 28,
          "booked_units": 8,
          "out_of_service": 3,
          "blocked_dates": 2
        }

    Returns:
        Dict with occupancy values on success, or None if the model
        could not see the Occupancy widget after several attempts.
    """
    if not AIRTOP_API_KEY or AIRTOP_API_KEY == "AIRTOP_API_KEY":
        raise RuntimeError("AIRTOP_API_KEY is not set")

    client: AsyncAirtop | None = None
    session_id: str | None = None

    try:
        # Initialize Airtop client
        client = AsyncAirtop(
            api_key=AIRTOP_API_KEY,
            timeout=httpx.Timeout(60.0, read=120.0, connect=10.0),
        )

        # Use a saved "Cloudbeds" profile that is already logged in
        configuration = SessionConfigV1(
            profile_name=CLOUDBEDS_PROFILE_NAME,
            timeout_minutes=10,
        )

        session = await client.sessions.create(configuration=configuration)
        if not session or (hasattr(session, "errors") and session.errors):
            raise RuntimeError(f"Failed to create session: {getattr(session, 'errors', None)}")

        session_id = session.data.id if session.data else None
        logging.info("Created Airtop session: %s", session_id)

        # Save login cookies etc. when we terminate
        await client.sessions.save_profile_on_termination(session_id, CLOUDBEDS_PROFILE_NAME)

        # Open Cloudbeds dashboard
        window = await client.windows.create(
            session_id,
            url=CLOUDBEDS_DASHBOARD_URL,
        )
        if not window.data:
            raise RuntimeError("Failed to create window")

        window_id = window.data.window_id
        logging.info("Created window: %s", window_id)

        # Give the dashboard time to fully load
        await asyncio.sleep(5)

        experimental_config = PageQueryExperimentalConfig(
            includeVisualAnalysis="auto",
        )

        max_attempts = 3
        expected_keys = {
            "available_units",
            "booked_units",
            "out_of_service",
            "blocked_dates",
        }

        for attempt in range(1, max_attempts + 1):
            logging.info("Running page_query attempt %d/%d...", attempt, max_attempts)
            prompt_response = await client.windows.page_query(
                session_id=session_id,
                window_id=window_id,
                prompt=PROMPT,
                configuration=PageQueryConfig(
                    experimental=experimental_config,
                ),
            )

            if hasattr(prompt_response, "error") and prompt_response.error:
                raise RuntimeError(f"Failed to prompt content: {prompt_response.error}")

            raw = prompt_response.data.model_response
            logging.info("Raw model response (attempt %d): %r", attempt, raw)

            raw_stripped = raw.strip()
            normalized = raw_stripped.strip("\"'").strip()

            # Handle NO_DATA sentinel response
            if normalized == "NO_DATA":
                logging.warning(
                    "Attempt %d: model returned NO_DATA – Occupancy widget not visible.",
                    attempt,
                )
                if attempt == max_attempts:
                    logging.warning(
                        "Maximum attempts reached with NO_DATA. Returning None and skipping update."
                    )
                    return None
                await asyncio.sleep(5)
                continue

            if not raw_stripped.startswith("{"):
                raise RuntimeError(f"Model did not return JSON; got: {raw_stripped}")

            # Try to parse JSON from the model response
            occupancy = json.loads(raw_stripped)

            # Basic sanity-check of keys
            missing = expected_keys - set(occupancy.keys())
            if missing:
                raise ValueError(f"Missing keys in occupancy JSON: {missing}")

            # Sanity check: if everything comes back as 0, retry unless we've exhausted attempts
            if all(occupancy.get(k) == 0 for k in expected_keys):
                logging.warning(
                    "Attempt %d: model returned 0 for all occupancy fields. "
                    "This is likely incorrect; will retry if attempts remain.",
                    attempt,
                )
                if attempt == max_attempts:
                    raise RuntimeError(
                        "Model returned 0 for all occupancy fields after multiple attempts."
                    )
                await asyncio.sleep(5)
                continue

            # Successful parse with non-zero data (or genuine zeroes that passed checks)
            return occupancy

        # Should not normally reach here
        raise RuntimeError("Failed to obtain occupancy data after retries.")

    except ApiError as e:
        logging.error("Airtop API Error: %s - %s", e.status_code, e.body)
        raise
    except httpx.ReadTimeout as e:
        logging.error(
            "Timed out while talking to the Airtop API. "
            "This is usually a capacity or network issue. Details: %s",
            e,
        )
        raise
    except httpx.HTTPError as e:
        logging.error("HTTP error while talking to the Airtop API: %s", e)
        raise
    finally:
        if client is not None and session_id is not None:
            logging.info("Terminating Airtop session...")
            try:
                await client.sessions.terminate(session_id)
            except Exception as e:
                logging.warning("Error terminating session: %s", e)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

async def main():
    occupancy = await fetch_occupancy_from_cloudbeds()
    if occupancy is None:
        logging.warning("No occupancy data available; skipping Airtable update this run.")
        return

    logging.info("Occupancy from Cloudbeds: %s", occupancy)
    update_airtable(occupancy)


if __name__ == "__main__":
    asyncio.run(main())
