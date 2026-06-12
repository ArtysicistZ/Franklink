"""Build rich location context for agent responses.

Combines stored location data with nearby places to give Frank
the context needed to respond like a knowledgeable local.
"""

import logging
import os
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Place type mappings for categorization
CAFE_TYPES = {"cafe", "coffee_shop"}
BAR_TYPES = {"bar", "wine_bar", "pub"}
RESTAURANT_TYPES = {"restaurant", "american_restaurant", "italian_restaurant", "mexican_restaurant"}
LIBRARY_TYPES = {"library"}


def _categorize_place(place: Dict[str, Any]) -> Optional[str]:
    """Categorize a place into cafe, bar, restaurant, or library.

    Args:
        place: Place dict with name, types, primary_type

    Returns:
        Category string or None if uncategorized
    """
    types = set(place.get("types", []))
    primary_type = place.get("primary_type", "")
    name = place.get("name", "").lower()

    # Check primary type first (most reliable)
    if primary_type in CAFE_TYPES or "coffee" in name:
        return "cafe"
    if primary_type in BAR_TYPES:
        return "bar"
    if primary_type in RESTAURANT_TYPES or primary_type.endswith("_restaurant"):
        return "restaurant"
    if primary_type in LIBRARY_TYPES:
        return "library"

    # Fall back to checking all types
    if types & CAFE_TYPES:
        return "cafe"
    if types & BAR_TYPES:
        return "bar"
    if types & RESTAURANT_TYPES:
        return "restaurant"
    if types & LIBRARY_TYPES:
        return "library"

    return None


async def build_location_context(
    location_data: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Build rich location context including nearby places.

    Args:
        location_data: User's stored location from user_locations table

    Returns:
        Dict with:
            - has_location: bool
            - short_address: str (e.g., "Palo Alto, CA")
            - long_address: str (full address)
            - latitude: float
            - longitude: float
            - nearby_cafes: List[str] (cafe/coffee shop names)
            - nearby_restaurants: List[str] (restaurant names)
            - nearby_bars: List[str] (bar names)
            - nearby_libraries: List[str] (library names)
            - area_summary: str (formatted for Frank's prompt)
    """
    if not location_data:
        return {
            "has_location": False,
            "area_summary": "location not shared yet",
        }

    lat = location_data.get("latitude")
    lon = location_data.get("longitude")
    short_addr = location_data.get("short_address", "")
    long_addr = location_data.get("long_address", "")

    if not lat or not lon:
        return {
            "has_location": False,
            "area_summary": "location not shared yet",
        }

    # Fetch nearby places if API key is configured
    api_key = os.getenv("GOOGLE_PLACES_API_KEY")
    cafes: List[str] = []
    restaurants: List[str] = []
    bars: List[str] = []
    libraries: List[str] = []

    if api_key:
        try:
            from app.integrations.google_places_client import GooglePlacesClient

            # Use context manager to ensure proper cleanup of HTTP client
            async with GooglePlacesClient(api_key) as client:
                places = await client.get_nearby_places(lat, lon, max_results=10)

                for p in places:
                    name = p.get("name", "")
                    if not name:
                        continue

                    category = _categorize_place(p)
                    if category == "cafe" and name not in cafes:
                        cafes.append(name)
                    elif category == "restaurant" and name not in restaurants:
                        restaurants.append(name)
                    elif category == "bar" and name not in bars:
                        bars.append(name)
                    elif category == "library" and name not in libraries:
                        libraries.append(name)

        except ValueError as e:
            # API key validation failed
            logger.warning(f"[LOCATION_CONTEXT] Places client init failed: {e}")
        except Exception as e:
            logger.warning(f"[LOCATION_CONTEXT] Places lookup failed: {e}")

    # Build area summary for Frank's context
    area_summary = _build_area_summary(
        short_addr, long_addr, cafes, restaurants, bars, libraries
    )

    return {
        "has_location": True,
        "short_address": short_addr,
        "long_address": long_addr,
        "latitude": lat,
        "longitude": lon,
        "nearby_cafes": cafes[:3],
        "nearby_restaurants": restaurants[:3],
        "nearby_bars": bars[:3],
        "nearby_libraries": libraries[:2],
        "area_summary": area_summary,
    }


def _build_area_summary(
    short_addr: str,
    long_addr: str,
    cafes: List[str],
    restaurants: List[str],
    bars: List[str],
    libraries: List[str],
) -> str:
    """Build a formatted summary string for Frank's prompt.

    Args:
        short_addr: Short address (e.g., "Palo Alto, CA")
        long_addr: Full address
        cafes: List of nearby cafe names
        restaurants: List of nearby restaurant names
        bars: List of nearby bar names
        libraries: List of nearby library names

    Returns:
        Formatted string like:
        '"Palo Alto, CA" - coffee spots: Philz, Verve | restaurants: Tamarine | bars: The Rose'
    """
    address = short_addr or long_addr or "unknown location"
    parts = [f'"{address}"']

    place_parts = []
    if cafes:
        place_parts.append(f"coffee spots: {', '.join(cafes[:3])}")
    if restaurants:
        place_parts.append(f"restaurants: {', '.join(restaurants[:3])}")
    if bars:
        place_parts.append(f"bars: {', '.join(bars[:2])}")
    if libraries:
        place_parts.append(f"libraries: {', '.join(libraries[:2])}")

    if place_parts:
        parts.append(" | ".join(place_parts))

    return " - ".join(parts)
