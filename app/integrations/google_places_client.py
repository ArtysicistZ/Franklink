"""Google Places API client for fetching nearby businesses.

Uses Google's new Places API (v1) to find cafes, coworking spaces,
and other networking-friendly spots near a user's location.

Results are cached in Redis for 24 hours since places don't change often.
"""

import httpx
import logging
from typing import Any, Dict, List

from app.utils.redis_client import redis_client

logger = logging.getLogger(__name__)

PLACES_CACHE_TTL = 86400  # 24 hours


class GooglePlacesClient:
    """Client for Google Places API (new version).

    Supports async context manager for proper resource cleanup:
        async with GooglePlacesClient(api_key) as client:
            places = await client.get_nearby_places(lat, lon)
    """

    BASE_URL = "https://places.googleapis.com/v1/places:searchNearby"

    def __init__(self, api_key: str):
        """Initialize the client.

        Args:
            api_key: Google Places API key

        Raises:
            ValueError: If api_key is empty
        """
        if not api_key:
            raise ValueError("API key is required for GooglePlacesClient")
        self.api_key = api_key
        self.client = httpx.AsyncClient(timeout=10.0)

    async def __aenter__(self) -> "GooglePlacesClient":
        """Enter async context manager."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit async context manager and close client."""
        await self.close()

    async def get_nearby_places(
        self,
        latitude: float,
        longitude: float,
        place_types: List[str] | None = None,
        radius_meters: int = 1500,
        max_results: int = 5,
    ) -> List[Dict[str, Any]]:
        """Fetch nearby places suitable for networking meetups.

        Args:
            latitude: User's latitude (-90 to 90)
            longitude: User's longitude (-180 to 180)
            place_types: Types of places to search for (default: cafe, coworking)
            radius_meters: Search radius in meters (default: 1500m / ~1 mile)
            max_results: Maximum number of results to return

        Returns:
            List of place dicts with name, address, rating, and types
        """
        # Validate coordinates
        if not (-90 <= latitude <= 90):
            logger.warning(f"[PLACES] Invalid latitude: {latitude}")
            return []
        if not (-180 <= longitude <= 180):
            logger.warning(f"[PLACES] Invalid longitude: {longitude}")
            return []

        if place_types is None:
            # Valid Google Places API types for networking spots
            # See: https://developers.google.com/maps/documentation/places/web-service/place-types
            place_types = [
                "cafe",
                "coffee_shop",
                "restaurant",
                "bar",
                "library",
            ]

        # Clamp radius to valid range
        radius_meters = max(1, min(radius_meters, 50000))

        # Use truncated coordinates for cache key (3 decimal places = ~100m precision)
        cache_key = f"places:{latitude:.3f}:{longitude:.3f}:{','.join(sorted(place_types))}"
        cached = redis_client.get_cached(cache_key)
        if cached is not None:
            logger.debug(f"[PLACES] Cache hit for {cache_key}")
            return cached

        headers = {
            "X-Goog-Api-Key": self.api_key,
            "X-Goog-FieldMask": "places.displayName,places.formattedAddress,places.types,places.rating,places.primaryType",
        }
        body = {
            "includedTypes": place_types,
            "locationRestriction": {
                "circle": {
                    "center": {"latitude": latitude, "longitude": longitude},
                    "radius": float(radius_meters),
                }
            },
            "maxResultCount": max_results,
        }

        try:
            logger.info(f"[PLACES] Fetching nearby places at ({latitude:.4f}, {longitude:.4f})")
            resp = await self.client.post(self.BASE_URL, json=body, headers=headers)
            resp.raise_for_status()
            data = resp.json()

            places = []
            for p in data.get("places", []):
                place = {
                    "name": p.get("displayName", {}).get("text", ""),
                    "address": p.get("formattedAddress", ""),
                    "rating": p.get("rating"),
                    "types": p.get("types", []),
                    "primary_type": p.get("primaryType", ""),
                }
                if place["name"]:  # Only include places with names
                    places.append(place)

            logger.info(f"[PLACES] Found {len(places)} places")
            redis_client.set_cached(cache_key, places, ttl=PLACES_CACHE_TTL)
            return places

        except httpx.HTTPStatusError as e:
            logger.warning(f"[PLACES] API error {e.response.status_code}: {e.response.text[:200]}")
            return []
        except Exception as e:
            logger.warning(f"[PLACES] Request failed: {e}")
            return []

    async def close(self) -> None:
        """Close the HTTP client."""
        await self.client.aclose()
