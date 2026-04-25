import os
import json
import requests
from typing import Any, Dict, Optional
from .utils import USER_AGENTS


def get_coordinates(city: str) -> Optional[Dict[str, str]]:
    """
    Convert a city name to latitude and longitude coordinates.
    
    Args:
        city (str): Name of the city to geocode
        
    Returns:
        tuple: (latitude, longitude) if successful, (None, None) if not
    """
    try:
        response = requests.get(
            "https://nominatim.openstreetmap.org/search", 
            params={"q": city, "format": "json"}, 
            headers={"User-Agent": USER_AGENTS[2]},
            timeout=30,
        )
        data = response.json()
        if data:
            return {"lat": data[0]['lat'], "lon": data[0]['lon']}
        else:
            return None
    except Exception as e:
        print(f"Error getting coordinates: {e}")
        return None


def search_places(query: str, coords: Dict[str, str], num_pages: int = 1, api_key: Optional[str] = None) -> list[dict]:
    """
    Search for places using Serper Maps API.
    
    Args:
        query (str): Search query (e.g., "restaurants", "dentists")
        coords (dict): Latitude and longitude dict
        num_pages (int): Number of pages to request (20 results per page)
        api_key (str | None): Serper API key. Falls back to SERPER_API_KEY env var.
        
    Returns:
        list: List of places data from the API
    """
    payload = []
    lat, lon = coords['lat'], coords['lon']
    
    # Create payload for each page
    for page in range(1, num_pages + 1):
        payload.append({
            "q": query,
            "ll": f"@{lat},{lon},13z", # Format the location string for Serper API
            "page": page
        })
    
    headers = {
        'X-API-KEY': api_key or os.getenv("SERPER_API_KEY"),
        'Content-Type': 'application/json'
    }
    if not headers["X-API-KEY"]:
        raise ValueError("Missing Serper API key. Provide api_key or set SERPER_API_KEY.")
    
    try:
        response = requests.post(
            "https://google.serper.dev/maps", 
            headers=headers, 
            data=json.dumps(payload),
            timeout=60,
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: API returned status code {response.status_code} - {response.text}")
            return []
            
    except Exception as e:
        print(f"Error making API request: {e}")
        return []


def serper_web_search(query: str, api_key: Optional[str] = None, num: int = 5, timeout: int = 60) -> Dict[str, Any]:
    """
    Run a Serper "search" query (standard Google web results).

    Returns the raw Serper response dict (commonly includes an "organic" list).
    """
    headers = {
        "X-API-KEY": api_key or os.getenv("SERPER_API_KEY"),
        "Content-Type": "application/json",
    }
    if not headers["X-API-KEY"]:
        raise ValueError("Missing Serper API key. Provide api_key or set SERPER_API_KEY.")

    payload = {"q": query, "num": int(num)}
    response = requests.post(
        "https://google.serper.dev/search",
        headers=headers,
        data=json.dumps(payload),
        timeout=timeout,
    )
    if response.status_code == 200:
        return response.json()
    raise RuntimeError(f"Serper search failed: {response.status_code} - {response.text}")
