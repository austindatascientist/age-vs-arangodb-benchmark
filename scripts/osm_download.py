#!/usr/bin/env python3
"""
Alabama OSM Data Downloader using Overpass API

Downloads pre-filtered OSM data (highways and tourist attractions only) for Alabama.
"""

import sys
import time
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from urllib.parse import quote


OVERPASS_URL = "https://overpass-api.de/api/interpreter"
MAX_RETRIES = 3
RETRY_DELAY = 60  # Overpass API rate limits aggressively, need long delay

# Highway types to download (freeways and major highways)
HIGHWAY_TYPES = ['motorway', 'motorway_link', 'trunk', 'trunk_link',
                 'primary', 'primary_link', 'secondary', 'secondary_link']

# Tourism types to download
TOURISM_TYPES = ['museum', 'attraction', 'viewpoint', 'artwork', 'gallery',
                 'theme_park', 'zoo', 'aquarium', 'hotel', 'motel', 'hostel',
                 'guest_house', 'camp_site', 'caravan_site', 'picnic_site']


def format_size(bytes_size: int) -> str:
    """Format bytes as human-readable size."""
    for unit in ["B", "KB", "MB", "GB"]:
        if bytes_size < 1024:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024
    return f"{bytes_size:.2f} TB"


def format_time(seconds: float) -> str:
    """Format seconds as human-readable time."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    if seconds < 3600:
        return f"{seconds / 60:.1f}m"
    return f"{seconds / 3600:.1f}h"


def build_overpass_query() -> str:
    """Build Overpass QL query for Alabama highways and tourist attractions."""
    highway_filter = "|".join(HIGHWAY_TYPES)
    tourism_filter = "|".join(TOURISM_TYPES)

    query = f"""
[out:xml][timeout:300];
area["name"="Alabama"]["admin_level"="4"]->.alabama;
(
  way(area.alabama)["highway"~"^({highway_filter})$"];
  node(area.alabama)["tourism"~"^({tourism_filter})$"];
);
(._;>;);
out meta;
"""
    return query.strip()


def download_alabama(output_path: Path, retry_count: int = 0) -> Path:
    """Download filtered Alabama OSM data using Overpass API."""
    query = build_overpass_query()

    print("Querying Overpass API for Alabama...")
    print(f"  Highways: {', '.join(HIGHWAY_TYPES[:4])}...")
    print(f"  Tourism: {', '.join(TOURISM_TYPES[:4])}...")

    encoded_query = quote(query)
    url = f"{OVERPASS_URL}?data={encoded_query}"

    try:
        req = Request(url, headers={"User-Agent": "osm_download.py/2.0"})
        start_time = time.time()

        print("Downloading (this may take a few minutes)...")
        # Overpass query timeout is 300s, connection timeout must exceed it
        response = urlopen(req, timeout=360)

        downloaded = 0
        chunk_size = 64 * 1024
        last_progress_time = start_time

        with open(output_path, "wb") as f:
            while True:
                chunk = response.read(chunk_size)
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)

                # Update progress every 0.5 seconds
                current_time = time.time()
                if current_time - last_progress_time > 0.5:
                    elapsed = current_time - start_time
                    speed = downloaded / elapsed if elapsed > 0 else 0
                    print(f"\r  Downloaded: {format_size(downloaded)} ({format_size(speed)}/s)    ", end="", flush=True)
                    last_progress_time = current_time

        elapsed = time.time() - start_time
        print("\n\nDownload complete!")
        print(f"  Size: {format_size(downloaded)}")
        print(f"  Time: {format_time(elapsed)}")
        print(f"  Saved to: {output_path}")

        return output_path

    except HTTPError as e:
        print(f"\nHTTP Error {e.code}: {e.reason}")

        if e.code == 429:
            # Rate limited
            if retry_count < MAX_RETRIES:
                print(f"Rate limited. Waiting {RETRY_DELAY} seconds before retry ({retry_count + 1}/{MAX_RETRIES})...")
                time.sleep(RETRY_DELAY)
                return download_alabama(output_path, retry_count + 1)
            else:
                print("Max retries exceeded. Please try again later.")
                sys.exit(1)

        elif e.code in [408, 504]:
            # Timeout errors
            if retry_count < MAX_RETRIES:
                print(f"Server timeout. Retrying ({retry_count + 1}/{MAX_RETRIES})...")
                time.sleep(10)
                return download_alabama(output_path, retry_count + 1)
            else:
                print("Max retries exceeded due to timeouts.")
                sys.exit(1)

        elif e.code == 400:
            print("Bad request - query may be malformed.")
            sys.exit(1)

        else:
            sys.exit(1)

    except URLError as e:
        print(f"\nURL Error: {e.reason}")

        if retry_count < MAX_RETRIES:
            print(f"Network error. Retrying in 10 seconds ({retry_count + 1}/{MAX_RETRIES})...")
            time.sleep(10)
            return download_alabama(output_path, retry_count + 1)
        else:
            print("Max retries exceeded due to network errors.")
            sys.exit(1)

    except TimeoutError:
        print("\nConnection timed out.")

        if retry_count < MAX_RETRIES:
            print(f"Retrying ({retry_count + 1}/{MAX_RETRIES})...")
            time.sleep(10)
            return download_alabama(output_path, retry_count + 1)
        else:
            print("Max retries exceeded due to timeouts.")
            sys.exit(1)


def main():
    output_dir = Path("/data/alabama")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "alabama.osm"

    download_alabama(output_path)


if __name__ == "__main__":
    main()
