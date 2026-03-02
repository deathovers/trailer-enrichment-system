import requests
import os
from fuzzywuzzy import fuzz

TMDB_API_KEY = os.getenv("TMDB_API_KEY", "YOUR_TMDB_API_KEY")
BASE_URL = "https://api.themoviedb.org/3"

def find_tmdb_id(imdb_id):
    url = f"{BASE_URL}/find/{imdb_id}"
    params = {
        "api_key": TMDB_API_KEY,
        "external_source": "imdb_id"
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        # Check movie_results or tv_results
        results = data.get("movie_results", []) or data.get("tv_results", [])
        if results:
            return results[0]
    return None

def validate_metadata(cms_title, tmdb_result):
    # 1. Title Match (Fuzzy > 80)
    tmdb_title = tmdb_result.get("title") or tmdb_result.get("name")
    ratio = fuzz.ratio(cms_title['title'].lower(), tmdb_title.lower())
    if ratio < 80:
        return False, f"Title mismatch: {cms_title['title']} vs {tmdb_title} (Ratio: {ratio})"

    # 2. Year Match (+/- 1 year)
    release_date = tmdb_result.get("release_date") or tmdb_result.get("first_air_date")
    if release_date:
        tmdb_year = int(release_date.split("-")[0])
        if abs(cms_title['year'] - tmdb_year) > 1:
            return False, f"Year mismatch: {cms_title['year']} vs {tmdb_year}"
    
    # 3. Language Match
    if cms_title['language'] != tmdb_result.get("original_language"):
        return False, f"Language mismatch: {cms_title['language']} vs {tmdb_result.get('original_language')}"

    return True, "Validated"

def get_youtube_trailer(tmdb_id, is_movie=True):
    media_type = "movie" if is_movie else "tv"
    url = f"{BASE_URL}/{media_type}/{tmdb_id}/videos"
    params = {"api_key": TMDB_API_KEY}
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        videos = response.json().get("results", [])
        # Filter: Trailer, YouTube, Official
        trailers = [v for v in videos if v['type'] == 'Trailer' and v['site'] == 'YouTube']
        official_trailers = [v for v in trailers if v.get('official') is True]
        
        target = official_trailers[0] if official_trailers else (trailers[0] if trailers else None)
        if target:
            return f"https://www.youtube.com/watch?v={target['key']}"
    return None
