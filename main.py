import logging
import os
from dotenv import load_dotenv
from utils.db import init_db, get_pending_titles, update_log, update_cms_trailer
from services.mapping import find_tmdb_id, validate_metadata, get_youtube_trailer
from services.transcoding import trigger_transcoding, poll_transcoding_status

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

def process_titles():
    titles = get_pending_titles()
    logger.info(f"Found {len(titles)} titles pending trailer enrichment.")

    for title in titles:
        content_id = title['id']
        imdb_id = title['imdb_id']
        logger.info(f"Processing: {title['title']} ({imdb_id})")

        try:
            # 1. Mapping
            tmdb_result = find_tmdb_id(imdb_id)
            if not tmdb_result:
                logger.warning(f"TMDB ID not found for {imdb_id}")
                update_log(content_id, imdb_id, None, 'NOT_FOUND', "TMDB ID not found")
                continue

            tmdb_id = tmdb_result['id']
            
            # 2. Validation
            is_valid, message = validate_metadata(title, tmdb_result)
            if not is_valid:
                logger.warning(f"Validation failed for {title['title']}: {message}")
                update_log(content_id, imdb_id, tmdb_id, 'NOT_FOUND', message)
                continue

            # 3. Extraction
            youtube_url = get_youtube_trailer(tmdb_id)
            if not youtube_url:
                logger.warning(f"No YouTube trailer found for TMDB ID {tmdb_id}")
                update_log(content_id, imdb_id, tmdb_id, 'NOT_FOUND', "No YouTube trailer found")
                continue

            # 4. Transcoding
            update_log(content_id, imdb_id, tmdb_id, 'TRANSCODING')
            job_id = trigger_transcoding(youtube_url)
            hls_url = poll_transcoding_status(job_id)

            # 5. CMS Update
            if hls_url:
                update_cms_trailer(content_id, hls_url)
                update_log(content_id, imdb_id, tmdb_id, 'COMPLETED')
                logger.info(f"Successfully enriched trailer for {title['title']}")
            else:
                update_log(content_id, imdb_id, tmdb_id, 'FAILED', "Transcoding failed")

        except Exception as e:
            logger.error(f"Error processing {title['title']}: {str(e)}")
            update_log(content_id, imdb_id, None, 'FAILED', str(e))

if __name__ == "__main__":
    init_db()
    process_titles()
