import sqlite3
import json
import time
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
from pathlib import Path
import scrapetube
from tqdm import tqdm
import argparse
import os

class YouTubeChannelScraper:
    def __init__(self, db_path: str = "youtube_videos.db"):
        self.db_path = db_path
        self.setup_database()
        self.setup_logging()
        
    def setup_logging(self):
        """Setup logging to track progress and errors"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def setup_database(self):
        """Create database and tables if they don't exist"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create main videos table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS videos (
                video_id TEXT PRIMARY KEY,
                title TEXT,
                description_snippet TEXT,
                published_time TEXT,
                length_text TEXT,
                view_count_text TEXT,
                short_view_count_text TEXT,
                thumbnail_url TEXT,
                channel_verified BOOLEAN,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                raw_data TEXT  -- Store complete JSON for backup
            )
        ''')
        
        # Create progress tracking table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scraping_progress (
                channel_name TEXT PRIMARY KEY,
                last_video_id TEXT,
                total_scraped INTEGER DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'active'
            )
        ''')
        
        # Create index for faster lookups
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_video_id ON videos(video_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_scraped_at ON videos(scraped_at)')
        
        conn.commit()
        conn.close()
    
    def extract_video_data(self, video_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and flatten relevant data from video dictionary"""
        try:
            # Extract basic info
            video_id = video_dict.get('videoId', '')
            
            # Extract title
            title = ''
            if 'title' in video_dict and 'runs' in video_dict['title']:
                title = ' '.join([run.get('text', '') for run in video_dict['title']['runs']])
            
            # Extract description snippet
            description = ''
            if 'descriptionSnippet' in video_dict and 'runs' in video_dict['descriptionSnippet']:
                description = ' '.join([run.get('text', '') for run in video_dict['descriptionSnippet']['runs']])
            
            # Extract other fields
            published_time = video_dict.get('publishedTimeText', {}).get('simpleText', '')
            length_text = video_dict.get('lengthText', {}).get('simpleText', '')
            view_count = video_dict.get('viewCountText', {}).get('simpleText', '')
            short_view_count = video_dict.get('shortViewCountText', {}).get('simpleText', '')
            
            # Extract thumbnail URL (highest quality available)
            thumbnail_url = ''
            if 'thumbnail' in video_dict and 'thumbnails' in video_dict['thumbnail']:
                thumbnails = video_dict['thumbnail']['thumbnails']
                if thumbnails:
                    # Get the highest resolution thumbnail
                    thumbnail_url = max(thumbnails, key=lambda x: x.get('width', 0) * x.get('height', 0)).get('url', '')
            
            # Check if channel is verified
            channel_verified = False
            if 'ownerBadges' in video_dict:
                for badge in video_dict['ownerBadges']:
                    if badge.get('metadataBadgeRenderer', {}).get('style') == 'BADGE_STYLE_TYPE_VERIFIED':
                        channel_verified = True
                        break
            
            return {
                'video_id': video_id,
                'title': title,
                'description_snippet': description,
                'published_time': published_time,
                'length_text': length_text,
                'view_count_text': view_count,
                'short_view_count_text': short_view_count,
                'thumbnail_url': thumbnail_url,
                'channel_verified': channel_verified,
                'raw_data': json.dumps(video_dict)  # Store complete data as backup
            }
        except Exception as e:
            self.logger.error(f"Error extracting data from video dict: {e}")
            return {
                'video_id': video_dict.get('videoId', ''),
                'raw_data': json.dumps(video_dict)
            }
    
    def save_video(self, video_data: Dict[str, Any]) -> bool:
        """Save video data to database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO videos 
                (video_id, title, description_snippet, published_time, length_text, 
                 view_count_text, short_view_count_text, thumbnail_url, channel_verified, raw_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                video_data['video_id'],
                video_data.get('title', ''),
                video_data.get('description_snippet', ''),
                video_data.get('published_time', ''),
                video_data.get('length_text', ''),
                video_data.get('view_count_text', ''),
                video_data.get('short_view_count_text', ''),
                video_data.get('thumbnail_url', ''),
                video_data.get('channel_verified', False),
                video_data.get('raw_data', '')
            ))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            self.logger.error(f"Error saving video {video_data.get('video_id', 'unknown')}: {e}")
            return False
    
    def update_progress(self, channel_name: str, last_video_id: str, total_scraped: int):
        """Update scraping progress"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO scraping_progress 
            (channel_name, last_video_id, total_scraped, last_updated)
            VALUES (?, ?, ?, ?)
        ''', (channel_name, last_video_id, total_scraped, datetime.now()))
        
        conn.commit()
        conn.close()
    
    def get_progress(self, channel_name: str) -> Dict[str, Any]:
        """Get current scraping progress"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT last_video_id, total_scraped, last_updated 
            FROM scraping_progress 
            WHERE channel_name = ?
        ''', (channel_name,))
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return {
                'last_video_id': result[0],
                'total_scraped': result[1],
                'last_updated': result[2]
            }
        return {'last_video_id': None, 'total_scraped': 0, 'last_updated': None}
    
    def is_video_exists(self, video_id: str) -> bool:
        """Check if video already exists in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT 1 FROM videos WHERE video_id = ?', (video_id,))
        exists = cursor.fetchone() is not None
        
        conn.close()
        return exists
    
    def scrape_channel(self, channel_username: str, estimated_total: int = 237000, 
                      sleep_interval: float = 0.5, batch_size: int = 100):
        """
        Scrape all videos from a YouTube channel
        
        Args:
            channel_username: YouTube channel username (without @)
            estimated_total: Estimated total number of videos for progress bar
            sleep_interval: Sleep time between requests to avoid rate limiting
            batch_size: Number of videos to process before updating progress
        """
        self.logger.info(f"Starting to scrape channel: {channel_username}")
        
        # Get previous progress
        progress = self.get_progress(channel_username)
        start_count = progress['total_scraped']
        last_video_id = progress['last_video_id']
        
        self.logger.info(f"Resuming from video count: {start_count}")
        if last_video_id:
            self.logger.info(f"Resuming from video ID: {last_video_id}")
        
        # Initialize progress bar
        pbar = tqdm(
            total=estimated_total,
            initial=start_count,
            desc=f"Scraping {channel_username}",
            unit="videos"
        )
        
        try:
            # Get video generator with pagination
            videos = scrapetube.get_channel(
                channel_username=channel_username,
                sort_by="newest",  # Ensure consistent ordering
                limit=None  # No limit on total videos
            )
            
            processed_count = 0
            skipped_count = 0
            saved_count = start_count
            batch_videos = []
            found_last_video = not last_video_id  # If no last_video_id, we start from beginning
            
            # Process videos in batches to handle pagination more efficiently
            current_batch = []
            for video in videos:
                video_id = video.get('videoId', '')
                
                # If we haven't found our last video yet, keep searching
                if not found_last_video:
                    if video_id == last_video_id:
                        found_last_video = True
                        self.logger.info(f"Found last processed video, continuing with new videos")
                    continue
                
                # Skip if we've already processed this video
                if self.is_video_exists(video_id):
                    skipped_count += 1
                    pbar.set_postfix({
                        'Saved': saved_count,
                        'Skipped': skipped_count,
                        'Current': video_id[:11]
                    })
                    continue
                
                # Process new video
                video_data = self.extract_video_data(video)
                if self.save_video(video_data):
                    saved_count += 1
                    processed_count += 1
                    batch_videos.append(video_id)
                    current_batch.append(video_id)
                
                # Update progress bar
                pbar.update(1)
                pbar.set_postfix({
                    'Saved': saved_count,
                    'Skipped': skipped_count,
                    'Current': video_id[:11]
                })
                
                # Update progress in database periodically
                if processed_count % batch_size == 0:
                    self.update_progress(channel_username, video_id, saved_count)
                    self.logger.info(f"Processed batch of {len(current_batch)} videos. "
                                   f"Last video ID: {video_id}")
                    current_batch = []
                    time.sleep(sleep_interval)  # Rate limiting
                
        except Exception as e:
            self.logger.error(f"Error during scraping: {e}")
            # Update progress even if there's an error
            if batch_videos:
                self.update_progress(channel_username, batch_videos[-1], saved_count)
                self.logger.info(f"Saved progress after error. Last video ID: {batch_videos[-1]}")
        finally:
            pbar.close()
            self.logger.info(f"Finished scraping {channel_username}. "
                           f"Saved: {saved_count}, Skipped: {skipped_count}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM videos')
        total_videos = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM videos WHERE channel_verified = 1')
        verified_videos = cursor.fetchone()[0]
        
        cursor.execute('SELECT MIN(scraped_at), MAX(scraped_at) FROM videos')
        date_range = cursor.fetchone()
        
        conn.close()
        
        return {
            'total_videos': total_videos,
            'verified_channel_videos': verified_videos,
            'first_scraped': date_range[0],
            'last_scraped': date_range[1]
        }
    
    def export_to_csv(self, output_file: str = "youtube_videos.csv"):
        """Export data to CSV file"""
        import pandas as pd
        
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query('''
            SELECT video_id, title, description_snippet, published_time, 
                   length_text, view_count_text, short_view_count_text, 
                   thumbnail_url, channel_verified, scraped_at
            FROM videos 
            ORDER BY scraped_at DESC
        ''', conn)
        
        df.to_csv(output_file, index=False)
        conn.close()
        
        self.logger.info(f"Data exported to {output_file}")

# --------------------
# Helper utilities
# --------------------
def _sanitize_channel_username(raw: str) -> str:
    """Normalize channel username by trimming whitespace and removing a leading '@'."""
    if raw is None:
        return ""
    value = raw.strip()
    if value.startswith('@'):
        value = value[1:]
    return value


def load_channels_from_file(file_path: str) -> List[str]:
    """Load channel usernames from a text file (one per line).

    - Ignores blank lines and lines starting with '#'
    - Trims whitespace and removes a leading '@'
    """
    channels: List[str] = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            stripped = line.strip()
            if not stripped or stripped.startswith('#'):
                continue
            username = _sanitize_channel_username(stripped)
            if username:
                channels.append(username)
    return channels

# Usage example
if __name__ == "__main__":
    # Initialize scraper
    
    parser = argparse.ArgumentParser(description="Youtube Channel Scraper")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("channel_name", nargs='?', help="Name of the Youtube channel to be scraped (without @)")
    group.add_argument("--channels_file", help="Path to a .txt file of channel usernames (one per line)")
    #parser.add_argument("--db_suffix", default = 'channel_videos', help="the suffix to be added to the channel name to create the name of the database")
    parser.add_argument("--save_path", default = '/mnt/hdd3/YT-channels', help="the location where channel databases are stored")
    parser.add_argument("--estimated_total", type=int, default=500000, help="Estimated total videos per channel for progress bar")
    parser.add_argument("--sleep_interval", type=float, default=1.0, help="Sleep time between batches to avoid rate limiting")
    parser.add_argument("--batch_size", type=int, default=50, help="Number of videos per progress save batch")
    parser.add_argument("--skip_existing_db", action='store_true', help="Skip channels whose DB already exists")

    args = parser.parse_args()
    
    # Determine channels to scrape (single or multiple)
    if args.channels_file:
        channels = load_channels_from_file(args.channels_file)
    else:
        channels = [_sanitize_channel_username(args.channel_name)]

    for channel in tqdm(set(channels)):
        if not channel:
            continue
        channel_dir = os.path.join(args.save_path, channel)
        os.makedirs(channel_dir, exist_ok=True)

        db_file = f"{channel_dir}/{channel}.db"
        if args.skip_existing_db and os.path.exists(db_file):
            print(f"Skipping {channel} (existing DB found: {db_file})")
            continue

        scraper = YouTubeChannelScraper(db_file)

        # Start scraping Youtube channel
        scraper.scrape_channel(
            channel_username=channel,
            estimated_total=args.estimated_total,
            sleep_interval=args.sleep_interval,
            batch_size=args.batch_size
        )

        # Get statistics
        stats = scraper.get_stats()
        print("\nScraping Statistics (channel: {}):".format(channel))
        for key, value in stats.items():
            print(f"{key}: {value}")

        # Export to CSV
        scraper.export_to_csv(f"{channel_dir}/{channel}.csv")
