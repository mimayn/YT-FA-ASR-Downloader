import sqlite3
import json
import time
import logging
import os
import subprocess
import random
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
from pathlib import Path
import scrapetube
from tqdm import tqdm
import argparse
import concurrent.futures
from urllib.parse import urlparse

class YouTubeChannelScraper:
    def __init__(self, db_path: str = "youtube_videos.db", 
                 subtitles_dir: str = "subtitles",
                 media_dir: str = "media", 
                 download_dir: str = None,
                 channel_name: str = None):
        
        self.db_path = db_path
        self.subtitles_dir = Path(subtitles_dir)
        self.download_dir = Path(download_dir)
        self.media_dir = Path(media_dir)
        self.channel_name = channel_name
            
        self.setup_logging()         # <-- Move this line up
        self.setup_database()
        self._check_dependencies()
        
    def setup_logging(self):
        """Setup logging to track progress and errors"""
        # Determine log file location
        if self.download_dir and self.channel_name:
            log_file = self.download_dir / f"{self.channel_name}_scraper.log"
        else:
            log_file = "scraper.log"  # Fallback to current directory
        
        # Create the directory if it doesn't exist
        if isinstance(log_file, Path):
            log_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Clear any existing handlers to avoid duplicate logs
        logger = logging.getLogger(__name__)
        logger.handlers.clear()
        
        # Setup new logging configuration
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(str(log_file)),
                logging.StreamHandler()
            ],
            force=True  # Force reconfiguration
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Logging to: {log_file}")
    
    def _check_dependencies(self):
        """Check if required dependencies are available"""
        try:
            result = subprocess.run(['yt-dlp', '--version'], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                self.logger.info(f"yt-dlp found: {result.stdout.strip()}")
            else:
                raise Exception("yt-dlp not working properly")
        except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as e:
            self.logger.error("yt-dlp is not available. Please install it: pip install yt-dlp")
            raise Exception("yt-dlp dependency missing") from e
    
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
                                 channel_name TEXT,
                 subtitle_path TEXT,
                 subtitle_type TEXT,  -- 'manual', 'auto', 'both', or 'none'
                 auto_subtitle_path TEXT,
                 subtitle_languages TEXT,  -- JSON array of available languages
                 audio_path TEXT,  -- Path to downloaded audio file
                 video_path TEXT,  -- Path to downloaded video file
                 download_status TEXT,  -- 'pending', 'downloading', 'completed', 'failed', 'skipped'
                 file_size_mb REAL,  -- File size in MB
                 scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                 subtitle_downloaded_at TIMESTAMP,
                 downloaded_at TIMESTAMP,
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
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_channel_name ON videos(channel_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_subtitle_type ON videos(subtitle_type)')
        
        # Migrate existing database if needed
        self._migrate_database(cursor)
        
        conn.commit()
        conn.close()
    
    def _migrate_database(self, cursor):
        """Add new columns to existing database if they don't exist"""
        # Get existing columns
        cursor.execute("PRAGMA table_info(videos)")
        existing_columns = {row[1] for row in cursor.fetchall()}
        
        # Add missing columns
        new_columns = {
            'channel_name': 'TEXT',
            'subtitle_path': 'TEXT',
            'subtitle_type': 'TEXT',
            'auto_subtitle_path': 'TEXT',
            'subtitle_languages': 'TEXT',
            'audio_path': 'TEXT',
            'video_path': 'TEXT',
            'download_status': 'TEXT',
            'file_size_mb': 'REAL',
            'subtitle_downloaded_at': 'TIMESTAMP',
            'downloaded_at': 'TIMESTAMP',
            # New granular completion tracking columns
            'metadata_completed': 'BOOLEAN DEFAULT 0',
            'subtitles_completed': 'TEXT',  # JSON: {"fa": "completed", "en": "failed", etc.}
            'media_completed': 'BOOLEAN DEFAULT 0',
            'processing_status': 'TEXT DEFAULT "pending"',  # 'pending', 'partial', 'completed', 'failed'
            'last_processing_step': 'TEXT',  # Last step that was attempted
            'completion_details': 'TEXT'  # JSON with detailed completion info
        }
        
        for column_name, column_type in new_columns.items():
            if column_name not in existing_columns:
                try:
                    cursor.execute(f'ALTER TABLE videos ADD COLUMN {column_name} {column_type}')
                    self.logger.info(f"Added column {column_name} to existing database")
                except sqlite3.OperationalError as e:
                    self.logger.warning(f"Could not add column {column_name}: {e}")
    
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
    
    def download_subtitles(self, video_id: str, languages: list = ['fa'], max_retries: int = 3, quick_check: bool = True) -> Tuple[Optional[str], Optional[str], str, Dict[str, str]]:
        """
        Download subtitles for a video
        
        Returns:
            Tuple of (manual_subtitle_path, auto_subtitle_path, subtitle_type, language_results)
            where language_results is a dict of {language: 'completed'|'failed'|'not_available'}
        """
        url = f"https://www.youtube.com/watch?v={video_id}"
        manual_path = None
        auto_path = None
        subtitle_type = "none"
        language_results = {}
        
        # Skip quick check - it's causing auth issues, just try downloading directly
        
        for language in languages:
            # Create language-specific directory
            lang_dir = self.subtitles_dir / language
            lang_dir.mkdir(exist_ok=True)
            
            # Try manual subtitles first
            manual_file = lang_dir / f"{video_id}_manual.srt"
            auto_file = lang_dir / f"{video_id}_auto.srt"
            
            lang_manual_success = False
            lang_auto_success = False
            
            for attempt in range(max_retries):
                try:
                    # Download manual subtitles (using working approach from bash script)
                    if not manual_file.exists():
                        cmd_manual = [
                            'yt-dlp',
                            '--cookies-from-browser', 'firefox',
                            '--write-subs',
                            '--sub-lang', language,
                            '--sub-format', 'srt',
                            '--skip-download',
                            '-o', str(lang_dir / f"{video_id}_manual.%(ext)s"),
                            url
                        ]
                        
                        result = subprocess.run(cmd_manual, capture_output=True, text=True, timeout=60)
                        if result.returncode == 0 and manual_file.exists() and manual_file.stat().st_size > 0:
                            if not manual_path:  # Only set once for first successful language
                                manual_path = str(manual_file.relative_to(Path.cwd()))
                                subtitle_type = "manual"
                            lang_manual_success = True
                            self.logger.info(f"Downloaded manual subtitles for {video_id} in {language}")
                        elif manual_file.exists() and manual_file.stat().st_size == 0:
                            manual_file.unlink()  # Remove empty files
                        else:
                            # Check for auth errors in subtitle download
                            if self._is_auth_or_bot_error(result.stderr):
                                self.logger.warning(f"Authentication error in subtitle download for {video_id}: {result.stderr}")
                                # Don't immediately fail, try auto subtitles first
                    
                    # Download auto-generated subtitles (using working approach from bash script)
                    if not auto_file.exists():
                        cmd_auto = [
                            'yt-dlp',
                            '--cookies-from-browser', 'firefox',
                            '--write-auto-subs',
                            '--sub-lang', language,
                            '--sub-format', 'srt',
                            '--skip-download',
                            '-o', str(lang_dir / f"{video_id}_auto.%(ext)s"),
                            url
                        ]
                        
                        result = subprocess.run(cmd_auto, capture_output=True, text=True, timeout=60)
                        if result.returncode == 0 and auto_file.exists() and auto_file.stat().st_size > 0:
                            if not auto_path:  # Only set once for first successful language
                                auto_path = str(auto_file.relative_to(Path.cwd()))
                                if subtitle_type == "manual":
                                    subtitle_type = "both"
                                elif subtitle_type == "none":
                                    subtitle_type = "auto"
                            lang_auto_success = True
                            self.logger.info(f"Downloaded auto subtitles for {video_id} in {language}")
                        elif auto_file.exists() and auto_file.stat().st_size == 0:
                            auto_file.unlink()  # Remove empty files
                    
                    # If we got at least one type for this language, break the retry loop
                    if lang_manual_success or lang_auto_success:
                        break
                        
                except subprocess.TimeoutExpired:
                    self.logger.warning(f"Subtitle download timeout for {video_id} in {language}, attempt {attempt + 1}")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                except Exception as e:
                    self.logger.error(f"Error downloading subtitles for {video_id} in {language}: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
            
            # Record result for this language
            if lang_manual_success or lang_auto_success:
                language_results[language] = 'completed'
            else:
                language_results[language] = 'not_available'
        
        return manual_path, auto_path, subtitle_type, language_results

    def _is_auth_or_bot_error(self, error_msg: str) -> bool:
        """Check if error is related to authentication or bot detection"""
        auth_indicators = [
            'sign in to confirm',
            'not a bot',
            'cookies',
            'authentication',
            'captcha',
            'bot detection',
            'verify you are human',
            'too many requests',
            'rate limit',
            'blocked',
            'forbidden'
        ]
        return any(indicator in error_msg.lower() for indicator in auth_indicators)
    
    def _quick_video_check(self, video_id: str) -> bool:
        """
        Quick check if video exists without downloading (inspired by bash script)
        Returns True if video exists, False if definitely deleted/unavailable
        """
        url = f"https://www.youtube.com/watch?v={video_id}"
        
        try:
            # Use yt-dlp to just extract info without downloading (with cookies like working script)
            cmd = ['yt-dlp', '--cookies-from-browser', 'firefox', '--quiet', '--skip-download', '--print', 'title', url]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            
            if result.returncode == 0:
                return True  # Video exists
            else:
                # Check if it's a definitive deletion error
                error_output = result.stderr.lower()
                deletion_indicators = ['unavailable', 'removed', 'deleted', 'private', 'not found']
                if any(indicator in error_output for indicator in deletion_indicators):
                    self.logger.info(f"Video {video_id} is definitively deleted/unavailable")
                    return False  # Video deleted
                else:
                    self.logger.debug(f"Temporary error for {video_id}, assuming it exists")
                    return True  # Assume temporary error, video exists
                    
        except subprocess.TimeoutExpired:
            self.logger.warning(f"Quick check timeout for {video_id}, assuming it exists")
            return True
        except Exception as e:
            self.logger.warning(f"Quick check failed for {video_id}: {e}, assuming it exists")
            return True

    def _try_alternative_download_methods(self, video_id: str, audio_only: bool = True) -> Tuple[Optional[str], str, float]:
        """Try alternative download methods when facing auth/bot errors"""
        url = f"https://www.youtube.com/watch?v={video_id}"
        
        # Progressive backoff delays like the bash script
        methods = [
            {
                'name': 'Firefox cookies + best audio',
                'cmd_args': [
                    '--cookies-from-browser', 'firefox',
                    '--extract-audio' if audio_only else '--format', 'bestaudio[ext=m4a]' if audio_only else 'best[height<=720]',
                    '--audio-format', 'mp3' if audio_only else None,
                    '--audio-quality', '0' if audio_only else None,
                ],
                'timeout': 120,
                'sleep_after': 30
            },
            {
                'name': 'Chrome cookies + lower quality',
                'cmd_args': [
                    '--cookies-from-browser', 'chrome',
                    '--extract-audio' if audio_only else '--format', 'bestaudio' if audio_only else 'best[height<=480]',
                    '--audio-format', 'mp3' if audio_only else None,
                    '--audio-quality', '5' if audio_only else None,  # Lower quality
                ],
                'timeout': 120,
                'sleep_after': 60
            },
            {
                'name': 'No cookies + delays + worst quality',
                'cmd_args': [
                    '--user-agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    '--sleep-requests', '3',
                    '--sleep-interval', '10',
                    '--max-sleep-interval', '20',
                    '--extract-audio' if audio_only else '--format', 'worst' if not audio_only else 'bestaudio',
                    '--audio-format', 'mp3' if audio_only else None,
                ],
                'timeout': 300,
                'sleep_after': 0
            }
        ]
        
        for i, method in enumerate(methods, 1):
            self.logger.info(f"Trying alternative method {i} for {video_id}: {method['name']}")
            try:
                cmd = [
                    'yt-dlp',
                    '--no-warnings',
                    '--socket-timeout', '60',
                    '--retries', '1',
                    '--paths', str(self.media_dir),
                    '-o', f"{video_id}.%(ext)s"
                ] + [arg for arg in method['cmd_args'] if arg is not None] + [url]
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=method['timeout'])
                if result.returncode == 0:
                    downloaded_files = list(self.media_dir.glob(f"{video_id}.*"))
                    if downloaded_files:
                        largest_file = max(downloaded_files, key=lambda f: f.stat().st_size)
                        file_size_mb = largest_file.stat().st_size / (1024 * 1024)
                        if file_size_mb > 1.0:
                            self.logger.info(f"✅ Method {i} successful for {video_id}")
                            return str(largest_file), "completed", file_size_mb
                
                # If this method failed but we have more methods, sleep before trying next
                if i < len(methods) and method['sleep_after'] > 0:
                    self.logger.info(f"Method {i} failed, waiting {method['sleep_after']}s before next attempt...")
                    time.sleep(method['sleep_after'])
                    
            except subprocess.TimeoutExpired:
                self.logger.warning(f"Method {i} timeout for {video_id}")
            except Exception as e:
                self.logger.warning(f"Method {i} failed for {video_id}: {e}")
        
        return None, "auth_failed", 0.0

    def download_media(self, video_id: str, audio_only: bool = True, max_retries: int = 3) -> Tuple[Optional[str], str, float]:
        """
        Download video or audio file
        
        Returns:
            Tuple of (file_path, download_status, file_size_mb)
        """
        if not self.download_dir:
            return None, "skipped", 0.0
            
        url = f"https://www.youtube.com/watch?v={video_id}"
        file_path = None
        download_status = "failed"
        file_size_mb = 0.0
        
        for attempt in range(max_retries):
            try:
                if audio_only:
                    # Simplified format selection - let yt-dlp choose best available audio
                    cmd = [
                        'yt-dlp',
                        '--cookies-from-browser', 'firefox',
                        '-x', '--audio-format', 'mp3',
                        '--audio-quality', '0',
                        '-o', str(self.media_dir / f"{video_id}.%(ext)s"),
                        url
                    ]
                else:
                    # Video download with cookies
                    cmd = [
                        'yt-dlp',
                        '--cookies-from-browser', 'firefox',
                        '--format', 'best[height<=720]',
                        '-o', str(self.media_dir / f"{video_id}.%(ext)s"),
                        url
                    ]
                
                # Check if file already exists (ignore incomplete files)
                existing_files = list(self.media_dir.glob(f"{video_id}.*"))
                complete_files = []
                incomplete_files = []
                
                for file_path in existing_files:
                    # Separate complete from incomplete files
                    if file_path.suffix in ['.part', '.ytdl', '.temp']:
                        incomplete_files.append(file_path)
                    elif file_path.stat().st_size > 1024 * 1024:  # > 1MB
                        complete_files.append(file_path)
                    else:
                        incomplete_files.append(file_path)  # Small files are likely corrupted
                
                # Clean up incomplete files
                for incomplete_file in incomplete_files:
                    try:
                        incomplete_file.unlink()
                        self.logger.info(f"Cleaned up incomplete file: {incomplete_file.name}")
                    except Exception as e:
                        self.logger.warning(f"Could not clean up {incomplete_file.name}: {e}")
                
                # If we have a complete file, use it
                if complete_files:
                    largest_file = max(complete_files, key=lambda f: f.stat().st_size)
                    file_path = str(largest_file)
                    file_size_mb = largest_file.stat().st_size / (1024 * 1024)
                    download_status = "completed"
                    self.logger.info(f"Found existing complete file for {video_id}: {file_path} ({file_size_mb:.1f}MB)")
                    break
                
                # Download the file
                self.logger.info(f"Attempting {'audio' if audio_only else 'video'} download for {video_id}, attempt {attempt + 1}")
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
                
                if result.returncode == 0:
                    # Find the downloaded file
                    downloaded_files = list(self.media_dir.glob(f"{video_id}.*"))
                    if downloaded_files:
                        largest_file = max(downloaded_files, key=lambda f: f.stat().st_size)
                        file_size_mb = largest_file.stat().st_size / (1024 * 1024)
                        
                        # Check if file is substantial (> 1MB)
                        if file_size_mb > 1.0:
                            file_path = str(largest_file)
                            download_status = "completed"
                            self.logger.info(f"Downloaded {'audio' if audio_only else 'video'} for {video_id}: {file_path} ({file_size_mb:.1f}MB)")
                            break
                        else:
                            self.logger.warning(f"Downloaded file for {video_id} is too small ({file_size_mb:.1f}MB), likely corrupted")
                            largest_file.unlink()  # Remove corrupted file
                            download_status = "corrupted"
                    else:
                        self.logger.warning(f"Download reported success for {video_id} but no files found")
                        download_status = "missing_file"
                else:
                    error_msg = result.stderr.strip() if result.stderr else "Unknown error"
                    self.logger.warning(f"Download failed for {video_id}, attempt {attempt + 1}: {error_msg}")
                    
                    # Check for authentication/bot detection errors
                    if self._is_auth_or_bot_error(error_msg):
                        self.logger.error(f"🚨 AUTHENTICATION/BOT DETECTION ERROR for {video_id}")
                        self.logger.error(f"Error: {error_msg}")
                        self.logger.info(f"🔄 Trying alternative download methods...")
                        
                        # Try alternative methods
                        alt_file_path, alt_status, alt_size = self._try_alternative_download_methods(video_id, audio_only)
                        if alt_status == "completed":
                            self.logger.info(f"✅ Successfully downloaded {video_id} using alternative method")
                            return alt_file_path, alt_status, alt_size
                        else:
                            self.logger.error(f"❌ All alternative methods failed for {video_id}")
                            self.logger.error("🛑 STOPPING SCRIPT - Authentication issue needs manual intervention")
                            self.logger.error("POSSIBLE SOLUTIONS:")
                            self.logger.error("1. Open YouTube in your browser and solve any captcha")
                            self.logger.error("2. Clear browser cookies and login again")
                            self.logger.error("3. Use a VPN to change IP address")
                            self.logger.error("4. Wait a few hours and try again")
                            self.logger.error("5. Export cookies manually: https://github.com/yt-dlp/yt-dlp/wiki/FAQ#how-do-i-pass-cookies-to-yt-dlp")
                            raise Exception(f"Authentication/bot detection error - manual intervention required")
                    
                    # Check for specific error types to avoid retries
                    if any(err in error_msg.lower() for err in ['unavailable', 'private', 'deleted', 'removed']):
                        self.logger.info(f"Video {video_id} appears to be unavailable/deleted - not retrying")
                        download_status = "unavailable"
                        break
                    
            except subprocess.TimeoutExpired:
                self.logger.warning(f"Download timeout for {video_id}, attempt {attempt + 1}")
                download_status = "timeout"
            except Exception as e:
                self.logger.error(f"Error downloading {video_id}, attempt {attempt + 1}: {e}")
                download_status = "error"
            
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
        
        return file_path, download_status, file_size_mb

    def save_video(self, video_data: Dict[str, Any], channel_name: str = None) -> bool:
        """Save video data to database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO videos 
                (video_id, title, description_snippet, published_time, length_text, 
                 view_count_text, short_view_count_text, thumbnail_url, channel_verified, 
                 channel_name, subtitle_path, subtitle_type, auto_subtitle_path, 
                 subtitle_languages, audio_path, video_path, download_status, file_size_mb,
                 subtitle_downloaded_at, downloaded_at, raw_data, metadata_completed, 
                 subtitles_completed, media_completed, processing_status, last_processing_step, 
                 completion_details)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                channel_name,
                video_data.get('subtitle_path'),
                video_data.get('subtitle_type'),
                video_data.get('auto_subtitle_path'),
                video_data.get('subtitle_languages'),
                video_data.get('audio_path'),
                video_data.get('video_path'),
                video_data.get('download_status', 'pending'),
                video_data.get('file_size_mb'),
                video_data.get('subtitle_downloaded_at'),
                video_data.get('downloaded_at'),
                video_data.get('raw_data', ''),
                video_data.get('metadata_completed', False),
                video_data.get('subtitles_completed'),
                video_data.get('media_completed', False),
                video_data.get('processing_status', 'pending'),
                video_data.get('last_processing_step'),
                video_data.get('completion_details')
            ))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            self.logger.error(f"Error saving video {video_data.get('video_id', 'unknown')}: {e}")
            return False
    
    def update_progress(self, identifier: str, last_video_id: str, total_scraped: int):
        """Update scraping progress for channel or playlist"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO scraping_progress 
            (channel_name, last_video_id, total_scraped, last_updated)
            VALUES (?, ?, ?, ?)
        ''', (identifier, last_video_id, total_scraped, datetime.now()))
        
        conn.commit()
        conn.close()
    
    def get_progress(self, identifier: str) -> Dict[str, Any]:
        """Get current scraping progress for channel or playlist"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            self.logger.debug(f"Querying progress for identifier: '{identifier}'")
            cursor.execute('''
                SELECT last_video_id, total_scraped, last_updated 
                FROM scraping_progress 
                WHERE channel_name = ?
            ''', (identifier,))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                self.logger.debug(f"Found existing progress: {result}")
                return {
                    'last_video_id': result[0],
                    'total_scraped': result[1],
                    'last_updated': result[2]
                }
            else:
                self.logger.debug(f"No existing progress found for '{identifier}', returning defaults")
                return {'last_video_id': None, 'total_scraped': 0, 'last_updated': None}
        except Exception as e:
            self.logger.error(f"Error in get_progress for '{identifier}': {e}")
            return {'last_video_id': None, 'total_scraped': 0, 'last_updated': None}
    
    def is_video_exists(self, video_id: str) -> bool:
        """Check if video already exists in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT 1 FROM videos WHERE video_id = ?', (video_id,))
        exists = cursor.fetchone() is not None
        
        conn.close()
        return exists
    
    def get_video_completion_status(self, video_id: str, 
                                  download_subtitles: bool = True, 
                                  subtitle_languages: list = ['fa', 'en'],
                                  download_media: bool = False) -> Dict[str, Any]:
        """
        Get detailed completion status for a video.
        Returns what steps need to be completed.
        
        Returns:
            {
                'exists': bool,
                'needs_metadata': bool,
                'needs_subtitles': bool,
                'missing_subtitle_languages': list,
                'needs_media': bool,
                'is_fully_completed': bool,
                'processing_status': str,
                'last_step': str
            }
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT metadata_completed, subtitles_completed, media_completed, 
                   processing_status, last_processing_step, subtitle_languages,
                   audio_path, video_path, subtitle_type
            FROM videos WHERE video_id = ?
        ''', (video_id,))
        
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            # Video doesn't exist at all
            return {
                'exists': False,
                'needs_metadata': True,
                'needs_subtitles': download_subtitles,
                'missing_subtitle_languages': subtitle_languages if download_subtitles else [],
                'needs_media': download_media,
                'is_fully_completed': False,
                'processing_status': 'not_started',
                'last_step': None
            }
        
        (metadata_completed, subtitles_completed_json, media_completed, 
         processing_status, last_step, stored_subtitle_langs, 
         audio_path, video_path, subtitle_type) = result
        
        # Parse subtitle completion status
        subtitles_completed = {}
        if subtitles_completed_json:
            try:
                subtitles_completed = json.loads(subtitles_completed_json)
            except:
                subtitles_completed = {}
        
        # Check what's needed
        needs_metadata = not metadata_completed
        
        # Check subtitle needs by ALWAYS verifying actual files exist on disk (like media files)
        needs_subtitles = False
        missing_subtitle_languages = []
        if download_subtitles:
            for lang in subtitle_languages:
                lang_status = subtitles_completed.get(lang)
                
                # If marked as not_available, skip (don't retry)
                if lang_status == 'not_available':
                    continue
                
                # ALWAYS check if subtitle files actually exist on disk
                lang_dir = self.subtitles_dir / lang
                manual_file = lang_dir / f"{video_id}_manual.srt"
                auto_file = lang_dir / f"{video_id}_auto.srt"
                
                files_exist = False
                try:
                    if ((manual_file.exists() and manual_file.stat().st_size > 0) or 
                        (auto_file.exists() and auto_file.stat().st_size > 0)):
                        files_exist = True
                except (OSError, FileNotFoundError):
                    files_exist = False
                
                # If files don't exist, need to download regardless of DB status
                if not files_exist:
                    needs_subtitles = True
                    missing_subtitle_languages.append(lang)
                    # Update DB if it was marked as completed but files are missing
                    if lang_status == 'completed':
                        self.logger.warning(f"Video {video_id} subtitle {lang} marked completed but files missing - will re-download")
                        self.update_video_completion_status(video_id, subtitles_completed={lang: 'missing'}, processing_status='partial')
        
        # Check media needs by verifying actual files exist (ignore incomplete files)
        needs_media = False
        if download_media:
            # ALWAYS check if substantial media files actually exist on disk
            existing_files = list(self.media_dir.glob(f"{video_id}.*"))
            has_substantial_file = False
            
            for file_path in existing_files:
                try:
                    # Skip incomplete files
                    if file_path.suffix in ['.part', '.ytdl', '.temp']:
                        continue
                    if file_path.exists() and file_path.stat().st_size > 1024 * 1024:  # > 1MB
                        has_substantial_file = True
                        break
                except (OSError, FileNotFoundError):
                    # File was deleted or corrupted
                    continue
            
            if not has_substantial_file:
                needs_media = True
                # If database says completed but no substantial files exist, update database
                if media_completed:
                    self.logger.warning(f"Video {video_id} marked as completed but no substantial files found - will re-download")
                    self.update_video_completion_status(video_id, media_completed=False, processing_status='partial')
                self.logger.debug(f"Video {video_id} needs media download - no substantial files found")
        
        is_fully_completed = (
            not needs_metadata and 
            not needs_subtitles and 
            not needs_media
        )
        
        return {
            'exists': True,
            'needs_metadata': needs_metadata,
            'needs_subtitles': needs_subtitles,
            'missing_subtitle_languages': missing_subtitle_languages,
            'needs_media': needs_media,
            'is_fully_completed': is_fully_completed,
            'processing_status': processing_status or 'unknown',
            'last_step': last_step
        }
    
    def update_video_completion_status(self, video_id: str, 
                                     metadata_completed: bool = None,
                                     subtitles_completed: Dict[str, str] = None,
                                     media_completed: bool = None,
                                     processing_status: str = None,
                                     last_step: str = None):
        """
        Update granular completion status for a video.
        
        Args:
            video_id: Video ID to update
            metadata_completed: Whether metadata extraction is completed
            subtitles_completed: Dict of language -> status ('completed', 'failed', 'skipped')
            media_completed: Whether media download is completed
            processing_status: Overall processing status ('pending', 'partial', 'completed', 'failed')
            last_step: Last processing step attempted
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get current status first
        cursor.execute('''
            SELECT subtitles_completed, completion_details 
            FROM videos WHERE video_id = ?
        ''', (video_id,))
        
        result = cursor.fetchone()
        current_subtitles = {}
        current_details = {}
        
        if result:
            if result[0]:
                try:
                    current_subtitles = json.loads(result[0])
                except:
                    current_subtitles = {}
            if result[1]:
                try:
                    current_details = json.loads(result[1])
                except:
                    current_details = {}
        
        # Update fields
        updates = []
        params = []
        
        if metadata_completed is not None:
            updates.append('metadata_completed = ?')
            params.append(metadata_completed)
        
        if subtitles_completed is not None:
            # Merge with existing subtitle status
            current_subtitles.update(subtitles_completed)
            updates.append('subtitles_completed = ?')
            params.append(json.dumps(current_subtitles))
        
        if media_completed is not None:
            updates.append('media_completed = ?')
            params.append(media_completed)
        
        if processing_status is not None:
            updates.append('processing_status = ?')
            params.append(processing_status)
        
        if last_step is not None:
            updates.append('last_processing_step = ?')
            params.append(last_step)
            current_details['last_step'] = last_step
            current_details['last_updated'] = datetime.now().isoformat()
        
        # Always update completion details
        updates.append('completion_details = ?')
        params.append(json.dumps(current_details))
        
        if updates:
            query = f'UPDATE videos SET {", ".join(updates)} WHERE video_id = ?'
            params.append(video_id)
            cursor.execute(query, params)
        
        conn.commit()
        conn.close()
        
        # Log the update for debugging
        self.logger.debug(f"Updated completion status for {video_id}: {updates}")
    
    def mark_step_completed(self, video_id: str, step: str, success: bool = True, details: str = None):
        """
        Mark a specific processing step as completed or failed.
        
        Args:
            video_id: Video ID
            step: Step name ('metadata', 'subtitles_fa', 'subtitles_en', 'media')
            success: Whether the step succeeded
            details: Additional details about the step completion
        """
        status = 'completed' if success else 'failed'
        
        if step == 'metadata':
            self.update_video_completion_status(
                video_id, 
                metadata_completed=success,
                last_step=step,
                processing_status='partial' if success else 'failed'
            )
        elif step.startswith('subtitles_'):
            language = step.split('_', 1)[1]
            self.update_video_completion_status(
                video_id,
                subtitles_completed={language: status},
                last_step=step
            )
        elif step == 'media':
            self.update_video_completion_status(
                video_id,
                media_completed=success,
                last_step=step
            )
        
        # Log the step completion
        self.logger.info(f"Step '{step}' for video {video_id}: {status}" + 
                        (f" - {details}" if details else ""))

    def initialize_completion_status_from_files(self, video_id: str, 
                                              subtitle_languages: list = ['fa', 'en'],
                                              download_media: bool = False,
                                              audio_only: bool = True):
        """
        Initialize completion status for existing records by checking actual file existence.
        This is used for videos that existed before granular tracking was added.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check if record exists and if completion status is already initialized
        cursor.execute('''
            SELECT metadata_completed, subtitles_completed, media_completed, 
                   audio_path, video_path, subtitle_type 
            FROM videos WHERE video_id = ?
        ''', (video_id,))
        
        result = cursor.fetchone()
        if not result:
            conn.close()
            return
        
        (metadata_completed, subtitles_completed_json, media_completed, 
         audio_path, video_path, subtitle_type) = result
        
        # If already initialized, skip
        if metadata_completed is not None:
            conn.close()
            return
        
        self.logger.debug(f"Initializing completion status for existing video {video_id}")
        
        # Initialize metadata as completed (since record exists)
        new_metadata_completed = True
        
        # Check subtitle completion by checking actual files
        subtitles_status = {}
        for lang in subtitle_languages:
            lang_dir = self.subtitles_dir / lang
            manual_file = lang_dir / f"{video_id}_manual.srt"
            auto_file = lang_dir / f"{video_id}_auto.srt"
            
            if ((manual_file.exists() and manual_file.stat().st_size > 0) or 
                (auto_file.exists() and auto_file.stat().st_size > 0)):
                subtitles_status[lang] = 'completed'
            else:
                subtitles_status[lang] = 'not_available'
        
        # Check media completion by checking actual files
        new_media_completed = False
        if download_media:
            # Always check media directory for actual files, regardless of database paths
            existing_files = list(self.media_dir.glob(f"{video_id}.*"))
            if existing_files:
                # Check if any file is substantial (> 1MB to avoid empty/corrupted files)
                for file_path in existing_files:
                    if file_path.stat().st_size > 1024 * 1024:  # > 1MB
                        new_media_completed = True
                        break
                if not new_media_completed:
                    self.logger.warning(f"Found media files for {video_id} but all are < 1MB (likely corrupted): {[f.name for f in existing_files]}")
            else:
                self.logger.debug(f"No media files found for {video_id} in {self.media_dir}")
        
        # Determine overall processing status
        all_subtitles_done = all(status == 'completed' for status in subtitles_status.values()) if subtitle_languages else True
        processing_status = 'completed' if (new_metadata_completed and all_subtitles_done and 
                                          (new_media_completed or not download_media)) else 'partial'
        
        # Update the database
        cursor.execute('''
            UPDATE videos SET 
                metadata_completed = ?, 
                subtitles_completed = ?, 
                media_completed = ?, 
                processing_status = ?,
                last_processing_step = 'initialized_from_files',
                completion_details = ?
            WHERE video_id = ?
        ''', (
            new_metadata_completed,
            json.dumps(subtitles_status),
            new_media_completed,
            processing_status,
            json.dumps({
                'initialized_from_files': True,
                'initialization_time': datetime.now().isoformat(),
                'subtitle_files_found': subtitles_status,
                'media_file_found': new_media_completed
            }),
            video_id
        ))
        
        conn.commit()
        conn.close()
        
        self.logger.info(f"Initialized completion status for {video_id}: "
                        f"metadata={new_metadata_completed}, "
                        f"subtitles={subtitles_status}, "
                        f"media={new_media_completed}, "
                        f"status={processing_status}")

    def _should_process_video(self, video_data: Dict[str, Any], 
                            title_pattern: str = None,
                            min_duration_minutes: int = None,
                            max_duration_minutes: int = None,
                            duration_filter_strict: bool = False) -> Tuple[bool, str]:
        """
        Check if a video should be processed based on title and duration filters.
        
        Returns:
            Tuple of (should_process: bool, reason: str)
        """
        import re
        
        title = video_data.get('title', '')
        length_text = video_data.get('length_text', '')
        
        # Check title pattern
        if title_pattern:
            try:
                if not re.search(title_pattern, title, re.IGNORECASE):
                    return False, f"Title doesn't match pattern: '{title_pattern}'"
            except re.error as e:
                self.logger.warning(f"Invalid regex pattern '{title_pattern}': {e}")
                return False, f"Invalid regex pattern: {e}"
        
        # Check duration filters
        if min_duration_minutes is not None or max_duration_minutes is not None:
            duration_minutes = self._parse_duration_to_minutes(length_text)
            
            if duration_minutes is None:
                if duration_filter_strict:
                    return False, f"Duration info missing and strict filtering enabled"
                else:
                    self.logger.debug(f"Duration info missing for video, proceeding anyway: {length_text}")
            else:
                if min_duration_minutes is not None and duration_minutes < min_duration_minutes:
                    return False, f"Duration {duration_minutes}min < minimum {min_duration_minutes}min"
                
                if max_duration_minutes is not None and duration_minutes > max_duration_minutes:
                    return False, f"Duration {duration_minutes}min > maximum {max_duration_minutes}min"
        
        return True, "Passed all filters"
    
    def _parse_duration_to_minutes(self, length_text: str) -> int:
        """
        Parse YouTube duration text to minutes.
        
        Examples:
            "1:23:45" -> 83 minutes
            "45:30" -> 45 minutes  
            "2:15" -> 2 minutes
            "30" -> 30 minutes (if just seconds)
        """
        if not length_text:
            return None
            
        try:
            # Remove any extra whitespace
            length_text = length_text.strip()
            
            # Split by colons
            parts = length_text.split(':')
            
            if len(parts) == 3:  # H:M:S format
                hours, minutes, seconds = map(int, parts)
                return hours * 60 + minutes
            elif len(parts) == 2:  # M:S format
                minutes, seconds = map(int, parts)
                return minutes
            elif len(parts) == 1:  # Just seconds or minutes
                value = int(parts[0])
                # Assume if > 100, it's seconds, otherwise minutes
                return value // 60 if value > 100 else value
            else:
                return None
                
        except (ValueError, AttributeError):
            self.logger.debug(f"Could not parse duration: '{length_text}'")
            return None

    def _process_single_video(self, video_id: str, download_subtitles: bool, 
                            subtitle_languages: list, download_media: bool, 
                            audio_only: bool, channel_name: str) -> bool:
        """
        Process a single video (existing or new).
        Returns True if successful, False if failed.
        """
        try:
            # Get completion status
            completion_status = self.get_video_completion_status(
                video_id, download_subtitles, subtitle_languages, download_media
            )
            
            if completion_status['is_fully_completed']:
                self.logger.debug(f"Video {video_id} is already fully completed")
                return True
            
            # Load existing video data or create new
            video_data = {}
            if completion_status['exists']:
                # Load from database
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM videos WHERE video_id = ?', (video_id,))
                existing_row = cursor.fetchone()
                if existing_row:
                    columns = [description[0] for description in cursor.description]
                    video_data = dict(zip(columns, existing_row))
                conn.close()
                
                missing_steps = []
                if completion_status['needs_metadata']:
                    missing_steps.append("metadata")
                if completion_status['needs_subtitles']:
                    missing_steps.append(f"subtitles({','.join(completion_status['missing_subtitle_languages'])})")
                if completion_status['needs_media']:
                    missing_steps.append("media")
                
                self.logger.info(f"Processing incomplete video {video_id} - missing: {', '.join(missing_steps)}")
            else:
                # This shouldn't happen in our new logic, but handle it just in case
                self.logger.warning(f"Video {video_id} not found in database but was expected to exist")
                return False
            
            # Extract metadata if needed (shouldn't be needed for existing videos)
            if completion_status['needs_metadata']:
                self.logger.warning(f"Video {video_id} needs metadata but this shouldn't happen for existing videos")
                return False
            
            # Download subtitles if needed
            if completion_status['needs_subtitles']:
                try:
                    languages_to_download = completion_status['missing_subtitle_languages']
                    manual_path, auto_path, subtitle_type, language_results = self.download_subtitles(
                        video_id, languages_to_download
                    )
                    video_data['subtitle_path'] = manual_path
                    video_data['auto_subtitle_path'] = auto_path
                    video_data['subtitle_type'] = subtitle_type
                    video_data['subtitle_languages'] = json.dumps(subtitle_languages)
                    video_data['subtitle_downloaded_at'] = datetime.now().isoformat()
                    
                    # Mark each language based on actual results
                    for lang in languages_to_download:
                        result = language_results.get(lang, 'failed')
                        if result == 'completed':
                            self.mark_step_completed(video_id, f'subtitles_{lang}', True)
                        else:
                            self.mark_step_completed(video_id, f'subtitles_{lang}', False, result)
                            
                except Exception as e:
                    self.logger.error(f"Failed to download subtitles for {video_id}: {e}")
                    video_data['subtitle_type'] = 'failed'
                    for lang in completion_status['missing_subtitle_languages']:
                        self.mark_step_completed(video_id, f'subtitles_{lang}', False, str(e))
            
            # Download media if needed
            if completion_status['needs_media']:
                try:
                    file_path, download_status, file_size_mb = self.download_media(
                        video_id, audio_only
                    )
                    if audio_only:
                        video_data['audio_path'] = file_path
                    else:
                        video_data['video_path'] = file_path
                    video_data['download_status'] = download_status
                    video_data['file_size_mb'] = file_size_mb
                    if download_status == 'completed':
                        video_data['downloaded_at'] = datetime.now().isoformat()
                        self.mark_step_completed(video_id, 'media', True)
                    else:
                        self.mark_step_completed(video_id, 'media', False, f"Download status: {download_status}")
                except Exception as e:
                    self.logger.error(f"Failed to download media for {video_id}: {e}")
                    video_data['download_status'] = 'failed'
                    self.mark_step_completed(video_id, 'media', False, str(e))
            
            # Update final completion status
            final_completion = self.get_video_completion_status(
                video_id, download_subtitles, subtitle_languages, download_media
            )
            
            if final_completion['is_fully_completed']:
                self.update_video_completion_status(
                    video_id, 
                    processing_status='completed',
                    last_step='all_completed'
                )
                self.logger.info(f"Video {video_id} now fully completed")
            else:
                self.update_video_completion_status(
                    video_id,
                    processing_status='partial',
                    last_step='partial_completion'
                )
            
            # Save updated video data
            return self.save_video(video_data, channel_name)
            
        except Exception as e:
            self.logger.error(f"Error processing video {video_id}: {e}")
            return False

    def scrape_playlist(self, playlist_id: str, estimated_total: int = None, 
                       sleep_interval: float = 1, batch_size: int = 100, 
                       download_subtitles: bool = True, subtitle_languages: list = ['fa', 'en'],
                       download_media: bool = False, audio_only: bool = True,
                       test_limit: int = None, title_pattern: str = None,
                       min_duration_minutes: int = None, max_duration_minutes: int = None,
                       duration_filter_strict: bool = False):
        """
        NEW TWO-PHASE APPROACH:
        Phase 1: Discover all videos and store metadata
        Phase 2: Download media and subtitles systematically
        """
        self.logger.info(f"=== PHASE 1: DISCOVERING ALL VIDEOS FROM PLAYLIST {playlist_id} ===")
        
        # Phase 1: Discover all videos
        discovered_count = self.discover_all_videos(
            playlist_id=playlist_id,
            title_pattern=title_pattern,
            min_duration_minutes=min_duration_minutes,
            max_duration_minutes=max_duration_minutes,
            duration_filter_strict=duration_filter_strict
        )
        
        self.logger.info(f"=== PHASE 2: DOWNLOADING MISSING FILES ===")
        
        # Phase 2: Download missing files
        if download_subtitles or download_media:
            success_count, failed_count = self.download_all_missing(
                download_subtitles=download_subtitles,
                subtitle_languages=subtitle_languages,
                download_media=download_media,
                audio_only=audio_only,
                sleep_interval=sleep_interval
            )
        else:
            success_count, failed_count = 0, 0
        
        self.logger.info(f"=== SCRAPING COMPLETE ===")
        self.logger.info(f"Videos discovered: {discovered_count}")
        self.logger.info(f"Downloads successful: {success_count}")
        self.logger.info(f"Downloads failed: {failed_count}")
        
        return

    def discover_all_videos(self, channel_username: str = None, playlist_id: str = None,
                          title_pattern: str = None, min_duration_minutes: int = None, 
                          max_duration_minutes: int = None, duration_filter_strict: bool = False):
        """
        Phase 1: Discover ALL videos and store metadata only (no downloads)
        Uses EXACT working logic from reference script
        """
        if channel_username:
            self.logger.info(f"Discovering all videos from channel: {channel_username}")
            identifier = channel_username
            
            # Use ONLY the Videos tab like the reference script - no Live tab complications
            video_generator = scrapetube.get_channel(
                channel_username=channel_username,
                sort_by="newest",
                limit=None
            )
        else:
            self.logger.info(f"Discovering all videos from playlist: {playlist_id}")
            identifier = f"playlist_{playlist_id}"
            video_generator = scrapetube.get_playlist(playlist_id)
        
        # Get previous progress (use same logic as reference)
        progress = self.get_progress(identifier)
        start_count = progress['total_scraped']
        last_video_id = progress['last_video_id']
        
        self.logger.info(f"Resuming from video count: {start_count}")
        if last_video_id:
            self.logger.info(f"Resuming from video ID: {last_video_id}")
        
        # EXACT variables from reference script
        processed_count = 0
        skipped_count = 0
        saved_count = start_count
        batch_videos = []
        found_last_video = not last_video_id
        batch_size = 50
        
        try:
            # EXACT loop structure from reference script
            for video in video_generator:
                video_id = video.get('videoId', '')
                
                # If we haven't found our last video yet, keep searching (EXACT logic)
                if not found_last_video:
                    if video_id == last_video_id:
                        found_last_video = True
                        self.logger.info(f"Found last processed video, continuing with new videos")
                    continue
                
                # Skip if we've already processed this video (EXACT logic)
                if self.is_video_exists(video_id):
                    skipped_count += 1
                    continue
                
                # Extract metadata (EXACT logic)
                video_data = self.extract_video_data(video)
                
                # Apply filters (our addition)
                should_process, filter_reason = self._should_process_video(
                    video_data, title_pattern, min_duration_minutes, 
                    max_duration_minutes, duration_filter_strict
                )
                
                if not should_process:
                    self.logger.debug(f"Filtered out {video_id}: {filter_reason}")
                    skipped_count += 1
                    continue
                
                # Set discovery-specific fields
                video_data['download_status'] = 'pending'
                video_data['metadata_completed'] = True
                video_data['processing_status'] = 'metadata_only'
                
                # Save video (EXACT logic)
                if self.save_video(video_data, identifier):
                    saved_count += 1
                    processed_count += 1
                    batch_videos.append(video_id)
                
                # Update progress in database periodically (EXACT logic)
                if processed_count % batch_size == 0:
                    self.update_progress(identifier, video_id, saved_count)
                    self.logger.info(f"Discovered {processed_count} new videos. Last video ID: {video_id}")
                    time.sleep(0.1)  # Rate limiting
                    
        except Exception as e:
            self.logger.error(f"Error during discovery: {e}")
            # Update progress even if there's an error (EXACT logic)
            if batch_videos:
                self.update_progress(identifier, batch_videos[-1], saved_count)
                self.logger.info(f"Saved progress after error. Last video ID: {batch_videos[-1]}")
        
        # Final update
        if batch_videos:
            self.update_progress(identifier, batch_videos[-1], saved_count)
        
        net_discovered = saved_count - start_count
        self.logger.info(f"Discovery finished. New videos found: {net_discovered}, Skipped: {skipped_count}")
        self.logger.info(f"Total videos in database: {saved_count}")
        return net_discovered
    
    def download_all_missing(self, download_subtitles: bool = True, subtitle_languages: list = ['fa', 'en'],
                           download_media: bool = False, audio_only: bool = True, sleep_interval: float = 1):
        """
        Phase 2: Go through database and download missing media/subtitles
        """
        self.logger.info("Starting download phase for all videos in database...")
        
        # Get all videos that need downloads
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = "SELECT video_id, title FROM videos WHERE processing_status != 'completed' ORDER BY scraped_at"
        cursor.execute(query)
        videos_to_process = cursor.fetchall()
        conn.close()
        
        total_videos = len(videos_to_process)
        self.logger.info(f"Found {total_videos} videos needing downloads")
        
        success_count = 0
        failed_count = 0
        
        for i, (video_id, title) in enumerate(videos_to_process, 1):
            self.logger.info(f"[{i}/{total_videos}] Processing {video_id}: {title[:50]}...")
            
            try:
                # Get current completion status
                completion_status = self.get_video_completion_status(
                    video_id, download_subtitles, subtitle_languages, download_media
                )
                
                if completion_status['is_fully_completed']:
                    self.logger.info(f"Video {video_id} already completed, skipping")
                    continue
                
                success = True
                
                # Download subtitles if needed
                if completion_status['needs_subtitles']:
                    try:
                        languages_to_download = completion_status['missing_subtitle_languages']
                        self.logger.info(f"Downloading subtitles for {video_id}: {languages_to_download}")
                        
                        manual_path, auto_path, subtitle_type, language_results = self.download_subtitles(
                            video_id, languages_to_download
                        )
                        
                        # Update database with subtitle info
                        conn = sqlite3.connect(self.db_path)
                        cursor = conn.cursor()
                        cursor.execute('''
                            UPDATE videos SET 
                                subtitle_path = ?, auto_subtitle_path = ?, subtitle_type = ?,
                                subtitle_languages = ?, subtitle_downloaded_at = ?
                            WHERE video_id = ?
                        ''', (manual_path, auto_path, subtitle_type, 
                              json.dumps(subtitle_languages), datetime.now().isoformat(), video_id))
                        conn.commit()
                        conn.close()
                        
                        # Mark each language result
                        for lang in languages_to_download:
                            result = language_results.get(lang, 'failed')
                            if result == 'completed':
                                self.mark_step_completed(video_id, f'subtitles_{lang}', True)
                            else:
                                self.mark_step_completed(video_id, f'subtitles_{lang}', False, result)
                                
                    except Exception as e:
                        self.logger.error(f"Failed to download subtitles for {video_id}: {e}")
                        success = False
                
                # Download media if needed
                if completion_status['needs_media']:
                    # Quick check if video still exists before attempting download
                    if not self._quick_video_check(video_id):
                        self.logger.info(f"Video {video_id} appears to be deleted/unavailable - skipping media download")
                        self.mark_step_completed(video_id, 'media', False, 'Video deleted/unavailable')
                        success = False
                    else:
                        try:
                            self.logger.info(f"Downloading {'audio' if audio_only else 'video'} for {video_id}")
                            
                            file_path, download_status, file_size_mb = self.download_media(video_id, audio_only)
                            
                            # Update database with media info
                            conn = sqlite3.connect(self.db_path)
                            cursor = conn.cursor()
                            if audio_only:
                                cursor.execute('''
                                    UPDATE videos SET 
                                        audio_path = ?, download_status = ?, file_size_mb = ?, downloaded_at = ?
                                    WHERE video_id = ?
                                ''', (file_path, download_status, file_size_mb, 
                                      datetime.now().isoformat() if download_status == 'completed' else None, video_id))
                            else:
                                cursor.execute('''
                                    UPDATE videos SET 
                                        video_path = ?, download_status = ?, file_size_mb = ?, downloaded_at = ?
                                    WHERE video_id = ?
                                ''', (file_path, download_status, file_size_mb, 
                                      datetime.now().isoformat() if download_status == 'completed' else None, video_id))
                            conn.commit()
                            conn.close()
                            
                            if download_status == 'completed':
                                self.mark_step_completed(video_id, 'media', True)
                                # Add random delay after successful download (like bash script)
                                sleep_time = random.randint(3, 10)
                                self.logger.debug(f"Successful download, sleeping {sleep_time}s...")
                                time.sleep(sleep_time)
                            else:
                                self.mark_step_completed(video_id, 'media', False, f"Status: {download_status}")
                                success = False
                                
                        except Exception as e:
                            error_msg = str(e)
                            self.logger.error(f"Failed to download media for {video_id}: {e}")
                            
                            # If it's an authentication error, stop the entire process
                            if "authentication" in error_msg.lower() or "manual intervention required" in error_msg.lower():
                                self.logger.error(f"🛑 STOPPING DOWNLOAD PROCESS due to authentication issue")
                                raise e  # Re-raise to stop the entire download process
                            
                            self.mark_step_completed(video_id, 'media', False, str(e))
                            success = False
                
                # Update final status
                if success:
                    self.update_video_completion_status(video_id, processing_status='completed')
                    success_count += 1
                else:
                    self.update_video_completion_status(video_id, processing_status='partial')
                    failed_count += 1
            
                # Rate limiting
                if i % 30 == 0:
                    time.sleep(sleep_interval)
                    
            except Exception as e:
                error_msg = str(e)
                self.logger.error(f"Error processing video {video_id}: {e}")
                
                # Check if it's an authentication error that should stop everything
                if ("authentication" in error_msg.lower() or 
                    "manual intervention required" in error_msg.lower() or
                    "bot detection" in error_msg.lower()):
                    
                    self.logger.error(f"🛑 CRITICAL ERROR - Stopping download process")
                    self.logger.error(f"Last processed video: {video_id}")
                    self.logger.error(f"Progress: {i}/{total_videos} videos processed")
                    self.logger.error(f"Successful downloads so far: {success_count}")
                    break  # Stop the entire download process
                else:
                    # For other errors, just mark as failed and continue
                    failed_count += 1
                    self.update_video_completion_status(video_id, processing_status='failed')
        
        self.logger.info(f"Download phase complete: {success_count} successful, {failed_count} failed")
        return success_count, failed_count

    def scrape_channel(self, channel_username: str, estimated_total: int = None, 
                      sleep_interval: float = 1, batch_size: int = 100, 
                      download_subtitles: bool = True, subtitle_languages: list = ['fa', 'en'],
                      download_media: bool = False, audio_only: bool = True,
                      test_limit: int = None, title_pattern: str = None,
                      min_duration_minutes: int = None, max_duration_minutes: int = None,
                      duration_filter_strict: bool = False):
        """
        NEW TWO-PHASE APPROACH:
        Phase 1: Discover all videos and store metadata
        Phase 2: Download media and subtitles systematically
        """
        self.logger.info(f"=== PHASE 1: DISCOVERING ALL VIDEOS FROM CHANNEL {channel_username} ===")
        
        # Phase 1: Discover all videos
        discovered_count = self.discover_all_videos(
            channel_username=channel_username,
            title_pattern=title_pattern,
            min_duration_minutes=min_duration_minutes,
            max_duration_minutes=max_duration_minutes,
            duration_filter_strict=duration_filter_strict
        )
        
        self.logger.info(f"=== PHASE 2: DOWNLOADING MISSING FILES ===")
        
        # Phase 2: Download missing files
        if download_subtitles or download_media:
            success_count, failed_count = self.download_all_missing(
                download_subtitles=download_subtitles,
                subtitle_languages=subtitle_languages,
                download_media=download_media,
                audio_only=audio_only,
                sleep_interval=sleep_interval
            )
        else:
            success_count, failed_count = 0, 0
        
        self.logger.info(f"=== SCRAPING COMPLETE ===")
        self.logger.info(f"Videos discovered: {discovered_count}")
        self.logger.info(f"Downloads successful: {success_count}")
        self.logger.info(f"Downloads failed: {failed_count}")
        
    
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
                   thumbnail_url, channel_verified, channel_name,
                   subtitle_path, subtitle_type, auto_subtitle_path, 
                   subtitle_languages, audio_path, video_path, download_status, 
                   file_size_mb, subtitle_downloaded_at, downloaded_at, scraped_at
            FROM videos 
            ORDER BY scraped_at DESC
        ''', conn)
        
        df.to_csv(output_file, index=False)
        conn.close()
        
        self.logger.info(f"Data exported to {output_file}")

def main():
    parser = argparse.ArgumentParser(
        description="Scrape all videos from a YouTube channel and optionally download subtitles",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("input_identifier", 
                        help="YouTube channel username (without @) or playlist ID")
    parser.add_argument("--db-name", default="",
                        help="Database name")
    parser.add_argument("--download-dir", 
                        help="Directory to download video/audio files (optional)",
                        default='/mnt/hdd2/Transcribed-YT')
    parser.add_argument("--estimated-total", type=int, default=None,
                        help="Estimated total number of videos (auto-detected if not provided)")
    parser.add_argument("--sleep-interval", type=float, default=0.5,
                        help="Sleep time between requests in seconds")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="Number of videos to process before updating progress")
    parser.add_argument("--download-subtitles", action="store_true", default=True,
                        help="Download subtitles alongside metadata")
    parser.add_argument("--subtitle-languages", nargs="+", default=["fa", "en"],
                        help="Language codes for subtitle download (e.g., en es fr)")
    parser.add_argument("--download-media", action="store_true", default=False,
                        help="Download actual video/audio files")
    parser.add_argument("--video-format", action="store_true", default=False,
                        help="Download video format instead of audio-only")
    parser.add_argument("--stats-only", action="store_true",
                        help="Only show statistics, don't scrape")
    parser.add_argument("--test-limit", type=int, default=None,
                        help="Test run: only process N videos (default: all)")
    
    # Content filtering options
    parser.add_argument("--title-pattern", type=str, default=None,
                        help="Only download videos matching this title pattern (supports regex)")
    parser.add_argument("--min-duration-minutes", type=int, default=None,
                        help="Minimum video duration in minutes")
    parser.add_argument("--max-duration-minutes", type=int, default=None,
                        help="Maximum video duration in minutes")
    parser.add_argument("--duration-filter-strict", action="store_true", default=False,
                        help="Skip videos with missing duration info instead of processing them")
    
    # Input type specification
    parser.add_argument("--playlist", action="store_true", default=False,
                        help="Treat input as playlist ID instead of channel username")
    
    args = parser.parse_args()

    
    # Determine if input is channel or playlist
    if args.playlist:
        identifier_name = f"playlist_{args.input_identifier}"
        input_type = "playlist"
    else:
        identifier_name = args.input_identifier
        input_type = "channel"
    
    download_dir = os.path.join(args.download_dir, identifier_name) if args.download_dir else f'./{identifier_name}'
    os.makedirs(download_dir, exist_ok=True)

    if args.download_media: 
        if not args.video_format:
            media_dir = os.path.join(download_dir, 'audio')
        elif args.video_format:
            media_dir = os.path.join(download_dir, 'video')
        os.makedirs(media_dir, exist_ok=True)
    else:
        media_dir = None

    if args.download_subtitles:
        subtitles_dir = os.path.join(download_dir, 'subtitles')
        os.makedirs(subtitles_dir, exist_ok=True)    
    else:
        subtitles_dir = None
    
    db_path = os.path.join(download_dir, f"{identifier_name}.db") if not args.db_name else os.path.join(download_dir, args.db_name)
    
    
    # Initialize scraper
    scraper = YouTubeChannelScraper(db_path, 
                                    download_dir = download_dir,
                                    subtitles_dir = subtitles_dir,
                                    media_dir = media_dir,
                                    channel_name = identifier_name)
    
    if args.stats_only:
        # Just show statistics
        stats = scraper.get_stats()
        print("\nDatabase Statistics:")
        for key, value in stats.items():
            print(f"{key}: {value}")
        return
    
    print(f"Starting to scrape {input_type}: {args.input_identifier}")
    print(f"Database: {db_path}")
    print(f"Subtitle directory: {subtitles_dir}")
    
    print(f"Download directory: {download_dir}")
    print(f"Download subtitles: {args.download_subtitles}")
    if args.download_subtitles:
        print(f"Subtitle languages: {args.subtitle_languages}")
    print(f"Download media: {args.download_media}")
    if args.download_media:
        print(f"Format: {'Video' if args.video_format else 'Audio only'}")
    
    # Show content filters
    if args.title_pattern or args.min_duration_minutes or args.max_duration_minutes:
        print(f"\nContent Filters:")
        if args.title_pattern:
            print(f"  Title pattern: {args.title_pattern}")
        if args.min_duration_minutes:
            print(f"  Minimum duration: {args.min_duration_minutes} minutes")
        if args.max_duration_minutes:
            print(f"  Maximum duration: {args.max_duration_minutes} minutes")
        if args.duration_filter_strict:
            print(f"  Strict duration filtering: Enabled")
    print()
    
    try:
        # Start scraping based on input type
        if args.playlist:
            scraper.scrape_playlist(
                playlist_id=args.input_identifier,
                estimated_total=args.estimated_total,
                sleep_interval=args.sleep_interval,
                batch_size=args.batch_size,
                download_subtitles=args.download_subtitles,
                subtitle_languages=args.subtitle_languages,
                download_media=args.download_media,
                audio_only=not args.video_format,
                test_limit=args.test_limit,
                title_pattern=args.title_pattern,
                min_duration_minutes=args.min_duration_minutes,
                max_duration_minutes=args.max_duration_minutes,
                duration_filter_strict=args.duration_filter_strict
            )
        else:
            scraper.scrape_channel(
                channel_username=args.input_identifier,
                estimated_total=args.estimated_total,
                sleep_interval=args.sleep_interval,
                batch_size=args.batch_size,
                download_subtitles=args.download_subtitles,
                subtitle_languages=args.subtitle_languages,
                download_media=args.download_media,
                audio_only=not args.video_format,
                test_limit=args.test_limit,
                title_pattern=args.title_pattern,
                min_duration_minutes=args.min_duration_minutes,
                max_duration_minutes=args.max_duration_minutes,
                duration_filter_strict=args.duration_filter_strict
            )
        
        # Get statistics
        stats = scraper.get_stats()
        print("\nScraping Statistics:")
        for key, value in stats.items():
            print(f"{key}: {value}")
        
        # Export to CSV if requested
        

        csv_path = os.path.join(download_dir, f"{identifier_name}.csv")
        scraper.export_to_csv(csv_path)
        print(f"\nData exported to {csv_path}")
            
    except KeyboardInterrupt:
        print("\nScraping interrupted by user. Progress has been saved.")
    except Exception as e:
        print(f"\nError during scraping: {e}")
        print("Progress has been saved. You can resume by running the same command.")

if __name__ == "__main__":
    main()