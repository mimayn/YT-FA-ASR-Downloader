#!/usr/bin/env python3
"""
Batch YouTube Scraper
Processes multiple channels and playlists from a text file, one after another.

Usage:
    python batch_youtube_scraper.py sources.txt [additional_args]

Text file format (one per line):
    Vittoparsa                    # Channel username
    playlist_PLxyz123abc         # Playlist ID (must start with 'playlist_')
    @SomeChannel                 # Channel with @
    playlist_PLabcdef456         # Another playlist

Additional args are passed to youtube_scraper.py for each source.
"""

import os
import sys
import subprocess
import time
import logging
from datetime import datetime
from pathlib import Path

class BatchYouTubeScraper:
    def __init__(self, sources_file: str, additional_args: list = None):
        self.sources_file = sources_file
        self.additional_args = additional_args or []
        self.setup_logging()
        
        # Track progress
        self.total_sources = 0
        self.processed_sources = 0
        self.successful_sources = 0
        self.failed_sources = []
        
    def setup_logging(self):
        """Setup logging for the batch process"""
        log_file = f"batch_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Batch scraper started. Logging to: {log_file}")
        
    def read_sources(self) -> list:
        """Read and validate sources from the input file"""
        if not os.path.exists(self.sources_file):
            self.logger.error(f"Sources file not found: {self.sources_file}")
            return []
            
        sources = []
        try:
            with open(self.sources_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    
                    # Skip empty lines and comments
                    if not line or line.startswith('#'):
                        continue
                    
                    # Clean up channel names (remove @ if present)
                    if line.startswith('@'):
                        line = line[1:]
                    
                    # Validate format
                    if line.startswith('playlist_'):
                        if len(line) < 20:  # playlist IDs are long
                            self.logger.warning(f"Line {line_num}: '{line}' looks like invalid playlist ID")
                        source_type = 'playlist'
                        identifier = line[9:]  # Remove 'playlist_' prefix
                    else:
                        source_type = 'channel'
                        identifier = line
                    
                    sources.append({
                        'type': source_type,
                        'identifier': identifier,
                        'original': line,
                        'line_num': line_num
                    })
                    
        except Exception as e:
            self.logger.error(f"Error reading sources file: {e}")
            return []
            
        self.total_sources = len(sources)
        self.logger.info(f"Found {self.total_sources} sources to process")
        return sources
        
    def run_scraper(self, source: dict) -> bool:
        """Run youtube_scraper.py for a single source"""
        source_type = source['type']
        identifier = source['identifier']
        original = source['original']
        
        # Build command
        cmd = ['python', 'youtube_scraper.py', identifier]
        
        # Add playlist flag if needed
        if source_type == 'playlist':
            cmd.append('--playlist')
            
        # Add additional arguments
        cmd.extend(self.additional_args)
        
        self.logger.info(f"🚀 Starting {source_type}: {original}")
        self.logger.info(f"Command: {' '.join(cmd)}")
        
        start_time = time.time()
        
        try:
            # Run the scraper
            result = subprocess.run(
                cmd,
                capture_output=False,  # Let output go to console
                text=True,
                cwd=os.getcwd()
            )
            
            duration = time.time() - start_time
            duration_str = f"{duration/60:.1f} minutes"
            
            if result.returncode == 0:
                self.logger.info(f"✅ Successfully completed {source_type}: {original} ({duration_str})")
                return True
            else:
                self.logger.error(f"❌ Failed {source_type}: {original} (exit code: {result.returncode}) ({duration_str})")
                return False
                
        except KeyboardInterrupt:
            self.logger.warning(f"⚠️  User interrupted processing of {source_type}: {original}")
            raise
        except Exception as e:
            duration = time.time() - start_time
            duration_str = f"{duration/60:.1f} minutes"
            self.logger.error(f"❌ Error processing {source_type}: {original} - {e} ({duration_str})")
            return False
            
    def process_all(self):
        """Process all sources in the file"""
        sources = self.read_sources()
        if not sources:
            self.logger.error("No valid sources found. Exiting.")
            return
            
        self.logger.info(f"🎯 Starting batch processing of {len(sources)} sources...")
        self.logger.info(f"Additional args: {' '.join(self.additional_args)}")
        print()
        
        start_time = time.time()
        
        try:
            for i, source in enumerate(sources, 1):
                self.processed_sources += 1
                
                self.logger.info(f"📋 [{i}/{len(sources)}] Processing: {source['original']}")
                
                if self.run_scraper(source):
                    self.successful_sources += 1
                else:
                    self.failed_sources.append(source['original'])
                
                # Add a small delay between sources to be nice to YouTube
                if i < len(sources):
                    self.logger.info("⏳ Waiting 30 seconds before next source...")
                    time.sleep(30)
                    print()
                    
        except KeyboardInterrupt:
            self.logger.warning("🛑 Batch processing interrupted by user")
            
        # Final summary
        total_time = time.time() - start_time
        total_time_str = f"{total_time/3600:.1f} hours" if total_time > 3600 else f"{total_time/60:.1f} minutes"
        
        print("\n" + "="*60)
        self.logger.info("📊 BATCH PROCESSING SUMMARY")
        self.logger.info(f"Total sources: {self.total_sources}")
        self.logger.info(f"Processed: {self.processed_sources}")
        self.logger.info(f"Successful: {self.successful_sources}")
        self.logger.info(f"Failed: {len(self.failed_sources)}")
        self.logger.info(f"Total time: {total_time_str}")
        
        if self.failed_sources:
            self.logger.error("❌ Failed sources:")
            for failed in self.failed_sources:
                self.logger.error(f"  - {failed}")
        else:
            self.logger.info("🎉 All sources processed successfully!")
            
        print("="*60)

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        print("\nExample:")
        print("  python batch_youtube_scraper.py sources.txt --download-subtitles --subtitle-languages fa en")
        sys.exit(1)
        
    sources_file = sys.argv[1]
    additional_args = sys.argv[2:]  # Pass remaining args to youtube_scraper.py
    
    # Validate that youtube_scraper.py exists
    if not os.path.exists('youtube_scraper.py'):
        print("❌ Error: youtube_scraper.py not found in current directory")
        sys.exit(1)
        
    # Create and run batch scraper
    batch_scraper = BatchYouTubeScraper(sources_file, additional_args)
    batch_scraper.process_all()

if __name__ == "__main__":
    main()
