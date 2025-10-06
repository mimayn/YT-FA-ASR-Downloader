#!/bin/bash

# Configuration (arguments)
# Usage:
#   Single DB:
#     ./download_channel_audios.sh /path/to/channel.db [audio_subdir]
#   DB list file:
#     ./download_channel_audios.sh --db-list /path/to/list.txt [audio_subdir]
#   - audio_subdir defaults to 'audio' and is created next to each DB

MODE="single"
if [ "${1:-}" = "--db-list" ]; then
    MODE="list"
    DB_LIST_FILE="${2:-}"
    AUDIO_SUBDIR="${3:-audio}"
else
    DB_PATH="${1:-}"
    AUDIO_SUBDIR="${2:-audio}"
fi

if [ "$MODE" = "single" ]; then
    if [ -z "$DB_PATH" ]; then
        echo "Usage: $0 /path/to/channel.db [audio_subdir]" >&2
        echo "   or: $0 --db-list /path/to/list.txt [audio_subdir]" >&2
        exit 1
    fi
    if [ ! -f "$DB_PATH" ]; then
        echo "Database file not found: $DB_PATH" >&2
        exit 1
    fi
else
    if [ -z "$DB_LIST_FILE" ]; then
        echo "Usage: $0 --db-list /path/to/list.txt [audio_subdir]" >&2
        exit 1
    fi
    if [ ! -f "$DB_LIST_FILE" ]; then
        echo "DB list file not found: $DB_LIST_FILE" >&2
        exit 1
    fi
fi

# Per-DB variables (initialized in init_for_db)
BASE_DIR=""
TARGET_DIR=""
DB=""
LOG_FILE=""
ERROR_LOG=""
FAILED_VIDEOS=""
PROGRESS_FILE=""
DELETED_VIDEOS=""
DOWNLOADED_VIDEOS=""

init_for_db() {
    local db_path_local="$1"
    BASE_DIR="$(dirname "$db_path_local")"
    TARGET_DIR="$BASE_DIR/$AUDIO_SUBDIR"
    DB="$db_path_local"
    LOG_FILE="$BASE_DIR/download_log.txt"
    ERROR_LOG="$BASE_DIR/download_errors.txt"
    FAILED_VIDEOS="$BASE_DIR/failed_videos.txt"
    PROGRESS_FILE="$BASE_DIR/download_progress.txt"
    DELETED_VIDEOS="$BASE_DIR/deleted_videos.txt"
    DOWNLOADED_VIDEOS="$BASE_DIR/downloaded_videos.txt"
    mkdir -p "$TARGET_DIR"
    touch "$LOG_FILE" "$ERROR_LOG" "$FAILED_VIDEOS" "$PROGRESS_FILE" "$DELETED_VIDEOS" "$DOWNLOADED_VIDEOS"
}

# Ensure yt-dlp is available and up-to-date (best effort)
python3 -m pip install -U yt-dlp >/dev/null 2>&1 || true

# Function to log messages
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# Function to log errors
log_error() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - ERROR: $1" | tee -a "$ERROR_LOG"
}

# Function to save progress
save_progress() {
    local current_rowid=$1
    local video_id=$2
    echo "$current_rowid:$video_id" > "$PROGRESS_FILE"
}

# Function to load progress
load_progress() {
    if [ -f "$PROGRESS_FILE" ] && [ -s "$PROGRESS_FILE" ]; then
        cat "$PROGRESS_FILE"
    else
        echo "0:"
    fi
}

# Function to check if video is already downloaded (from log)
is_downloaded() {
    local video_id=$1
    grep -q "^$video_id$" "$DOWNLOADED_VIDEOS" 2>/dev/null
}

# Function to check if video is known to be deleted
is_deleted() {
    local video_id=$1
    grep -q "^$video_id$" "$DELETED_VIDEOS" 2>/dev/null
}

# Function to mark video as downloaded
mark_downloaded() {
    local video_id=$1
    echo "$video_id" >> "$DOWNLOADED_VIDEOS"
}

# Function to mark video as deleted
mark_deleted() {
    local video_id=$1
    echo "$video_id" >> "$DELETED_VIDEOS"
}

# Function to quickly check if video exists (smarter than 3 retries)
quick_video_check() {
    local video_id=$1
    local url="https://www.youtube.com/watch?v=$video_id"
    
    # Use yt-dlp to just extract info without downloading
    if yt-dlp --quiet --no-warnings --skip-download --print title "$url" >/dev/null 2>&1; then
        return 0  # Video exists
    else
        # Check if it's a definitive deletion error
        local error_output=$(yt-dlp --quiet --no-warnings --skip-download --print title "$url" 2>&1)
        if echo "$error_output" | grep -qi "unavailable\|removed\|deleted\|private\|not found"; then
            return 1  # Video deleted
        else
            return 0  # Assume temporary error, video exists
        fi
    fi
}

# Function to download a video (your existing logic)
download_video() {
    local video_id=$1
    local output_path="$TARGET_DIR/${video_id}.mp3"
    
    # Skip if already exists
    if [ -f "$output_path" ]; then
        log "Skipping $video_id - already exists"
        mark_downloaded "$video_id"
        return 0
    fi
    
    log "Downloading $video_id..."
    
    # Try downloading with increasing delays
    local attempt=1
    local max_attempts=3
    
    while [ $attempt -le $max_attempts ]; do
        log "Attempt $attempt for $video_id"
        
        if yt-dlp \
            --cookies-from-browser firefox \
            -x --audio-format mp3 \
            --audio-quality 0 \
            --format "bestaudio[ext=m4a]" \
            -o "$TARGET_DIR/%(id)s.%(ext)s" \
            "https://www.youtube.com/watch?v=$video_id" >/dev/null 2>&1; then
            
            log "Successfully downloaded $video_id"
            mark_downloaded "$video_id"
            return 0
        else
            log_error "Attempt $attempt failed for $video_id"
            if [ $attempt -lt $max_attempts ]; then
                # Progressive backoff: 30s, 60s
                sleep_time=$((30 * attempt))
                log "Waiting $sleep_time seconds before retry..."
                sleep $sleep_time
            fi
        fi
        attempt=$((attempt + 1))
    done
    
    log_error "Failed to download $video_id after $max_attempts attempts"
    echo "$video_id" >> "$FAILED_VIDEOS"
    return 1
}

# Function to handle script interruption
cleanup() {
    log "Script interrupted. Cleaning up..."
    exit 1
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM

process_db() {
    init_for_db "$1"

    # Main execution
    log "Starting download process..."

    # Load previous progress
    progress_info=$(load_progress)
    last_rowid=$(echo "$progress_info" | cut -d':' -f1)
    last_video_id=$(echo "$progress_info" | cut -d':' -f2)

    if [ "$last_rowid" -gt 0 ]; then
        log "Resuming from row ID $last_rowid (last processed: $last_video_id)"
    else
        log "Starting fresh download"
    fi

    # Read video IDs from the SQLite database starting from last processed row
    if [ "$last_rowid" -gt 0 ]; then
        video_ids=$(sqlite3 "$DB" "SELECT rowid, video_id FROM videos WHERE rowid > $last_rowid ORDER BY rowid")
    else
        video_ids=$(sqlite3 "$DB" "SELECT rowid, video_id FROM videos ORDER BY rowid")
    fi

    total_videos=$(sqlite3 "$DB" "SELECT COUNT(*) FROM videos")
    remaining_videos=$(echo "$video_ids" | wc -l)

    log "Total videos in database: $total_videos"
    log "Videos to process: $remaining_videos"
    log "Already downloaded (from log): $(wc -l < "$DOWNLOADED_VIDEOS" 2>/dev/null || echo 0)"
    log "Known deleted videos: $(wc -l < "$DELETED_VIDEOS" 2>/dev/null || echo 0)"

    # Process each video with progress tracking
    while IFS= read -r line; do
        [ -z "$line" ] && continue
        
        current_rowid=$(echo "$line" | cut -d'|' -f1)
        video_id=$(echo "$line" | cut -d'|' -f2)
        
        log "Processing row $current_rowid: $video_id"
        
        # Save progress with row ID
        save_progress "$current_rowid" "$video_id"
        
        # Quick check if already downloaded (from log)
        if is_downloaded "$video_id"; then
            log "Skipping $video_id - found in downloaded log"
            continue
        fi
        
        # Quick check if known to be deleted
        if is_deleted "$video_id"; then
            log "Skipping $video_id - found in deleted log"
            continue
        fi
        
        # Check if file already exists (fast local check, no delay)
        output_path="$TARGET_DIR/${video_id}.mp3"
        if [ -f "$output_path" ]; then
            log "Skipping $video_id - already exists"
            mark_downloaded "$video_id"
            continue
        fi
        
        # Smart check if video still exists before attempting download
        if ! quick_video_check "$video_id"; then
            log "Video $video_id appears to be deleted/unavailable - skipping"
            mark_deleted "$video_id"
            continue
        fi
        
        # Only download if file doesn't exist and video exists
        if download_video "$video_id"; then
            # Add random delay between 5-15 seconds after successful download
            sleep_time=$((RANDOM % 10 + 5))
            sleep $sleep_time
        fi
        
        # Periodic status update
        if [ $((current_rowid % 10)) -eq 0 ]; then
            failed_count=$(wc -l < "$FAILED_VIDEOS" 2>/dev/null || echo 0)
            downloaded_count=$(wc -l < "$DOWNLOADED_VIDEOS" 2>/dev/null || echo 0)
            deleted_count=$(wc -l < "$DELETED_VIDEOS" 2>/dev/null || echo 0)
            log "Progress: Row $current_rowid processed, $downloaded_count downloaded, $failed_count failed, $deleted_count deleted"
        fi
    done <<< "$video_ids"

    # Final statistics
    failed_count=$(wc -l < "$FAILED_VIDEOS" 2>/dev/null || echo 0)
    downloaded_count=$(wc -l < "$DOWNLOADED_VIDEOS" 2>/dev/null || echo 0)
    deleted_count=$(wc -l < "$DELETED_VIDEOS" 2>/dev/null || echo 0)

    log "Download process completed"
    log "Successfully downloaded: $downloaded_count videos"
    log "Failed downloads: $failed_count"
    log "Deleted/unavailable videos: $deleted_count"
    log "Failed videos have been saved to $FAILED_VIDEOS"
    log "Deleted videos have been saved to $DELETED_VIDEOS"

    # Clean up progress file on successful completion
    rm -f "$PROGRESS_FILE"
}

# Dispatch: single or list mode
if [ "$MODE" = "single" ]; then
    process_db "$DB_PATH"
else
    while IFS= read -r db_line; do
        db_line_trimmed="${db_line##*( )}"
        db_line_trimmed="${db_line_trimmed%%*( )}"
        [ -z "$db_line_trimmed" ] && continue
        [[ "$db_line_trimmed" =~ ^# ]] && continue
        if [ -f "$db_line_trimmed" ]; then
            echo "\n===== Processing DB: $db_line_trimmed ====="
            process_db "$db_line_trimmed"
        else
            echo "Skipping missing DB path: $db_line_trimmed" >&2
        fi
    done < "$DB_LIST_FILE"
fi
