#!/bin/bash

# Usage:
#   Single DB:
#     ./download_channel_subs.sh /path/to/channel.db en,fa,ar [subtitles_subdir]
#   DB list file:
#     ./download_channel_subs.sh --db-list /path/to/list.txt en,fa,ar [subtitles_subdir]
#   - For single DB: args are as before
#   - For list mode: 1st arg is --db-list, 2nd is txt file of DB paths (one per line, # comments allowed), 3rd is languages, 4th optional subdir

set -uo pipefail

MODE="single"
if [ "${1:-}" = "--db-list" ]; then
    MODE="list"
    DB_LIST_FILE="${2:-}"
    LANGS_CSV="${3:-}"
    SUBS_SUBDIR="${4:-subtitles}"
else
    DB_PATH="${1:-}"
    LANGS_CSV="${2:-}"
    SUBS_SUBDIR="${3:-subtitles}"
fi

if [ "$MODE" = "single" ]; then
    if [ -z "$DB_PATH" ] || [ -z "$LANGS_CSV" ]; then
        echo "Usage: $0 /path/to/channel.db en,fa[,more] [subtitles_subdir]" >&2
        echo "   or: $0 --db-list /path/to/list.txt en,fa[,more] [subtitles_subdir]" >&2
        exit 1
    fi
    if [ ! -f "$DB_PATH" ]; then
        echo "Database file not found: $DB_PATH" >&2
        exit 1
    fi
else
    if [ -z "$DB_LIST_FILE" ] || [ -z "$LANGS_CSV" ]; then
        echo "Usage: $0 --db-list /path/to/list.txt en,fa[,more] [subtitles_subdir]" >&2
        exit 1
    fi
    if [ ! -f "$DB_LIST_FILE" ]; then
        echo "DB list file not found: $DB_LIST_FILE" >&2
        exit 1
    fi
fi

# Per-DB variables (will be set in init_for_db)
BASE_DIR=""
TARGET_DIR=""
DB=""
LOG_FILE=""
ERROR_LOG=""
FAILED_FILE=""
TEMP_UNAVAILABLE_FILE=""

init_for_db() {
    local db_path_local="$1"
    BASE_DIR="$(dirname "$db_path_local")"
    TARGET_DIR="$BASE_DIR/$SUBS_SUBDIR"
    DB="$db_path_local"
    LOG_FILE="$BASE_DIR/subs_download_log.txt"
    ERROR_LOG="$BASE_DIR/subs_download_errors.txt"
    FAILED_FILE="$BASE_DIR/subs_failed.txt"
    TEMP_UNAVAILABLE_FILE="$BASE_DIR/temp_unavailable_videos.txt"
    mkdir -p "$TARGET_DIR"
    touch "$LOG_FILE" "$ERROR_LOG" "$FAILED_FILE" "$TEMP_UNAVAILABLE_FILE" 
}

# Ensure yt-dlp is available and reasonably up-to-date (best effort)
python3 -m pip install -U yt-dlp >/dev/null 2>&1 || true

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

log_error() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - ERROR: $1" | tee -a "$ERROR_LOG"
}

# New tracking system: completed and nonexistent files per language/type
get_completed_file() {
    local lang=$1
    local kind=$2
    echo "$BASE_DIR/completed_${kind}_${lang}_subs.txt"
}

get_nonexistent_file() {
    local lang=$1
    local kind=$2
    echo "$BASE_DIR/nonexistent_${kind}_${lang}_subs.txt"
}

is_completed() {
    local video_id=$1
    local lang=$2
    local kind=$3
    local completed_file=$(get_completed_file "$lang" "$kind")
    grep -q "^$video_id$" "$completed_file" 2>/dev/null
}

is_nonexistent() {
    local video_id=$1
    local lang=$2
    local kind=$3
    local nonexistent_file=$(get_nonexistent_file "$lang" "$kind")
    grep -q "^$video_id$" "$nonexistent_file" 2>/dev/null
}

mark_completed() {
    local video_id=$1
    local lang=$2
    local kind=$3
    local completed_file=$(get_completed_file "$lang" "$kind")
    echo "$video_id" >> "$completed_file"
}

mark_nonexistent() {
    local video_id=$1
    local lang=$2
    local kind=$3
    local nonexistent_file=$(get_nonexistent_file "$lang" "$kind")
    echo "$video_id" >> "$nonexistent_file"
}

should_skip_download() {
    local video_id=$1
    local lang=$2
    local kind=$3
    is_completed "$video_id" "$lang" "$kind" || is_nonexistent "$video_id" "$lang" "$kind"
}


quick_video_check() {
    local video_id=$1
    local url="https://www.youtube.com/watch?v=$video_id"
    local error_output
    if yt-dlp \
        --cookies-from-browser chrome \
        --quiet \
        --no-warnings \
        --skip-download \
        --retries 2 \
        --socket-timeout 15 \
        --print title "$url" >/dev/null 2>&1; then
        return 0
    else
        error_output=$(yt-dlp \
            --cookies-from-browser chrome \
            --quiet \
            --no-warnings \
            --skip-download \
            --retries 2 \
            --socket-timeout 15 \
            --print title "$url" 2>&1)


        # Bot-check / rate limit / network as temporary
        if echo "$error_output" | grep -qi "confirm you're not a bot\|Please try again later\|HTTP Error 4\|HTTP Error 5\|timeout\|temporarily unavailable\|rate limit"; then
            echo "$video_id" >> "$TEMP_UNAVAILABLE_FILE"
            return 2
        fi
        # Permanent unavailable signals
        if echo "$error_output" | grep -qi "unavailable\|removed\|deleted\|private\|not found"; then
            return 1
        fi
        # Default to temporary if unknown
        echo "$video_id" >> "$TEMP_UNAVAILABLE_FILE"
        return 2
    fi
}

# Detect if a video exposes no captions at all (neither manual nor auto)
has_any_captions() {
    local video_id=$1
    local url="https://www.youtube.com/watch?v=$video_id"
    # Query metadata; if either field contains entries, captions exist
    local json_output
    json_output=$(yt-dlp --cookies-from-browser chrome -J "$url" 2>/dev/null || true)
    # If yt-dlp completely fails, assume unknown (treat as has captions to attempt download)
    if [ -z "$json_output" ]; then
        return 0
    fi
    # Grep lightweight check to avoid jq dependency: look for keys with non-empty arrays/objects
    # If both subtitles and automatic_captions are missing or empty, return 1 (no captions)
    if echo "$json_output" | grep -q '"subtitles"\s*:\s*{'; then
        # presence of key; might still be empty but many cases include langs; treat as captions exist
        return 0
    fi
    if echo "$json_output" | grep -q '"automatic_captions"\s*:\s*{'; then
        return 0
    fi
    return 1
}

download_subs_for_lang() {
    local video_id=$1
    local lang=$2
    local url="https://www.youtube.com/watch?v=$video_id"
    local out_dir="$TARGET_DIR/$lang"

    mkdir -p "$out_dir"

    # Helper to finalize a downloaded subtitle: convert name to *_manual.srt or *_auto.srt
    finalize_sub() {
        local kind=$1  # manual|auto
        local base="$out_dir/${video_id}"
        local lang_suffix=".${lang}"
        local src_srt="${base}${lang_suffix}.srt"
        local src_vtt="${base}${lang_suffix}.vtt"
        local dst_srt_manual="${base}_manual.srt"
        local dst_srt_auto="${base}_auto.srt"

        if [ "$kind" = "manual" ]; then
            if [ -f "$src_srt" ]; then mv -f "$src_srt" "$dst_srt_manual"; mark_completed "$video_id" "$lang" "manual"; return 0; fi
            if [ -f "$src_vtt" ]; then mv -f "$src_vtt" "${base}_manual.vtt"; mark_completed "$video_id" "$lang" "manual"; return 0; fi
        else
            if [ -f "$src_srt" ]; then mv -f "$src_srt" "$dst_srt_auto"; mark_completed "$video_id" "$lang" "auto"; return 0; fi
            if [ -f "$src_vtt" ]; then mv -f "$src_vtt" "${base}_auto.vtt"; mark_completed "$video_id" "$lang" "auto"; return 0; fi
        fi
        return 1
    }

    # Download subtitles for a specific kind (manual or auto)
    download_kind() {
        local kind=$1
        local write_flag=""
        local log_msg=""
        
        if [ "$kind" = "manual" ]; then
            write_flag="--write-sub"
            log_msg="manual subtitles"
        else
            write_flag="--write-auto-sub"
            log_msg="auto subtitles"
        fi

        log "Downloading $log_msg for $video_id ($lang)"
        
        # Capture yt-dlp output to check for specific errors
        local yt_dlp_output=$(yt-dlp \
            --cookies-from-browser chrome \
            --quiet \
            --no-warnings \
            --skip-download \
            $write_flag \
            --sub-langs "$lang" \
            --sub-format srt \
            --retries 3 \
            --fragment-retries 3 \
            --socket-timeout 30 \
            -o "$out_dir/%(id)s.%(ext)s" \
            "$url" 2>&1)
        
        local yt_dlp_exit_code=$?
        
        # Check if subtitle was actually downloaded
        if finalize_sub "$kind"; then
            log "$log_msg saved for $video_id ($lang)"
            return 0
        fi
        
        # If yt-dlp succeeded but no file was created, subtitle doesn't exist
        if [ $yt_dlp_exit_code -eq 0 ]; then
            log "No $log_msg available for $video_id ($lang) - marking as nonexistent"
            mark_nonexistent "$video_id" "$lang" "$kind"
            return 1
        fi
        
        # If yt-dlp failed, it's a download error (not nonexistent)
        log_error "Download error for $log_msg $video_id ($lang): $yt_dlp_output"
        echo "${video_id}:${lang}:${kind}" >> "$FAILED_FILE"
        return 2
    }

    # Try manual subtitles
    if ! should_skip_download "$video_id" "$lang" "manual"; then
        download_kind "manual"
    fi

    # Try auto subtitles  
    if ! should_skip_download "$video_id" "$lang" "auto"; then
        download_kind "auto"
    fi

    # Return success if at least one kind was completed
    if is_completed "$video_id" "$lang" "manual" || is_completed "$video_id" "$lang" "auto"; then
        return 0
    else
        return 1
    fi
}

cleanup() {
    log "Script interrupted. Cleaning up..."
    exit 1
}

trap cleanup SIGINT SIGTERM

process_db() {
    init_for_db "$1"

    log "Starting subtitles download..."

    # Get all videos from database
    video_rows=$(sqlite3 "$DB" "SELECT rowid, video_id FROM videos ORDER BY rowid")
    total_videos=$(sqlite3 "$DB" "SELECT COUNT(*) FROM videos")

    log "Total videos in database: $total_videos"

    # Normalize languages into array
    IFS=',' read -r -a LANGS <<< "$LANGS_CSV"

    processed_count=0
    skipped_count=0

    while IFS= read -r line; do
        [ -z "$line" ] && continue

        current_rowid=$(echo "$line" | cut -d'|' -f1)
        video_id=$(echo "$line" | cut -d'|' -f2)

        log "Processing row $current_rowid: $video_id"

        # FAST-PATH: If all requested subtitles are either completed or marked nonexistent,
        # skip immediately without any network calls.
        needs_processing=false
        for lang in "${LANGS[@]}"; do
            lang_trimmed="${lang//[[:space:]]/}"
            [ -z "$lang_trimmed" ] && continue
            
            if ! should_skip_download "$video_id" "$lang_trimmed" "manual" || ! should_skip_download "$video_id" "$lang_trimmed" "auto"; then
                needs_processing=true
                break
            fi
        done

        if [ "$needs_processing" = false ]; then
            log "Skipping $video_id - all requested subtitles already completed or marked nonexistent"
            skipped_count=$((skipped_count + 1))
            continue
        fi


        # Quick existence check with classification
        quick_video_check "$video_id"
        qvc_status=$?
        if [ $qvc_status -eq 1 ]; then
            log "Video $video_id appears permanently unavailable - marking as deleted and skipping"
            skipped_count=$((skipped_count + 1))
            continue
        elif [ $qvc_status -eq 2 ]; then
            log "Video $video_id temporarily unavailable (geo/rate/bot) - will retry next runs"
            skipped_count=$((skipped_count + 1))
            continue
        fi

        # Attempt for each requested language
        for lang in "${LANGS[@]}"; do
            lang_trimmed="${lang//[[:space:]]/}"
            [ -z "$lang_trimmed" ] && continue

            download_subs_for_lang "$video_id" "$lang_trimmed"
            # small jitter to be nice
            sleep_time=$((RANDOM % 3 + 1))
            sleep $sleep_time
        done

        processed_count=$((processed_count + 1))

        # Periodic status
        if [ $((processed_count % 10)) -eq 0 ]; then
            failed_count=$(wc -l < "$FAILED_FILE" 2>/dev/null || echo 0)
            log "Progress: Processed $processed_count videos, skipped $skipped_count, $failed_count failed," 
        fi
    done <<< "$video_rows"

    # Final statistics
    failed_count=$(wc -l < "$FAILED_FILE" 2>/dev/null || echo 0)

    log "Subtitles download completed"
    log "Processed videos: $processed_count"
    log "Skipped videos: $skipped_count"
    log "Failed downloads: $failed_count"
    log "Failed entries saved in $FAILED_FILE"
    log "Completed subtitles tracked in completed_*_*_subs.txt files"
    log "Nonexistent subtitles tracked in nonexistent_*_*_subs.txt files"
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
            echo "\\n===== Processing DB: $db_line_trimmed ====="
            process_db "$db_line_trimmed"
        else
            echo "Skipping missing DB path: $db_line_trimmed" >&2
        fi
    done < "$DB_LIST_FILE"
fi


