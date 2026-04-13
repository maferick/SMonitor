import glob
import os

from parameters import MIN_SEGMENT_FILE_MIB


def get_segment_snapshot(pattern):
    return set(glob.glob(pattern))


def cleanup_small_segments(pattern, previous_snapshot, logger=None):
    if MIN_SEGMENT_FILE_MIB <= 0:
        return 0

    current_files = set(glob.glob(pattern))
    new_files = [path for path in (current_files - previous_snapshot) if os.path.isfile(path)]
    if len(new_files) <= 1:
        return 0

    min_size_bytes = int(MIN_SEGMENT_FILE_MIB * 1024 * 1024)
    if min_size_bytes <= 0:
        return 0

    largest = max(new_files, key=lambda p: os.path.getsize(p))
    deleted = 0
    for path in new_files:
        if path == largest:
            continue
        if os.path.getsize(path) < min_size_bytes:
            os.remove(path)
            deleted += 1

    if deleted and logger:
        logger(f"Removed {deleted} small segment file(s) under {MIN_SEGMENT_FILE_MIB} MiB")
    return deleted
