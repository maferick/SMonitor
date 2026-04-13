import m3u8
import os
import subprocess
from threading import Thread
from ffmpy import FFmpeg, FFRuntimeError
from time import sleep, monotonic
from contextlib import ExitStack
from parameters import DEBUG, CONTAINER, SEGMENT_TIME, FFMPEG_PATH, STREAM_HICCUP_GRACE
from streamonitor.downloaders.segment_cleanup import cleanup_small_segments, get_segment_snapshot

_http_lib = None
if not _http_lib:
    try:
        import pycurl_requests as requests
        _http_lib = 'pycurl'
    except ImportError:
        pass
if not _http_lib:
    try:
        import requests
        _http_lib = 'requests'
    except ImportError:
        pass
if not _http_lib:
    raise ImportError("Please install requests or pycurl package to proceed")


def getVideoNativeHLS(self, url, filename, m3u_processor=None):
    self.stopDownloadFlag = False
    error = False
    tmpfilename = filename[:-len('.' + CONTAINER)] + '.tmp.ts'
    session = requests.Session()
    request_exception = getattr(getattr(requests, "exceptions", None), "RequestException", Exception)

    def execute():
        nonlocal error
        downloaded_segments = set()
        last_success_time = monotonic()
        with open(tmpfilename, 'wb') as outfile:
            while not self.stopDownloadFlag:
                did_download = False
                try:
                    r = session.get(url, headers=self.headers, cookies=self.cookies, timeout=30)
                    if r.status_code != 200:
                        if monotonic() - last_success_time >= STREAM_HICCUP_GRACE:
                            error = True
                            return
                        sleep(2)
                        continue
                    content = r.content.decode("utf-8")
                except (request_exception, UnicodeDecodeError):
                    if monotonic() - last_success_time >= STREAM_HICCUP_GRACE:
                        error = True
                        return
                    sleep(2)
                    continue
                if m3u_processor:
                    content = m3u_processor(content)
                chunklist = m3u8.loads(content)
                if len(chunklist.segments) == 0:
                    if monotonic() - last_success_time >= STREAM_HICCUP_GRACE:
                        return
                    sleep(2)
                    continue
                should_retry_playlist = False
                for chunk in chunklist.segment_map + chunklist.segments:
                    if chunk.uri in downloaded_segments:
                        continue
                    did_download = True
                    downloaded_segments.add(chunk.uri)
                    chunk_uri = chunk.uri
                    self.debug('Downloading ' + chunk_uri)
                    if not chunk_uri.startswith("https://"):
                        chunk_uri = '/'.join(url.split('.m3u8')[0].split('/')[:-1]) + '/' + chunk_uri
                    try:
                        m = session.get(chunk_uri, headers=self.headers, cookies=self.cookies, timeout=30)
                    except request_exception:
                        if monotonic() - last_success_time >= STREAM_HICCUP_GRACE:
                            error = True
                            return
                        should_retry_playlist = True
                        break
                    if m.status_code != 200:
                        if monotonic() - last_success_time >= STREAM_HICCUP_GRACE:
                            error = True
                            return
                        should_retry_playlist = True
                        break
                    outfile.write(m.content)
                    last_success_time = monotonic()
                    if self.stopDownloadFlag:
                        return
                if should_retry_playlist:
                    sleep(2)
                    continue
                if not did_download:
                    if monotonic() - last_success_time >= STREAM_HICCUP_GRACE:
                        return
                    sleep(2)

    def terminate():
        self.stopDownloadFlag = True

    process = Thread(target=execute)
    process.start()
    self.stopDownload = terminate
    process.join()
    self.stopDownload = None
    session.close()

    if error:
        return False

    if not os.path.exists(tmpfilename):
        return False

    if os.path.getsize(tmpfilename) == 0:
        os.remove(tmpfilename)
        return False

    # Post-processing
    try:
        with ExitStack() as stack:
            stdout = stack.enter_context(open(filename + '.postprocess_stdout.log', 'w+')) if DEBUG else subprocess.DEVNULL
            stderr = stack.enter_context(open(filename + '.postprocess_stderr.log', 'w+')) if DEBUG else subprocess.DEVNULL
            output_str = '-c:a copy -c:v copy'
            suffix = ''
            segment_pattern = None
            previous_segments = set()
            if SEGMENT_TIME is not None:
                output_str += f' -f segment -reset_timestamps 1 -segment_time {str(SEGMENT_TIME)}'
                if hasattr(self, 'filename_extra_suffix'):
                    suffix = self.filename_extra_suffix
                segment_pattern = filename[:-len('.' + CONTAINER)] + '_*' + suffix + '.' + CONTAINER
                previous_segments = get_segment_snapshot(segment_pattern)
                filename = filename[:-len('.' + CONTAINER)] + '_%03d' + suffix + '.' + CONTAINER
            ff = FFmpeg(executable=FFMPEG_PATH, inputs={tmpfilename: None}, outputs={filename: output_str})
            ff.run(stdout=stdout, stderr=stderr)
        if segment_pattern:
            cleanup_small_segments(segment_pattern, previous_segments, logger=self.log)
        os.remove(tmpfilename)
    except FFRuntimeError as e:
        if e.exit_code and e.exit_code != 255:
            return False

    return True
