import m3u8
import os
import subprocess
from threading import Thread
from ffmpy import FFmpeg, FFRuntimeError
from time import sleep
from contextlib import ExitStack
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from parameters import DEBUG, CONTAINER, SEGMENT_TIME, FFMPEG_PATH

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
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    request_exception = getattr(getattr(requests, "exceptions", None), "RequestException", Exception)

    def execute():
        nonlocal error
        downloaded_segments = set()
        consecutive_failures = 0
        with open(tmpfilename, 'wb') as outfile:
            while not self.stopDownloadFlag:
                did_download = False
                try:
                    r = session.get(url, headers=self.headers, cookies=self.cookies, timeout=30)
                    if r.status_code != 200:
                        consecutive_failures += 1
                        if consecutive_failures >= 3:
                            error = True
                            return
                        sleep(2)
                        continue
                    content = r.content.decode("utf-8")
                except (request_exception, UnicodeDecodeError):
                    consecutive_failures += 1
                    if consecutive_failures >= 3:
                        error = True
                        return
                    sleep(2)
                    continue
                if m3u_processor:
                    content = m3u_processor(content)
                chunklist = m3u8.loads(content)
                if len(chunklist.segments) == 0:
                    return
                consecutive_failures = 0
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
                        continue
                    if m.status_code != 200:
                        continue
                    outfile.write(m.content)
                    if self.stopDownloadFlag:
                        return
                if not did_download:
                    sleep(10)

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
            if SEGMENT_TIME is not None:
                output_str += f' -f segment -reset_timestamps 1 -segment_time {str(SEGMENT_TIME)}'
                if hasattr(self, 'filename_extra_suffix'):
                    suffix = self.filename_extra_suffix
                filename = filename[:-len('.' + CONTAINER)] + '_%03d' + suffix + '.' + CONTAINER
            ff = FFmpeg(executable=FFMPEG_PATH, inputs={tmpfilename: None}, outputs={filename: output_str})
            ff.run(stdout=stdout, stderr=stderr)
        os.remove(tmpfilename)
    except FFRuntimeError as e:
        if e.exit_code and e.exit_code != 255:
            return False

    return True
