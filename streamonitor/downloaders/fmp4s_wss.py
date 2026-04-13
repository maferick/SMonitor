import json
import os
import subprocess
from threading import Thread
from websocket import create_connection, WebSocketConnectionClosedException, WebSocketException
from contextlib import ExitStack, closing
from ffmpy import FFmpeg, FFRuntimeError
from parameters import DEBUG, CONTAINER, SEGMENT_TIME, FFMPEG_PATH
from streamonitor.downloaders.segment_cleanup import cleanup_small_segments, get_segment_snapshot


def getVideoWSSVR(self, url, filename):
    self.stopDownloadFlag = False
    error = False
    url = url.replace('fmp4s://', 'wss://')

    suffix = ''
    if hasattr(self, 'filename_extra_suffix'):
        suffix = self.filename_extra_suffix

    basefilename = filename[:-len('.' + CONTAINER)]
    filename = basefilename + suffix + '.' + CONTAINER
    tmpfilename = basefilename + '.tmp.mp4'

    def debug_(message):
        self.debug(message, filename + '.log')

    def execute():
        nonlocal error
        with open(tmpfilename, 'wb') as outfile:
            while not self.stopDownloadFlag:
                try:
                    with closing(create_connection(url, timeout=10)) as conn:
                        conn.send('{"url":"stream/hello","version":"0.0.1"}')
                        while not self.stopDownloadFlag:
                            t = conn.recv()
                            try:
                                tj = json.loads(t)
                                if 'url' in tj:
                                    if tj['url'] == 'stream/qual':
                                        conn.send('{"quality":"test","url":"stream/play","version":"0.0.1"}')
                                        debug_('Connection opened')
                                        break
                                if 'message' in tj:
                                    if tj['message'] == 'ping':
                                        debug_('Server is not ready or there was a change')
                                        error = True
                                        return
                            except (TypeError, json.JSONDecodeError):
                                debug_('Failed to open the connection')
                                error = True
                                return

                        while not self.stopDownloadFlag:
                            outfile.write(conn.recv())
                except WebSocketConnectionClosedException:
                    debug_('WebSocket connection closed - try to continue')
                    continue
                except WebSocketException as wex:
                    debug_('Error when downloading')
                    debug_(wex)
                    error = True
                    return

    def terminate():
        self.stopDownloadFlag = True

    process = Thread(target=execute)
    process.start()
    self.stopDownload = terminate
    process.join()
    self.stopDownload = None

    if error:
        return False
    if not os.path.exists(tmpfilename) or os.path.getsize(tmpfilename) == 0:
        if os.path.exists(tmpfilename):
            os.remove(tmpfilename)
        return False

    # Post-processing
    try:
        with ExitStack() as stack:
            stdout = stack.enter_context(open(filename + '.postprocess_stdout.log', 'w+')) if DEBUG else subprocess.DEVNULL
            stderr = stack.enter_context(open(filename + '.postprocess_stderr.log', 'w+')) if DEBUG else subprocess.DEVNULL
            output_str = '-c:a copy -c:v copy'
            segment_pattern = None
            previous_segments = set()
            if SEGMENT_TIME is not None:
                output_str += f' -f segment -reset_timestamps 1 -segment_time {str(SEGMENT_TIME)}'
                segment_pattern = basefilename + '_*' + suffix + '.' + CONTAINER
                previous_segments = get_segment_snapshot(segment_pattern)
                filename = basefilename + '_%03d' + suffix + '.' + CONTAINER
            ff = FFmpeg(executable=FFMPEG_PATH, inputs={tmpfilename: '-ignore_editlist 1'}, outputs={filename: output_str})
            ff.run(stdout=stdout, stderr=stderr)
        if segment_pattern:
            cleanup_small_segments(segment_pattern, previous_segments, logger=self.log)
        os.remove(tmpfilename)
    except FFRuntimeError as e:
        if e.exit_code and e.exit_code != 255:
            return False

    return True
