import re
import requests
import time
import random
from streamonitor.bot import Bot
from streamonitor.enums import Status
from requests.utils import dict_from_cookiejar, cookiejar_from_dict


class Chaturbate(Bot):
    site = 'Chaturbate'
    siteslug = 'CB'

    def __init__(self, username):
        super().__init__(username)
        self.sleep_on_offline = 120
        self.sleep_on_error = 180
        self.session = requests.Session()
        self.session.trust_env = False

        self.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": f"https://chaturbate.com/{username}/",
            "Origin": "https://chaturbate.com",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        })

        self.consecutive_errors = 0
        self.last_request_time = 0
        self.min_request_interval = 20
        self.cookies_initialized = False
        self.hls_failures = 0

    def _page_headers(self):
        return {
            "User-Agent": self.headers["User-Agent"],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        }

    def _ajax_headers(self):
        csrf = None
        if self.cookies is not None:
            try:
                csrf = self.cookies.get('csrftoken')
            except Exception:
                csrf = None

        headers = {
            "User-Agent": self.headers["User-Agent"],
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "https://chaturbate.com",
            "Referer": f"https://chaturbate.com/{self.username}/",
            "X-Requested-With": "XMLHttpRequest",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }
        if csrf:
            headers["X-CSRFToken"] = csrf
        return headers

    def _normalize_cookies(self, jar):
        return cookiejar_from_dict(dict_from_cookiejar(jar))

    def getWebsiteURL(self):
        return "https://www.chaturbate.com/" + self.username

    def getPlaylistVariants(self, url):
        try:
            result = self.session.get(
                url,
                headers=self.headers,
                cookies=self.cookies,
                timeout=15
            )
            result.raise_for_status()
            return super().getPlaylistVariants(m3u_data=result.content.decode("utf-8"))
        except Exception:
            return []

    def getVideoUrl(self):
        url = self.lastInfo.get("url", "")
        if not url:
            return None
        url = url.replace('\\/', '/')
        if self.lastInfo.get('cmaf_edge'):
            url = url.replace('playlist.m3u8', 'playlist_sfm4s.m3u8')
            url = re.sub('live-.+amlst', 'live-c-fhls/amlst', url)
        return self.getWantedResolutionPlaylist(url)

    def _wait_for_rate_limit(self):
        now = time.time()
        since = now - self.last_request_time
        if since < self.min_request_interval:
            time.sleep(self.min_request_interval - since + random.uniform(1, 3))
        self.last_request_time = time.time()

    def _initialize_cookies(self):
        if self.cookies_initialized:
            return True
        try:
            r = self.session.get(
                f"https://chaturbate.com/{self.username}/",
                headers=self._page_headers(),
                timeout=30
            )
            if r.status_code == 200:
                self.cookies_initialized = True
                self.cookies = self._normalize_cookies(r.cookies)
                return True
            return False
        except Exception:
            return False

    def getStatus(self):
        self._wait_for_rate_limit()

        self._check_count = getattr(self, "_check_count", 0) + 1
        if self._check_count > 10:
            self.cookies_initialized = False
            self._check_count = 0

        if not self.cookies_initialized:
            if not self._initialize_cookies():
                self.consecutive_errors += 1
                return Status.ERROR
            time.sleep(2)

        try:
            page = self.session.get(
                f"https://chaturbate.com/{self.username}/",
                headers=self._page_headers(),
                cookies=self.cookies,
                timeout=30
            )
            if page.status_code in (403, 429):
                self.cookies_initialized = False
                self.consecutive_errors += 1
                return Status.RATELIMIT
            if page.cookies:
                self.cookies = self._normalize_cookies(page.cookies)

            r = self.session.post(
                "https://chaturbate.com/get_edge_hls_url_ajax/",
                headers=self._ajax_headers(),
                cookies=self.cookies,
                data={"room_slug": self.username, "bandwidth": "high"},
                timeout=30
            )

            if r.status_code in (429, 403):
                self.cookies_initialized = False
                self.consecutive_errors += 1
                return Status.RATELIMIT

            if r.status_code != 200:
                self.consecutive_errors += 1
                return Status.ERROR

            self.lastInfo = r.json()
            status = self.lastInfo.get("room_status", "offline")

            if status == "public":
                url = self.lastInfo.get("url", "")
                if not url:
                    self.hls_failures += 1
                    if self.hls_failures >= 2:
                        self.cookies_initialized = False
                        self._initialize_cookies()
                        self.hls_failures = 0
                    return Status.ERROR

                if r.cookies:
                    self.cookies = self._normalize_cookies(r.cookies)

                self.hls_failures = 0
                self.consecutive_errors = 0
                return Status.PUBLIC

            if status in ("private", "hidden"):
                return Status.PRIVATE

            return Status.OFFLINE

        except Exception:
            self.consecutive_errors += 1
            self.cookies_initialized = False
            return Status.ERROR

        finally:
            self.sleep_on_error = min(900, 120 * (2 ** self.consecutive_errors))
            self.ratelimit = False
