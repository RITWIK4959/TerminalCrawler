import os
import threading
import queue
import time
import gzip
import io
from typing import Optional, List, Dict, Any
from collections import deque

import json
import requests
import logging
from logging.handlers import RotatingFileHandler
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urldefrag, urlparse
import xml.etree.ElementTree as ET

from scraper import scrape_html
from db import CrawlerDB


USER_AGENT = "TerminalCrawler/1.0 (+https://example.com/bot)"  # change to your own identifier if desired
MAX_RETRIES = 3


class Crawler:
    def __init__(self, db_path: str, num_workers: int, delay_seconds: float):
        self.db_path = db_path
        self.num_workers = num_workers
        self.delay_seconds = delay_seconds

        self.json_lock = threading.Lock()
        self.json_path = os.path.join(os.path.dirname(self.db_path), "scraped_data.jsonl")
        self.db = CrawlerDB(self.db_path)

        # Determine a "main domain" for UI filtering: infer from earliest DB entry if available
        self.main_domain: Optional[str] = None
        try:
            first = self.db.get_earliest_url()
            if first:
                p = urlparse(first)
                host = p.netloc.lower()
                if host.startswith("www."):
                    host = host[4:]
                self.main_domain = host
        except Exception:
            self.main_domain = None

        # Setup file logger per crawler instance (rotating)
        log_dir = os.path.dirname(self.db_path)
        self.log_path = os.path.join(log_dir, "crawler.log")
        self.logger = logging.getLogger(f"crawler.{id(self)}")
        self.logger.setLevel(logging.INFO)
        # Avoid adding multiple handlers if logger is reused in same process
        if not any(isinstance(h, RotatingFileHandler) and getattr(h, 'baseFilename', None) == os.path.abspath(self.log_path) for h in self.logger.handlers):
            handler = RotatingFileHandler(self.log_path, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8")
            fmt = logging.Formatter("%(asctime)s %(levelname)s [%(threadName)s] %(message)s")
            handler.setFormatter(fmt)
            self.logger.addHandler(handler)

        self.logger.info(f"Initialized Crawler(db={self.db_path!r}, workers={self.num_workers}, delay={self.delay_seconds})")

        self.pending_queue: "queue.Queue[str]" = queue.Queue()
        self.stop_event = threading.Event()
        self.workers: List[threading.Thread] = []

        self._load_pending_into_queue()

    def _load_pending_into_queue(self) -> None:
        for url in self.db.load_pending_urls():
            self.pending_queue.put(url)

    def _save_json_record(self, data: Dict[str, Any]) -> None:
        with self.json_lock:
            with open(self.json_path, "a", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
                f.write("\n")
        # Log saved record (core fields)
        try:
            self.logger.info(f"Saved record: {data.get('url')!r} status={data.get('status_code')}")
        except Exception:
            pass

    def get_status_counts(self):
        return self.db.get_status_counts()

    # ------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------
    def add_seed_url(self, url: str) -> None:
        url = self._normalize_url(url)
        if not url:
            print("Invalid URL.")
            return

        is_sitemap = 1 if self._looks_like_sitemap(url) else 0
        inserted = self.db.insert_or_ignore_url(url, status="pending", is_sitemap=is_sitemap)
        if inserted:
            self.pending_queue.put(url)
            print(f"Seeded URL: {url}")
            self.logger.info(f"Seeded URL: {url}")
            # if we don't yet have a main_domain, use the first seed's host
            if not self.main_domain:
                try:
                    p = urlparse(url)
                    host = p.netloc.lower()
                    if host.startswith("www."):
                        host = host[4:]
                    self.main_domain = host
                except Exception:
                    pass
        else:
            print(f"URL already known (skipped): {url}")
            self.logger.debug(f"Seed skipped (exists): {url}")

    def pause_url(self, url: str, reason: str = "user-pause") -> None:
        url = self._normalize_url(url)
        row = self.db.get_url_row(url)
        if not row:
            print(f"URL not found in DB: {url}")
            return

        self.db.update_url_status(url, "paused", pause_reason=reason)
        print(f"Paused URL: {url}")
        self.logger.info(f"Paused URL: {url} reason={reason}")

    def pause_prefix(self, prefix: str, reason: str = "user-pause-prefix") -> None:
        affected = self.db.pause_prefix(prefix, reason)
        # Also remove matching items from the in-memory pending queue so workers don't
        # pick them up after a pause. This is best-effort and uses the queue's mutex
        # to mutate the underlying deque atomically.
        removed = self._remove_from_pending_queue(prefix)
        print(f"Paused {affected} URL(s) with prefix: {prefix} (removed {removed} from in-memory queue)")
        self.logger.info(
            f"Paused {affected} URL(s) with prefix: {prefix} reason={reason} removed_from_queue={removed}"
        )

    def _remove_from_pending_queue(self, prefix: str) -> int:
        """
        Remove queued URLs that start with `prefix` from the in-memory pending queue.
        Returns the number removed. This manipulates the queue internals under the
        queue mutex to avoid race conditions with consumer threads.
        """
        removed = 0
        try:
            with self.pending_queue.mutex:
                q = self.pending_queue.queue  # this is a collections.deque
                new_q = deque()
                while q:
                    try:
                        u = q.popleft()
                    except IndexError:
                        break
                    if isinstance(u, str) and u.startswith(prefix):
                        removed += 1
                    else:
                        new_q.append(u)
                # replace underlying deque
                self.pending_queue.queue = new_q
        except Exception as e:
            # Log and continue — removal is best-effort
            try:
                self.logger.exception(f"Error removing from pending queue for prefix {prefix}: {e}")
            except Exception:
                pass
        return removed

    def resume_url(self, url: str) -> None:
        url = self._normalize_url(url)
        row = self.db.get_url_row(url)
        if not row:
            print(f"URL not found in DB: {url}")
            return
        if row[2] != "paused":
            print(f"URL is not paused: {url}")
            return

        self.db.update_url_status(url, "pending", clear_pause_reason=True)
        self.pending_queue.put(url)
        print(f"Resumed URL: {url}")
        self.logger.info(f"Resumed URL: {url}")

    def resume_prefix(self, prefix: str) -> None:
        urls, affected = self.db.resume_prefix(prefix)

        for u in urls:
            self.pending_queue.put(u)
        self.logger.info(f"Resumed {affected} URL(s) with prefix: {prefix}")
        print(f"Resumed {affected} URL(s) with prefix: {prefix}")

    def print_status(self) -> None:
        pending, visited, paused, error = self.get_status_counts()
        print(
            f"Workers: {self.num_workers} | "
            f"Pending: {pending} | Visited: {visited} | "
            f"Paused: {paused} | Error: {error}"
        )

    def get_stats(self, top_n: int = 10) -> Dict[str, Any]:
        """Return a stats dictionary with totals and breakdowns.
        - totals: pending/visited/paused/error/total
        - earliest_seed: earliest URL in DB (or None)
        - top_paused_domains: list of (domain, count)
        - paused_prefixes: list of (prefix, count) where prefix is host[/first_path_segment]
        - domain_distribution: list of (domain, count) across all URLs
        """
        pending, visited, paused, error = self.get_status_counts()
        total = pending + visited + paused + error

        earliest = None
        try:
            earliest = self.db.get_earliest_url()
        except Exception:
            earliest = None

        paused_urls = []
        try:
            paused_urls = self.db.list_paused_urls()
        except Exception:
            paused_urls = []

        from collections import Counter

        paused_domains = []
        paused_prefixes = []
        for u in paused_urls:
            try:
                p = urlparse(u)
                h = p.netloc.lower()
                if h.startswith('www.'):
                    h = h[4:]
                paused_domains.append(h)
                # construct prefix = host[/first_path_segment]
                path = p.path or ""
                seg = [s for s in path.split('/') if s]
                if seg:
                    pref = f"{h}/{seg[0]}"
                else:
                    pref = h
                paused_prefixes.append(pref)
            except Exception:
                continue

        top_paused_domains = Counter(paused_domains).most_common(top_n)
        top_paused_prefixes = Counter(paused_prefixes).most_common(top_n)

        # domain distribution across all URLs
        all_domains = []
        try:
            with self.db.db_lock:
                cur = self.db.conn.cursor()
                cur.execute("SELECT url FROM urls")
                rows = cur.fetchall()
            for (u,) in rows:
                try:
                    p = urlparse(u)
                    h = p.netloc.lower()
                    if h.startswith('www.'):
                        h = h[4:]
                    all_domains.append(h)
                except Exception:
                    continue
        except Exception:
            all_domains = []

        domain_distribution = Counter(all_domains).most_common(top_n)

        return {
            "totals": {"total": total, "pending": pending, "visited": visited, "paused": paused, "error": error},
            "earliest_seed": earliest,
            "top_paused_domains": top_paused_domains,
            "top_paused_prefixes": top_paused_prefixes,
            "domain_distribution": domain_distribution,
        }

    def print_stats(self, top_n: int = 10) -> None:
        s = self.get_stats(top_n=top_n)
        t = s["totals"]
        print("\n=== Crawler Stats ===")
        print(f"Total URLs: {t['total']}")
        print(f"  Pending: {t['pending']}  Visited: {t['visited']}  Paused: {t['paused']}  Error: {t['error']}")
        print(f"Earliest seed: {s['earliest_seed']!r}")
        print("\nTop paused domains:")
        for dom, cnt in s["top_paused_domains"]:
            print(f"  {dom}: {cnt}")
        print("\nTop paused prefixes (host[/first_segment]):")
        for pref, cnt in s["top_paused_prefixes"]:
            print(f"  {pref}: {cnt}")
        print("\nTop domains overall:")
        for dom, cnt in s["domain_distribution"]:
            print(f"  {dom}: {cnt}")
        print()  # trailing blank line

    # Convenience helpers for startup / menus
    def list_paused_urls(self):
        return self.db.list_paused_urls()

    def resume_all_paused(self) -> int:
        urls = self.db.resume_all_paused()
        for u in urls:
            self.pending_queue.put(u)
        if urls:
            print(f"Resumed {len(urls)} paused URL(s).")
            self.logger.info(f"Resumed all paused: {len(urls)} URL(s)")
        return len(urls)

    def resume_paused_for_domain(self, domain: str) -> int:
        """
        Resume only paused URLs whose host matches `domain` (exact or subdomain match).
        Returns the number resumed.
        """
        resumed = 0
        try:
            paused_urls = self.db.list_paused_urls()
        except Exception:
            paused_urls = []

        for u in paused_urls:
            try:
                h = urlparse(u).netloc.lower()
                if h.startswith("www."):
                    h = h[4:]
            except Exception:
                continue

            # match exact or subdomain
            if h == domain or h.endswith("." + domain):
                self.db.update_url_status(u, "pending", clear_pause_reason=True)
                self.pending_queue.put(u)
                resumed += 1

        if resumed:
            self.logger.info(f"Resumed {resumed} paused URL(s) for domain {domain}")
            print(f"Resumed {resumed} paused URL(s) for domain {domain}")

        return resumed

    def start_workers(self) -> None:
        for i in range(self.num_workers):
            t = threading.Thread(target=self._worker_loop, name=f"worker-{i+1}", daemon=True)
            self.workers.append(t)
            t.start()
        self.logger.info(f"Started {len(self.workers)} worker threads")

    def stop(self) -> None:
        print("Stopping crawler... (workers will finish current tasks)")
        self.stop_event.set()
        # No need to join daemon threads strictly; they exit with main.
        self.db.close()
        self.logger.info("Stopping crawler and closing DB")
        print("State saved. Exiting.")

    # ------------------------------------------------------------
    # Worker logic
    # ------------------------------------------------------------
    def _worker_loop(self) -> None:
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})

        while not self.stop_event.is_set():
            try:
                url = self.pending_queue.get(timeout=1.0)
            except queue.Empty:
                continue

            try:
                self._process_url_with_db_check(session, url)
            except Exception as e:
                # Catch-all to avoid killing the worker thread
                self.logger.exception(f"Unexpected error in worker for {url}: {e}")
            finally:
                try:
                    self.pending_queue.task_done()
                except Exception:
                    pass

    def _process_url_with_db_check(self, session: requests.Session, url: str) -> None:
        row = self.db.get_url_row(url)
        if not row:
            # URL somehow removed; skip
            return

        status = row[2]
        retry_count = row[5] if len(row) > 5 else 0
        is_sitemap = row[6] if len(row) > 6 else 0

        # Skip anything that is not pending
        if status != "pending":
            return

        # Respect pause commands that might have just come in
        row_after = self.db.get_url_row(url)
        if row_after and row_after[2] != "pending":
            return

        # Crawl delay between requests per thread
        if self.delay_seconds > 0:
            time.sleep(self.delay_seconds)

        success = False
        error_msg: Optional[str] = None

        try:
            self.logger.info(f"[{threading.current_thread().name}] Fetching: {url}")
            resp = session.get(url, timeout=15)
            if resp.status_code != 200:
                raise requests.RequestException(f"HTTP {resp.status_code}")

            content_type = resp.headers.get("Content-Type", "")
            raw_bytes = resp.content

            if self._looks_like_sitemap(url) or "xml" in content_type:
                self._handle_sitemap_content(url, raw_bytes)
                is_sitemap = 1
            else:
                text = self._decode_body(raw_bytes, resp.encoding)
                # Scrape core data and save to JSON
                data = scrape_html(url, text, resp.status_code)
                self._save_json_record(data)
                # Then extract links and log snippet as before
                self._handle_html_content(url, text)
                is_sitemap = 0

            success = True
        except Exception as e:
            error_msg = str(e)
            self.logger.warning(f"[{threading.current_thread().name}] Error fetching {url}: {error_msg}")

        if success:
            self.db.update_url_status(url, "visited", last_error=None, is_sitemap=is_sitemap)
            self.logger.info(f"Visited: {url}")
        else:
            if retry_count + 1 < MAX_RETRIES:
                # schedule another retry
                self.db.update_url_status(
                    url,
                    "pending",
                    last_error=error_msg,
                    increment_retry=True,
                )
                self.pending_queue.put(url)
                self.logger.info(f"Re-queued for retry: {url} (retry_count now +1)")
            else:
                self.db.update_url_status(
                    url,
                    "error",
                    last_error=error_msg,
                    increment_retry=True,
                )
                self.logger.error(f"Marked error after retries: {url} error={error_msg}")
    # ------------------------------------------------------------
    # Content handling
    # ------------------------------------------------------------
    def _decode_body(self, raw: bytes, encoding: Optional[str]) -> str:
        if not encoding:
            encoding = "utf-8"
        try:
            return raw.decode(encoding, errors="replace")
        except LookupError:
            return raw.decode("utf-8", errors="replace")

    def _handle_html_content(self, base_url: str, html: str) -> None:
        soup = BeautifulSoup(html, "html.parser")

        # Extract plain text (example: just print first 200 chars)
        text = soup.get_text(separator=" ", strip=True)
        snippet = text[:200].replace("\n", " ")
        print(f"[CONTENT] {base_url} :: {len(text)} chars, snippet: {snippet!r}")

        # Extract links
        for a in soup.find_all("a", href=True):
            href = a["href"]
            new_url = urljoin(base_url, href)
            new_url, _ = urldefrag(new_url)  # drop fragments
            new_url = self._normalize_url(new_url)
            if not new_url:
                continue
            # filter non-http(s)
            if not (new_url.startswith("http://") or new_url.startswith("https://")):
                continue

            self._enqueue_new_url(new_url)

    def _handle_sitemap_content(self, url: str, raw_bytes: bytes) -> None:
        # Decompress if gzip
        if url.endswith(".gz"):
            try:
                with gzip.GzipFile(fileobj=io.BytesIO(raw_bytes)) as gz:
                    raw_bytes = gz.read()
            except OSError:
                # not actually gzip, fall back to raw
                pass

        try:
            text = raw_bytes.decode("utf-8", errors="replace")
            root = ET.fromstring(text)
        except Exception as e:
            print(f"[SITEMAP] Failed to parse {url}: {e}")
            return

        tag = root.tag.lower()
        if tag.endswith("sitemapindex"):
            # sitemap index: contains <sitemap><loc>...</loc></sitemap>
            loc_tag = self._tag_with_ns(root, "loc")
            sitemap_tag = self._tag_with_ns(root, "sitemap")
            for sm in root.findall(sitemap_tag):
                loc_el = sm.find(loc_tag)
                if loc_el is None or not loc_el.text:
                    continue
                sm_url = self._normalize_url(loc_el.text.strip())
                if not sm_url:
                    continue
                self._enqueue_new_url(sm_url, is_sitemap=1)
            print(f"[SITEMAP] Parsed sitemap index: {url}")
        elif tag.endswith("urlset"):
            # standard sitemap: contains <url><loc>...</loc></url>
            url_tag = self._tag_with_ns(root, "url")
            loc_tag = self._tag_with_ns(root, "loc")
            count = 0
            for u in root.findall(url_tag):
                loc_el = u.find(loc_tag)
                if loc_el is None or not loc_el.text:
                    continue
                page_url = self._normalize_url(loc_el.text.strip())
                if not page_url:
                    continue
                self._enqueue_new_url(page_url)
                count += 1
            print(f"[SITEMAP] Parsed sitemap: {url} -> {count} URL(s)")
        else:
            print(f"[SITEMAP] Unrecognized XML root in {url}: {root.tag}")

    def _tag_with_ns(self, root: ET.Element, local: str) -> str:
        # Root tag may look like "{namespace}urlset"
        if root.tag.startswith("{"):
            ns = root.tag.split("}", 1)[0] + "}"
            return ns + local
        return local

    # ------------------------------------------------------------
    # URL helpers
    # ------------------------------------------------------------
    def _normalize_url(self, url: str) -> str:
        url = url.strip()
        if not url:
            return ""
        # Basic normalization: remove trailing slash duplicates
        if url.startswith("http://") or url.startswith("https://"):
            # Further normalization could be added here
            return url
        return url

    def _looks_like_sitemap(self, url: str) -> bool:
        lower = url.lower()
        return lower.endswith(".xml") or lower.endswith(".xml.gz") or "sitemap" in lower

    def _enqueue_new_url(self, url: str, is_sitemap: int = 0) -> None:
        inserted = self.db.insert_or_ignore_url(url, status="pending", is_sitemap=is_sitemap)
        if inserted:
            self.pending_queue.put(url)
            self.logger.info(f"Enqueued new URL: {url}")


def determine_auto_threads() -> int:
    try:
        cpu = os.cpu_count() or 4
    except Exception:
        cpu = 4
    # network-bound, allow more threads
    return max(2, min(32, cpu * 4))

