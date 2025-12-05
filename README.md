## Terminal Web Crawler (Multi-threaded, SQLite-backed)

This is a simple terminal-driven web crawler implemented in Python. It
supports multi-threaded crawling, sitemap discovery, JSONL output of scraped
content, and an SQLite-backed persistent state for pause/resume across runs.

**Highlights**
- **Threaded workers:** configurable worker count (auto-detected option available).
- **Persistent state:** URLs and statuses stored in `crawler_state.db` (SQLite, WAL).
- **Output:** scraped page summaries are appended to `scraped_data.jsonl` (one JSON object per line).
- **Sitemaps:** detects and parses standard sitemaps and sitemap indexes (handles `.xml` and `.xml.gz`).
- **Interactive CLI:** start/stop, seed/pause/resume, and realtime stats.

**Quickstart**
- **Activate virtualenv and install deps:**

```bash
cd /home/ritwik/Documents/Operating_System/OS_Final_Project
source venv/bin/activate
pip install -r requirements.txt
```

- **Run the crawler UI:**

```bash
python main.py
```

You will be prompted for a per-thread crawl delay and the number of worker
threads (enter `0` to use an auto-detected recommended thread count).

**Startup Menu Options**
- `1) Add new seed URL(s)` — enqueue a seed URL (page or sitemap) into the DB.
- `2) Resume ALL paused URLs and start crawling` — resumes paused entries and starts workers.
- `3) Resume specific paused URL(s) and start crawling` — interactively pick paused URLs to resume (filtered by main domain when possible).
- `4) Pause a URL` — mark a specific URL as paused before crawling.
- `5) List pending URLs by prefix` — show pending URLs matching a prefix.
- `6) Pause URLs by prefix (before starting)` — pause pending URLs by prefix.
- `7) Show crawler stats` — print counts and domain/prefix breakdowns.
- `8) Exit` — quit without starting workers.

**Runtime Commands (when workers are running)**
- `seed <url>` — add a new seed URL (page or sitemap).
- `pause <url>` — pause a single URL (status set to `paused`).
- `pause-prefix <prefix>` — pause pending URLs starting with `<prefix>` (best-effort removes them from in-memory queue).
- `resume <url>` — resume a paused URL (status -> `pending`).
- `resume-prefix <prefix>` — resume paused URLs matching `<prefix>`.
- `stats` — display crawler statistics and distributions.
- `status` — print worker/pending/visited/paused/error counts.
- `stop` / `quit` — graceful shutdown; DB is closed and state saved.
- `help` — show command list.

**Where state & output live**
- `crawler_state.db` — SQLite DB that stores all known URLs and their statuses.
- `scraped_data.jsonl` — newline-delimited JSON of scraped page summaries (append-only).
- `crawler.log` — rotating log file with INFO/ERROR messages for debugging.

**Project files**
- `main.py`: CLI entry point and startup menu. Collects crawl delay and worker count.
- `crawler.py`: `Crawler` class — worker loop, fetching, sitemap parsing, link extraction, and queue management.
- `scraper.py`: `scrape_html()` — extracts `title`, `content` (first 500 chars), and other basic fields written to JSONL.
- `db.py`: `CrawlerDB` — thread-safe (uses locks) SQLite wrapper for URL state, pause/resume, and counts.
- `requirements.txt`: Python dependencies (e.g., `requests`, `beautifulsoup4`).

**Functional workflow (end-to-end)**
1. `main.py` creates a `Crawler` instance with `db_path`, `num_workers`, and `delay_seconds`.
2. On startup, the `Crawler` loads any `pending` URLs from the DB into an in-memory `queue.Queue`.
3. When the user starts crawling, `Crawler.start_workers()` spawns N daemon threads; each runs `_worker_loop()`.
4. Each worker pulls a URL from the queue and checks its DB row (status/retry/is_sitemap).
5. The worker fetches the URL using `requests` (with a per-thread delay). If the response looks like XML or the URL looks like a sitemap, the sitemap handler parses URLs and enqueues them. Otherwise, the worker:
   - decodes the response body,
   - calls `scrape_html(url, html, status_code)` (from `scraper.py`) to extract core fields,
   - appends the returned dict as a JSON object to `scraped_data.jsonl`,
   - extracts `<a href>` links using `BeautifulSoup`, normalizes them, and inserts new ones into the DB (deduped) and in-memory queue.
6. Successful fetches are marked `visited` in the DB. Failures are retried up to `MAX_RETRIES` (default 3) and then marked `error`.

**Sitemap handling**
- The crawler recognizes sitemap indices (`sitemapindex`) and standard sitemaps (`urlset`).
- It can also handle gzipped sitemaps (`.xml.gz`). Sitemap URLs are enqueued with an `is_sitemap` flag so they can be parsed appropriately.

**Concurrency & persistence details**
- `CrawlerDB` uses a `threading.Lock` (`db_lock`) to serialize SQLite access; the connection is opened with `check_same_thread=False` and `PRAGMA journal_mode=WAL` for concurrent readers/writers.
- The in-memory `pending_queue` is a `queue.Queue` (thread-safe). Pausing by prefix attempts a best-effort removal from the queue by manipulating the underlying deque within the queue mutex.
- Worker threads are daemon threads; `stop()` sets an Event to request shutdown and closes the DB.

**Data format: `scraped_data.jsonl`**
Each line is a JSON object produced by `scrape_html()` with at least these keys:

```json
{
  "url": "https://example.com/path",
  "title": "Page Title",
  "status_code": 200,
  "content": "First 500 characters of visible text..."
}
```

Each line is appended with UTF-8 encoding. `scraped_data.jsonl` can be read with standard JSONL tools or processed line-by-line in Python.

**Important constants & behavior**
- `USER_AGENT` (in `crawler.py`) — default request User-Agent string.
- `MAX_RETRIES` (in `crawler.py`) — number of fetch retries before marking `error` (default 3).
- `DB_FILENAME` (in `db.py`) — default DB filename: `crawler_state.db`.
- `determine_auto_threads()` (in `crawler.py`) — recommends a sensible default thread count based on `os.cpu_count()` (network-bound behavior uses more threads).

**Troubleshooting & tips**
- If the crawler appears idle, check `crawler_state.db` for `pending` URLs and `crawler.log` for worker errors.
- To recover or re-run a failed URL, you can `resume <url>` or remove its row from the DB manually (SQLite tools).
- Pausing by prefix is best-effort: it updates DB rows and attempts to remove pending items from the in-memory queue, but there may be race conditions if workers already fetched the item.

**Extending / Development notes**
- The `scrape_html()` function is intentionally small — extend it to extract metadata, structured data, or save full HTML if needed.
- Consider adding polite host-based rate limiting, robots.txt checks, or per-domain concurrency limits for production use.

**Contributing**
- Open an issue or submit a PR if you want help improving features (e.g., more robust URL normalization, robots.txt, or export formats).

---

If you'd like, I can also:
- add a short example script to replay `scraped_data.jsonl`, or
- add more detailed developer documentation for the database schema and log format.
