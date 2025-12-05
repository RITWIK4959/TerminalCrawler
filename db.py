import threading
import sqlite3
from datetime import datetime
from typing import Optional, Tuple, List


DB_FILENAME = "crawler_state.db"


class CrawlerDB:
    """
    Thread-safe SQLite wrapper for crawler state:
    - URL statuses (pending/visited/paused/error)
    - Retry counts, error messages, sitemap flags
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db_lock = threading.Lock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self._init_db()

    # ------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------
    def _init_db(self) -> None:
        with self.db_lock:
            cur = self.conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS urls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL, -- pending, visited, paused, error
                    last_status_change TEXT,
                    last_error TEXT,
                    retry_count INTEGER DEFAULT 0,
                    is_sitemap INTEGER DEFAULT 0,
                    pause_reason TEXT
                )
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_urls_status ON urls (status)"
            )
            self.conn.commit()

    def _now(self) -> str:
        return datetime.utcnow().isoformat(timespec="seconds")

    # ------------------------------------------------------------
    # Public DB operations
    # ------------------------------------------------------------
    def get_url_row(self, url: str) -> Optional[Tuple]:
        with self.db_lock:
            cur = self.conn.cursor()
            cur.execute("SELECT * FROM urls WHERE url = ?", (url,))
            return cur.fetchone()

    def get_earliest_url(self) -> Optional[str]:
        with self.db_lock:
            cur = self.conn.cursor()
            cur.execute("SELECT url FROM urls ORDER BY id ASC LIMIT 1")
            row = cur.fetchone()
        if row:
            return row[0]
        return None

    def insert_or_ignore_url(
        self, url: str, status: str = "pending", is_sitemap: int = 0
    ) -> bool:
        """
        Insert URL if not present. Returns True if inserted, False if duplicate.
        """
        now = self._now()
        with self.db_lock:
            cur = self.conn.cursor()
            try:
                cur.execute(
                    """
                    INSERT INTO urls (url, status, last_status_change, retry_count, is_sitemap)
                    VALUES (?, ?, ?, 0, ?)
                    """,
                    (url, status, now, is_sitemap),
                )
                self.conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def update_url_status(
        self,
        url: str,
        status: str,
        last_error: Optional[str] = None,
        is_sitemap: Optional[int] = None,
        pause_reason: Optional[str] = None,
        clear_pause_reason: bool = False,
        increment_retry: bool = False,
    ) -> None:
        now = self._now()
        with self.db_lock:
            cur = self.conn.cursor()

            fields = ["status = ?", "last_status_change = ?"]
            params: List = [status, now]

            if last_error is not None:
                fields.append("last_error = ?")
                params.append(last_error)

            if increment_retry:
                fields.append("retry_count = retry_count + 1")

            if is_sitemap is not None:
                fields.append("is_sitemap = ?")
                params.append(is_sitemap)

            if pause_reason is not None:
                fields.append("pause_reason = ?")
                params.append(pause_reason)
            elif clear_pause_reason:
                fields.append("pause_reason = NULL")

            params.append(url)
            query = f"UPDATE urls SET {', '.join(fields)} WHERE url = ?"
            cur.execute(query, tuple(params))
            self.conn.commit()

    def load_pending_urls(self) -> List[str]:
        with self.db_lock:
            cur = self.conn.cursor()
            cur.execute("SELECT url FROM urls WHERE status = 'pending'")
            rows = cur.fetchall()
        return [url for (url,) in rows]

    def get_status_counts(self) -> Tuple[int, int, int, int]:
        with self.db_lock:
            cur = self.conn.cursor()
            cur.execute(
                """
                SELECT
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN status = 'visited' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN status = 'paused' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END)
                FROM urls
                """
            )
            row = cur.fetchone()
        if row is None:
            return 0, 0, 0, 0
        return tuple(int(x or 0) for x in row)  # type: ignore[return-value]

    def pause_prefix(self, prefix: str, reason: str) -> int:
        with self.db_lock:
            cur = self.conn.cursor()
            cur.execute(
                """
                UPDATE urls
                SET status = 'paused',
                    last_status_change = ?,
                    pause_reason = ?
                WHERE url LIKE ? AND status = 'pending'
                """,
                (self._now(), reason, f"{prefix}%"),
            )
            affected = cur.rowcount
            self.conn.commit()
        return affected

    def list_paused_urls(self) -> List[str]:
        with self.db_lock:
            cur = self.conn.cursor()
            cur.execute(
                "SELECT url FROM urls WHERE status = 'paused' ORDER BY id ASC"
            )
            rows = cur.fetchall()
        return [row[0] for row in rows]

    def resume_all_paused(self) -> List[str]:
        """
        Set all paused URLs back to pending and return the list of affected URLs.
        """
        with self.db_lock:
            cur = self.conn.cursor()
            cur.execute(
                "SELECT url FROM urls WHERE status = 'paused'"
            )
            urls = [row[0] for row in cur.fetchall()]

            cur.execute(
                """
                UPDATE urls
                SET status = 'pending',
                    last_status_change = ?,
                    pause_reason = NULL
                WHERE status = 'paused'
                """,
                (self._now(),),
            )
            self.conn.commit()

        return urls

    def resume_prefix(self, prefix: str):
        with self.db_lock:
            cur = self.conn.cursor()
            cur.execute(
                """
                SELECT url FROM urls
                WHERE url LIKE ? AND status = 'paused'
                """,
                (f"{prefix}%",),
            )
            urls = [row[0] for row in cur.fetchall()]

            cur.execute(
                """
                UPDATE urls
                SET status = 'pending',
                    last_status_change = ?,
                    pause_reason = NULL
                WHERE url LIKE ? AND status = 'paused'
                """,
                (self._now(), f"{prefix}%",),
            )
            affected = cur.rowcount
            self.conn.commit()

        return urls, affected

    def close(self) -> None:
        with self.db_lock:
            self.conn.commit()
            self.conn.close()


