"""
Small Reddit API scraper for recent subreddit submissions and comments.

The scraper uses Reddit's OAuth API with an installed-script/client-credentials
application. It intentionally writes JSONL rather than mixing live API results
into the package's Pushshift Parquet layout.
"""

import base64
import json
import os
import uuid
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Tuple
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_SUBREDDITS = ["wales", "Cardiff", "southwales", "northwales"]
REDDIT_TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
REDDIT_API_BASE = "https://oauth.reddit.com"
INSTALLED_CLIENT_GRANT = "https://oauth.reddit.com/grants/installed_client"
AUTH_MODES = {"client_credentials", "installed_client"}


class RedditAPIError(RuntimeError):
    """Raised when Reddit returns an API error that cannot be recovered."""


@dataclass
class RedditAPICredentials:
    client_id: str
    user_agent: str
    client_secret: str = ""
    auth_mode: str = "client_credentials"
    device_id: str = ""

    @classmethod
    def from_env(cls) -> "RedditAPICredentials":
        client_id = os.environ.get("REDDIT_CLIENT_ID", "").strip()
        client_secret = os.environ.get("REDDIT_CLIENT_SECRET", "").strip()
        user_agent = os.environ.get("REDDIT_USER_AGENT", "").strip()
        auth_mode = os.environ.get("REDDIT_AUTH_MODE", "client_credentials").strip()
        device_id = os.environ.get("REDDIT_DEVICE_ID", "").strip()
        if auth_mode not in AUTH_MODES:
            raise ValueError(f"REDDIT_AUTH_MODE must be one of: {', '.join(sorted(AUTH_MODES))}")
        if not client_id or not user_agent or (auth_mode == "client_credentials" and not client_secret):
            missing = [
                name
                for name, value in [
                    ("REDDIT_CLIENT_ID", client_id),
                    ("REDDIT_CLIENT_SECRET", client_secret if auth_mode == "client_credentials" else "optional"),
                    ("REDDIT_USER_AGENT", user_agent),
                ]
                if not value
            ]
            raise ValueError(f"Missing Reddit API environment variables: {', '.join(missing)}")
        return cls(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            auth_mode=auth_mode,
            device_id=device_id,
        )


class RedditAPIClient:
    def __init__(self, credentials: RedditAPICredentials, timeout: float = 30.0):
        self.credentials = credentials
        self.timeout = timeout
        self._access_token: Optional[str] = None
        self._token_expires_at = 0.0

    def get(self, path: str, params: Optional[Mapping[str, Any]] = None) -> Tuple[Dict[str, Any], Mapping[str, str]]:
        self._ensure_token()
        assert self._access_token is not None
        url = f"{REDDIT_API_BASE}{path}"
        query = dict(params or {})
        query.setdefault("raw_json", 1)
        if query:
            url = f"{url}?{urlencode(query)}"
        request = Request(
            url,
            headers={
                "Authorization": f"Bearer {self._access_token}",
                "User-Agent": self.credentials.user_agent,
            },
        )
        payload, headers = self._open_json(request)
        self._respect_rate_limit(headers)
        return payload, headers

    def _ensure_token(self) -> None:
        if self._access_token and time.time() < self._token_expires_at - 60:
            return
        credentials = f"{self.credentials.client_id}:{self.credentials.client_secret}".encode("utf-8")
        token_payload = {"grant_type": "client_credentials"}
        if self.credentials.auth_mode == "installed_client":
            token_payload = {
                "grant_type": INSTALLED_CLIENT_GRANT,
                "device_id": self.credentials.device_id or _device_id(),
            }
        request = Request(
            REDDIT_TOKEN_URL,
            data=urlencode(token_payload).encode("utf-8"),
            headers={
                "Authorization": f"Basic {base64.b64encode(credentials).decode('ascii')}",
                "User-Agent": self.credentials.user_agent,
                "Content-Type": "application/x-www-form-urlencoded",
            },
            method="POST",
        )
        payload, _headers = self._open_json(request)
        access_token = payload.get("access_token")
        if not access_token:
            raise RedditAPIError("Reddit OAuth response did not include access_token")
        expires_in = int(payload.get("expires_in", 3600))
        self._access_token = str(access_token)
        self._token_expires_at = time.time() + expires_in

    def _open_json(self, request: Request) -> Tuple[Dict[str, Any], Mapping[str, str]]:
        try:
            with urlopen(request, timeout=self.timeout) as response:
                body = response.read().decode("utf-8")
                return json.loads(body), response.headers
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            detail = f"Reddit API returned HTTP {exc.code}: {body[:500]}"
            if exc.code == 401 and request.full_url == REDDIT_TOKEN_URL:
                detail += (
                    "\nOAuth token request was unauthorized. Check REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, "
                    "and app type. If your Reddit app is an installed app, set REDDIT_AUTH_MODE=installed_client "
                    "and leave REDDIT_CLIENT_SECRET blank."
                )
            raise RedditAPIError(detail) from exc

    @staticmethod
    def _respect_rate_limit(headers: Mapping[str, str]) -> None:
        remaining_raw = headers.get("x-ratelimit-remaining")
        reset_raw = headers.get("x-ratelimit-reset")
        if remaining_raw is None or reset_raw is None:
            return
        try:
            remaining = float(remaining_raw)
            reset_seconds = float(reset_raw)
        except ValueError:
            return
        if remaining < 2 and reset_seconds > 0:
            time.sleep(min(reset_seconds + 1, 120))


def clean_subreddit_name(value: str) -> str:
    subreddit = value.strip()
    if subreddit.startswith("/r/"):
        subreddit = subreddit[3:]
    elif subreddit.lower().startswith("r/"):
        subreddit = subreddit[2:]
    if not subreddit:
        raise ValueError("Subreddit names cannot be empty")
    return subreddit


def flatten_comment_listing(
    listing: Mapping[str, Any],
    submission_id: str,
    subreddit: str,
    retrieved_at: Optional[str] = None,
    include_raw: bool = False,
) -> Iterator[Dict[str, Any]]:
    children = listing.get("data", {}).get("children", [])
    yield from _flatten_comment_children(
        children,
        submission_id=submission_id,
        subreddit=subreddit,
        depth=0,
        retrieved_at=retrieved_at,
        include_raw=include_raw,
    )


def _flatten_comment_children(
    children: Iterable[Mapping[str, Any]],
    submission_id: str,
    subreddit: str,
    depth: int,
    retrieved_at: Optional[str],
    include_raw: bool,
) -> Iterator[Dict[str, Any]]:
    for child in children:
        if child.get("kind") != "t1":
            continue
        data = dict(child.get("data", {}))
        yield normalize_comment(
            data,
            submission_id=submission_id,
            subreddit=subreddit,
            depth=depth,
            retrieved_at=retrieved_at,
            include_raw=include_raw,
        )
        replies = data.get("replies")
        if isinstance(replies, Mapping):
            yield from _flatten_comment_children(
                replies.get("data", {}).get("children", []),
                submission_id=submission_id,
                subreddit=subreddit,
                depth=depth + 1,
                retrieved_at=retrieved_at,
                include_raw=include_raw,
            )


def normalize_submission(data: Mapping[str, Any], subreddit: str, retrieved_at: str, include_raw: bool = False) -> Dict[str, Any]:
    permalink = data.get("permalink")
    record = {
        "record_type": "submission",
        "retrieved_at": retrieved_at,
        "subreddit": subreddit,
        "id": data.get("id"),
        "name": data.get("name"),
        "author": data.get("author"),
        "created_utc": data.get("created_utc"),
        "title": data.get("title"),
        "selftext": data.get("selftext"),
        "url": data.get("url"),
        "permalink": permalink,
        "full_link": f"https://www.reddit.com{permalink}" if permalink else None,
        "score": data.get("score"),
        "upvote_ratio": data.get("upvote_ratio"),
        "num_comments": data.get("num_comments"),
        "over_18": data.get("over_18"),
        "spoiler": data.get("spoiler"),
        "stickied": data.get("stickied"),
        "locked": data.get("locked"),
        "is_self": data.get("is_self"),
        "link_flair_text": data.get("link_flair_text"),
    }
    if include_raw:
        record["raw"] = dict(data)
    return record


def normalize_comment(
    data: Mapping[str, Any],
    submission_id: str,
    subreddit: str,
    depth: int,
    retrieved_at: Optional[str] = None,
    include_raw: bool = False,
) -> Dict[str, Any]:
    permalink = data.get("permalink")
    record = {
        "record_type": "comment",
        "retrieved_at": retrieved_at,
        "subreddit": subreddit,
        "submission_id": submission_id,
        "id": data.get("id"),
        "name": data.get("name"),
        "author": data.get("author"),
        "created_utc": data.get("created_utc"),
        "body": data.get("body"),
        "link_id": data.get("link_id"),
        "parent_id": data.get("parent_id"),
        "score": data.get("score"),
        "depth": depth,
        "is_submitter": data.get("is_submitter"),
        "stickied": data.get("stickied"),
        "distinguished": data.get("distinguished"),
        "permalink": permalink,
        "full_link": f"https://www.reddit.com{permalink}" if permalink else None,
    }
    if include_raw:
        record["raw"] = dict(data)
    return record


class RedditSubmissionScraper:
    def __init__(
        self,
        client: Any,
        output_path: Path,
        subreddits: Iterable[str] = DEFAULT_SUBREDDITS,
        sort: str = "new",
        max_submissions: int = 100,
        page_limit: int = 100,
        comments_limit: int = 500,
        comment_depth: Optional[int] = None,
        include_raw: bool = False,
        since_utc: Optional[float] = None,
        until_utc: Optional[float] = None,
        fetch_comments: bool = True,
    ):
        if sort not in {"hot", "new", "rising", "top"}:
            raise ValueError("--sort must be one of: hot, new, rising, top")
        if max_submissions < 1:
            raise ValueError("--max-submissions must be at least 1")
        self.client = client
        self.output_path = Path(output_path)
        self.subreddits = [clean_subreddit_name(item) for item in subreddits]
        self.sort = sort
        self.max_submissions = max_submissions
        self.page_limit = min(max(page_limit, 1), 100)
        self.comments_limit = min(max(comments_limit, 1), 500)
        self.comment_depth = comment_depth
        self.include_raw = include_raw
        self.since_utc = since_utc
        self.until_utc = until_utc
        self.fetch_comments_enabled = fetch_comments
        if self.since_utc is not None and self.until_utc is not None and self.since_utc >= self.until_utc:
            raise ValueError("--since must be earlier than --until")

    def run(self) -> Dict[str, Any]:
        self.output_path.mkdir(parents=True, exist_ok=True)
        started_at = _utc_now()
        totals: Dict[str, Any] = {
            "started_at": started_at,
            "finished_at": None,
            "subreddits": self.subreddits,
            "sort": self.sort,
            "max_submissions": self.max_submissions,
            "comments_limit": self.comments_limit,
            "comment_depth": self.comment_depth,
            "include_raw": self.include_raw,
            "since_utc": self.since_utc,
            "until_utc": self.until_utc,
            "fetch_comments": self.fetch_comments_enabled,
            "subreddit_results": {},
            "total_submissions": 0,
            "total_comments": 0,
        }

        for subreddit in self.subreddits:
            submissions_path = self.output_path / subreddit / "submissions.jsonl"
            comments_path = self.output_path / subreddit / "comments.jsonl"
            submissions_path.parent.mkdir(parents=True, exist_ok=True)
            submission_count = 0
            comment_count = 0
            with submissions_path.open("w", encoding="utf-8") as submissions_handle, comments_path.open(
                "w", encoding="utf-8"
            ) as comments_handle:
                for raw_submission in self.iter_submissions(subreddit):
                    retrieved_at = _utc_now()
                    submission = normalize_submission(
                        raw_submission,
                        subreddit=subreddit,
                        retrieved_at=retrieved_at,
                        include_raw=self.include_raw,
                    )
                    _write_jsonl(submissions_handle, submission)
                    submission_count += 1

                    if not self.fetch_comments_enabled:
                        continue
                    submission_id = str(raw_submission.get("id", ""))
                    for raw_comment in self.fetch_comments(subreddit, submission_id):
                        _write_jsonl(comments_handle, raw_comment)
                        comment_count += 1

            totals["subreddit_results"][subreddit] = {
                "submissions": submission_count,
                "comments": comment_count,
                "submissions_path": str(submissions_path),
                "comments_path": str(comments_path),
            }
            totals["total_submissions"] += submission_count
            totals["total_comments"] += comment_count

        totals["finished_at"] = _utc_now()
        metadata_path = self.output_path / "metadata.json"
        metadata_path.write_text(json.dumps(totals, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return totals

    def iter_submissions(self, subreddit: str) -> Iterator[Dict[str, Any]]:
        after = None
        yielded = 0
        while yielded < self.max_submissions:
            limit = min(self.page_limit, self.max_submissions - yielded)
            params: Dict[str, Any] = {"limit": limit}
            if after:
                params["after"] = after
            payload, _headers = self.client.get(f"/r/{subreddit}/{self.sort}", params=params)
            listing = payload.get("data", {})
            children = listing.get("children", [])
            if not children:
                break
            stop_for_since = False
            for child in children:
                if child.get("kind") != "t3":
                    continue
                submission = dict(child.get("data", {}))
                created_utc = _created_timestamp(submission)
                if not self._submission_in_date_window(created_utc):
                    if self.since_utc is not None and created_utc is not None and created_utc < self.since_utc:
                        stop_for_since = self.sort == "new"
                        if stop_for_since:
                            break
                    continue
                yield submission
                yielded += 1
                if yielded >= self.max_submissions:
                    break
            if stop_for_since:
                break
            after = listing.get("after")
            if not after:
                break

    def _submission_in_date_window(self, created_utc: Optional[float]) -> bool:
        if created_utc is None:
            return self.since_utc is None and self.until_utc is None
        if self.since_utc is not None and created_utc < self.since_utc:
            return False
        if self.until_utc is not None and created_utc >= self.until_utc:
            return False
        return True

    def fetch_comments(self, subreddit: str, submission_id: str) -> Iterator[Dict[str, Any]]:
        if not submission_id:
            return
        params: Dict[str, Any] = {"limit": self.comments_limit}
        if self.comment_depth is not None:
            params["depth"] = self.comment_depth
        payload, _headers = self.client.get(f"/r/{subreddit}/comments/{submission_id}", params=params)
        if not isinstance(payload, list) or len(payload) < 2:
            return
        retrieved_at = _utc_now()
        for comment in flatten_comment_listing(
            payload[1],
            submission_id=submission_id,
            subreddit=subreddit,
            retrieved_at=retrieved_at,
            include_raw=self.include_raw,
        ):
            yield comment


class RedditCommentFetcher:
    def __init__(
        self,
        client: Any,
        source_path: Path,
        output_path: Optional[Path] = None,
        subreddits: Optional[Iterable[str]] = None,
        comments_limit: int = 500,
        comment_depth: Optional[int] = None,
        include_raw: bool = False,
        max_submissions: Optional[int] = None,
        force: bool = False,
    ):
        self.client = client
        self.source_path = Path(source_path)
        self.output_path = Path(output_path) if output_path else self.source_path
        self.subreddits = [clean_subreddit_name(item) for item in subreddits] if subreddits else None
        self.comments_limit = min(max(comments_limit, 1), 500)
        self.comment_depth = comment_depth
        self.include_raw = include_raw
        self.max_submissions = max_submissions
        self.force = force

    def run(self) -> Dict[str, Any]:
        if not self.source_path.exists():
            raise ValueError(f"Source path does not exist: {self.source_path}")
        started_at = _utc_now()
        subreddits = self.subreddits or self._discover_subreddits()
        totals: Dict[str, Any] = {
            "started_at": started_at,
            "finished_at": None,
            "source_path": str(self.source_path),
            "output_path": str(self.output_path),
            "subreddits": subreddits,
            "comments_limit": self.comments_limit,
            "comment_depth": self.comment_depth,
            "include_raw": self.include_raw,
            "max_submissions": self.max_submissions,
            "subreddit_results": {},
            "total_submissions_processed": 0,
            "total_comments": 0,
        }

        remaining = self.max_submissions
        for subreddit in subreddits:
            submissions = list(self._iter_submissions(subreddit))
            if remaining is not None:
                submissions = submissions[:remaining]
                remaining -= len(submissions)
            comments_path = self.output_path / subreddit / "comments.jsonl"
            comments_path.parent.mkdir(parents=True, exist_ok=True)
            if comments_path.exists() and comments_path.stat().st_size > 0 and not self.force:
                totals["subreddit_results"][subreddit] = {
                    "submissions_processed": 0,
                    "comments": None,
                    "comments_path": str(comments_path),
                    "skipped_existing": True,
                }
                if remaining == 0:
                    break
                continue

            comment_count = 0
            with comments_path.open("w", encoding="utf-8") as comments_handle:
                for submission in submissions:
                    submission_id = str(submission.get("id", ""))
                    for comment in self.fetch_comments(subreddit, submission_id):
                        _write_jsonl(comments_handle, comment)
                        comment_count += 1

            totals["subreddit_results"][subreddit] = {
                "submissions_processed": len(submissions),
                "comments": comment_count,
                "comments_path": str(comments_path),
                "skipped_existing": False,
            }
            totals["total_submissions_processed"] += len(submissions)
            totals["total_comments"] += comment_count
            if remaining == 0:
                break

        totals["finished_at"] = _utc_now()
        metadata_path = self.output_path / "comments_metadata.json"
        metadata_path.write_text(json.dumps(totals, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return totals

    def fetch_comments(self, subreddit: str, submission_id: str) -> Iterator[Dict[str, Any]]:
        if not submission_id:
            return
        params: Dict[str, Any] = {"limit": self.comments_limit}
        if self.comment_depth is not None:
            params["depth"] = self.comment_depth
        payload, _headers = self.client.get(f"/r/{subreddit}/comments/{submission_id}", params=params)
        if not isinstance(payload, list) or len(payload) < 2:
            return
        retrieved_at = _utc_now()
        for comment in flatten_comment_listing(
            payload[1],
            submission_id=submission_id,
            subreddit=subreddit,
            retrieved_at=retrieved_at,
            include_raw=self.include_raw,
        ):
            yield comment

    def _discover_subreddits(self) -> List[str]:
        metadata_path = self.source_path / "metadata.json"
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            subreddits = metadata.get("subreddits", [])
            if subreddits:
                return [clean_subreddit_name(item) for item in subreddits]
        return sorted(
            path.name
            for path in self.source_path.iterdir()
            if path.is_dir() and (path / "submissions.jsonl").exists()
        )

    def _iter_submissions(self, subreddit: str) -> Iterator[Dict[str, Any]]:
        submissions_path = self.source_path / subreddit / "submissions.jsonl"
        if not submissions_path.exists():
            raise ValueError(f"Missing submissions file for r/{subreddit}: {submissions_path}")
        with submissions_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield json.loads(line)


def _write_jsonl(handle: Any, record: Mapping[str, Any]) -> None:
    handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _device_id() -> str:
    return uuid.uuid4().hex[:30]


def _created_timestamp(record: Mapping[str, Any]) -> Optional[float]:
    value = record.get("created_utc")
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
