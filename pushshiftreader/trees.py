"""
Thread reconstruction on top of canonical extracted corpora.
"""

import logging
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterator, List, Optional

from .models import Comment, CommentNode, Submission, Thread
from .storage import (
    COMMENT_FIELD_SPECS,
    THREAD_ROW_SCHEMA,
    CorpusLayout,
    read_json,
    read_parquet_records,
    write_parquet_records,
)
from .utils import format_duration

logger = logging.getLogger(__name__)


THREAD_ROW_SPECS = COMMENT_FIELD_SPECS + [
    ("submission_id", THREAD_ROW_SCHEMA.field("submission_id").type),
    ("parent_comment_id", THREAD_ROW_SCHEMA.field("parent_comment_id").type),
    ("parent_author", THREAD_ROW_SCHEMA.field("parent_author").type),
    ("submission_author", THREAD_ROW_SCHEMA.field("submission_author").type),
    ("submission_title", THREAD_ROW_SCHEMA.field("submission_title").type),
    ("submission_created_utc", THREAD_ROW_SCHEMA.field("submission_created_utc").type),
    ("depth", THREAD_ROW_SCHEMA.field("depth").type),
    ("thread_size", THREAD_ROW_SCHEMA.field("thread_size").type),
    ("time_since_submission", THREAD_ROW_SCHEMA.field("time_since_submission").type),
    ("missing_parent", THREAD_ROW_SCHEMA.field("missing_parent").type),
    ("is_top_level", THREAD_ROW_SCHEMA.field("is_top_level").type),
]


class TreeBuilder:
    """
    Build thread-aware comment tables from a canonical subreddit corpus.
    """

    def __init__(self, extracted_path: Path):
        self.extracted_path = Path(extracted_path)
        if not self.extracted_path.exists():
            raise ValueError(f"Dataset path does not exist: {self.extracted_path}")
        self.layout = CorpusLayout(self.extracted_path)

    def _get_months(self) -> List[str]:
        if self.layout.dataset_metadata_path.exists():
            metadata = read_json(self.layout.dataset_metadata_path)
            return list(metadata.get("months", []))
        return [path.stem for path in self.layout.month_metadata_files()]

    def build_month(self, month: str) -> int:
        started = time.time()
        submissions = {
            row["id"]: Submission.from_dict(row)
            for row in read_parquet_records(self.layout.submissions_path(month))
        }
        comments = [
            Comment.from_dict(row)
            for row in read_parquet_records(self.layout.comments_path(month))
        ]

        comments_by_submission: Dict[str, List[Comment]] = defaultdict(list)
        for comment in comments:
            comments_by_submission[comment.submission_id].append(comment)

        derived_rows: List[dict] = []
        thread_count = 0

        for submission_id, submission in submissions.items():
            submission_comments = comments_by_submission.get(submission_id, [])
            thread_count += 1
            if not submission_comments:
                continue

            nodes = {
                comment.id: {
                    "comment": comment,
                    "children": [],
                    "missing_parent": False,
                }
                for comment in submission_comments
            }
            top_level: List[dict] = []

            for comment in submission_comments:
                node = nodes[comment.id]
                parent_comment_id = comment.parent_comment_id
                if parent_comment_id and parent_comment_id in nodes:
                    nodes[parent_comment_id]["children"].append(node)
                elif parent_comment_id:
                    node["missing_parent"] = True
                    top_level.append(node)
                else:
                    top_level.append(node)

            top_level.sort(key=lambda item: item["comment"].created_utc)
            thread_size = len(submission_comments)

            def emit(node: dict, depth: int, parent_author: str) -> None:
                comment = node["comment"]
                base = comment.to_dict(include_extra=False)
                base["submission_id"] = submission.id
                base["parent_comment_id"] = comment.parent_comment_id or ""
                base["parent_author"] = parent_author or ""
                base["submission_author"] = submission.author
                base["submission_title"] = submission.title
                base["submission_created_utc"] = submission.created_utc
                base["depth"] = depth
                base["thread_size"] = thread_size
                base["time_since_submission"] = comment.created_utc - submission.created_utc
                base["missing_parent"] = node["missing_parent"]
                base["is_top_level"] = depth == 0
                derived_rows.append(base)

                node["children"].sort(key=lambda item: item["comment"].created_utc)
                for child in node["children"]:
                    emit(child, depth + 1, comment.author)

            for node in top_level:
                emit(node, 0, submission.author)

        write_parquet_records(
            self.layout.threads_path(month),
            derived_rows,
            THREAD_ROW_SCHEMA,
            THREAD_ROW_SPECS,
        )

        logger.info(
            "Built thread table for %s: %s threads in %s",
            month,
            f"{thread_count:,}",
            format_duration(time.time() - started),
        )
        return thread_count

    def build_all_months(self) -> Dict[str, int]:
        return {month: self.build_month(month) for month in self._get_months()}


def load_threads(threads_path: Path) -> Iterator[Thread]:
    threads_path = Path(threads_path)
    if not threads_path.exists():
        return iter(())

    dataset_root = threads_path.parent.parent
    month = threads_path.stem
    layout = CorpusLayout(dataset_root)
    submissions = {
        row["id"]: Submission.from_dict(row)
        for row in read_parquet_records(layout.submissions_path(month))
    }
    rows = read_parquet_records(threads_path)

    by_submission: Dict[str, List[dict]] = defaultdict(list)
    for row in rows:
        by_submission[row["submission_id"]].append(row)

    def _iterator() -> Iterator[Thread]:
        for submission_id, submission in submissions.items():
            grouped = by_submission.get(submission_id, [])
            nodes_by_id = {
                row["id"]: CommentNode(comment=Comment.from_dict(row))
                for row in grouped
            }
            top_level: List[CommentNode] = []

            for row in grouped:
                node = nodes_by_id[row["id"]]
                parent_comment_id = row.get("parent_comment_id") or ""
                if parent_comment_id and parent_comment_id in nodes_by_id:
                    nodes_by_id[parent_comment_id].replies.append(node)
                else:
                    top_level.append(node)

            top_level.sort(key=lambda node: node.comment.created_utc)
            yield Thread(submission=submission, comments=top_level)

    return _iterator()
