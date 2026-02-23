"""
Data models for Reddit submissions and comments.

These dataclasses preserve all fields from the Pushshift dumps.
Fields are optional where they may not exist in older records.
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional, Any, List, Iterator, Tuple
import json


@dataclass
class Submission:
    """A Reddit submission (post)."""
    
    # Core identifiers
    id: str
    name: Optional[str] = None  # fullname, e.g., "t3_abc123"
    
    # Author info
    author: str = "[deleted]"
    author_flair_text: Optional[str] = None
    author_flair_css_class: Optional[str] = None
    author_fullname: Optional[str] = None
    
    # Subreddit info
    subreddit: str = ""
    subreddit_id: Optional[str] = None
    subreddit_name_prefixed: Optional[str] = None
    subreddit_type: Optional[str] = None
    subreddit_subscribers: Optional[int] = None
    
    # Content
    title: str = ""
    selftext: str = ""
    url: Optional[str] = None
    domain: Optional[str] = None
    thumbnail: Optional[str] = None
    
    # Metadata
    created_utc: int = 0
    retrieved_on: Optional[int] = None
    edited: Any = False  # Can be False or a timestamp
    
    # Scores and counts
    score: int = 0
    upvote_ratio: Optional[float] = None
    num_comments: int = 0
    num_crossposts: Optional[int] = None
    
    # Flags
    is_self: bool = True
    is_video: bool = False
    is_original_content: bool = False
    is_reddit_media_domain: bool = False
    is_meta: bool = False
    is_crosspostable: Optional[bool] = None
    is_robot_indexable: Optional[bool] = None
    
    # Moderation
    over_18: bool = False
    spoiler: bool = False
    stickied: bool = False
    locked: bool = False
    archived: bool = False
    removed_by_category: Optional[str] = None
    
    # Awards
    gilded: int = 0
    total_awards_received: Optional[int] = None
    
    # Link/crosspost info
    permalink: Optional[str] = None
    full_link: Optional[str] = None
    crosspost_parent: Optional[str] = None
    crosspost_parent_list: Optional[List[dict]] = None
    
    # Media
    media: Optional[dict] = None
    media_embed: Optional[dict] = None
    secure_media: Optional[dict] = None
    secure_media_embed: Optional[dict] = None
    preview: Optional[dict] = None
    gallery_data: Optional[dict] = None
    media_metadata: Optional[dict] = None
    
    # Flair
    link_flair_text: Optional[str] = None
    link_flair_css_class: Optional[str] = None
    link_flair_type: Optional[str] = None
    link_flair_template_id: Optional[str] = None
    
    # Collections and events
    contest_mode: bool = False
    distinguished: Optional[str] = None
    suggested_sort: Optional[str] = None
    post_hint: Optional[str] = None
    category: Optional[str] = None
    
    # Store any extra fields not explicitly defined
    _extra: dict = field(default_factory=dict)
    
    @property
    def created_datetime(self) -> datetime:
        """Convert created_utc to datetime object."""
        return datetime.utcfromtimestamp(self.created_utc)
    
    @property
    def is_deleted(self) -> bool:
        """Check if the submission was deleted by the author."""
        return self.author == "[deleted]" or self.selftext == "[deleted]"
    
    @property
    def is_removed(self) -> bool:
        """Check if the submission was removed by moderators."""
        return self.selftext == "[removed]" or self.removed_by_category is not None
    
    @property
    def url_permalink(self) -> str:
        """Generate the full Reddit URL for this submission."""
        if self.permalink:
            return f"https://www.reddit.com{self.permalink}"
        return f"https://www.reddit.com/r/{self.subreddit}/comments/{self.id}/"
    
    @classmethod
    def from_dict(cls, data: dict) -> "Submission":
        """Create a Submission from a raw JSON dict."""
        known_fields = {f.name for f in cls.__dataclass_fields__.values() if f.name != '_extra'}
        
        kwargs = {}
        extra = {}
        
        for key, value in data.items():
            if key in known_fields:
                kwargs[key] = value
            else:
                extra[key] = value
        
        kwargs['_extra'] = extra
        if 'created_utc' in kwargs:
            kwargs['created_utc'] = int(kwargs['created_utc'])
        return cls(**kwargs)

    def to_dict(self, include_extra: bool = True) -> dict:
        """Convert to dictionary, optionally including extra fields."""
        d = asdict(self)
        extra = d.pop('_extra', {})
        if include_extra:
            d.update(extra)
        return d


@dataclass
class Comment:
    """A Reddit comment."""
    
    # Core identifiers
    id: str
    name: Optional[str] = None  # fullname, e.g., "t1_abc123"
    
    # Relationship IDs
    link_id: str = ""  # submission this comment belongs to (t3_...)
    parent_id: str = ""  # parent comment or submission (t1_... or t3_...)
    
    # Author info
    author: str = "[deleted]"
    author_flair_text: Optional[str] = None
    author_flair_css_class: Optional[str] = None
    author_fullname: Optional[str] = None
    
    # Subreddit info
    subreddit: str = ""
    subreddit_id: Optional[str] = None
    subreddit_name_prefixed: Optional[str] = None
    subreddit_type: Optional[str] = None
    
    # Content
    body: str = ""
    
    # Metadata
    created_utc: int = 0
    retrieved_on: Optional[int] = None
    edited: Any = False
    
    # Scores
    score: int = 0
    controversiality: int = 0
    
    # Flags
    stickied: bool = False
    locked: bool = False
    archived: bool = False
    collapsed: Optional[bool] = None
    collapsed_reason: Optional[str] = None
    is_submitter: bool = False
    
    # Moderation
    distinguished: Optional[str] = None
    removed_by_category: Optional[str] = None
    
    # Awards
    gilded: int = 0
    total_awards_received: Optional[int] = None
    
    # Permalink
    permalink: Optional[str] = None
    
    # Store any extra fields
    _extra: dict = field(default_factory=dict)
    
    @property
    def created_datetime(self) -> datetime:
        """Convert created_utc to datetime object."""
        return datetime.utcfromtimestamp(self.created_utc)
    
    @property
    def is_deleted(self) -> bool:
        """Check if the comment was deleted by the author."""
        return self.author == "[deleted]" or self.body == "[deleted]"
    
    @property
    def is_removed(self) -> bool:
        """Check if the comment was removed by moderators."""
        return self.body == "[removed]" or self.removed_by_category is not None
    
    @property
    def is_top_level(self) -> bool:
        """Check if this is a top-level comment (direct reply to submission)."""
        return self.parent_id.startswith("t3_")
    
    @property
    def submission_id(self) -> str:
        """Get the submission ID (without t3_ prefix)."""
        return self.link_id[3:] if self.link_id.startswith("t3_") else self.link_id
    
    @property
    def parent_comment_id(self) -> Optional[str]:
        """Get parent comment ID if this is a reply to another comment."""
        if self.parent_id.startswith("t1_"):
            return self.parent_id[3:]
        return None
    
    @property
    def url_permalink(self) -> str:
        """Generate the full Reddit URL for this comment."""
        if self.permalink:
            return f"https://www.reddit.com{self.permalink}"
        return f"https://www.reddit.com/r/{self.subreddit}/comments/{self.submission_id}/_/{self.id}/"
    
    @classmethod
    def from_dict(cls, data: dict) -> "Comment":
        """Create a Comment from a raw JSON dict."""
        known_fields = {f.name for f in cls.__dataclass_fields__.values() if f.name != '_extra'}
        
        kwargs = {}
        extra = {}
        
        for key, value in data.items():
            if key in known_fields:
                kwargs[key] = value
            else:
                extra[key] = value
        
        kwargs['_extra'] = extra
        if 'created_utc' in kwargs:
            kwargs['created_utc'] = int(kwargs['created_utc'])
        return cls(**kwargs)

    def to_dict(self, include_extra: bool = True) -> dict:
        """Convert to dictionary, optionally including extra fields."""
        d = asdict(self)
        extra = d.pop('_extra', {})
        if include_extra:
            d.update(extra)
        return d


@dataclass
class Thread:
    """A complete Reddit thread with submission and nested comments."""
    
    submission: Submission
    comments: List["CommentNode"] = field(default_factory=list)  # top-level comments
    
    @property
    def all_comments(self) -> List[Comment]:
        """Get all comments in the thread as a flat list."""
        result = []
        for node in self.comments:
            result.extend(node.flatten())
        return result
    
    @property
    def comment_count(self) -> int:
        """Total number of comments in thread."""
        return len(self.all_comments)
    
    def walk(self) -> Iterator[Tuple[Comment, int]]:
        """
        Walk through all comments depth-first.
        Yields (comment, depth) tuples.
        """
        for node in self.comments:
            yield from node.walk(depth=0)
    
    def to_dict(self) -> dict:
        """Convert to nested dictionary structure."""
        return {
            "submission": self.submission.to_dict(),
            "comments": [node.to_dict() for node in self.comments]
        }
    
    def to_json(self, **kwargs) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), **kwargs)

    def to_dataframe(self):
        """
        Return a pandas DataFrame of all comments in this thread.

        Each row is one comment.  Thread-level features are joined in as
        extra columns:

        - ``depth`` — depth in the reply tree (0 = top-level)
        - ``thread_size`` — total number of comments in the thread
        - ``time_since_submission`` — seconds between the submission's
          post time and the comment's post time
        - ``submission_id``, ``submission_title``, ``submission_score``,
          ``submission_author`` — metadata from the parent submission

        Returns an empty DataFrame if the thread has no comments.

        Requires ``pandas`` (``pip install pandas``).
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for DataFrame export. "
                "Install it with: pip install pandas"
            )

        sub = self.submission
        thread_size = self.comment_count

        rows = []
        for comment, depth in self.walk():
            row = comment.to_dict(include_extra=False)
            row['depth'] = depth
            row['thread_size'] = thread_size
            row['time_since_submission'] = comment.created_utc - sub.created_utc
            row['submission_id'] = sub.id
            row['submission_title'] = sub.title
            row['submission_score'] = sub.score
            row['submission_author'] = sub.author
            rows.append(row)

        if not rows:
            return pd.DataFrame()

        return pd.DataFrame(rows)

    def to_comment_graph(self):
        """
        Build a conversation graph for this thread.

        Returns ``(nodes, edges)`` — two lists of dicts ready to write as
        CSV or pass to a graph library.

        **Node fields:** ``node_id`` (``t3_`` for submission, ``t1_`` for
        comments), ``type``, ``author``, ``score``, ``created_utc``, ``depth``
        (0 = submission root, 1 = top-level reply, etc.)

        **Edge fields:** ``source``, ``target``, ``time_delta`` (seconds
        from the parent's post time to the child's post time).

        Example::

            nodes, edges = thread.to_comment_graph()
        """
        sub = self.submission
        nodes = [{
            'node_id': f't3_{sub.id}',
            'type': 'submission',
            'author': sub.author,
            'score': sub.score,
            'created_utc': sub.created_utc,
            'depth': 0,
        }]
        edges = []

        def _recurse(node: "CommentNode", parent_id: str, parent_utc: int, depth: int):
            c = node.comment
            nid = f't1_{c.id}'
            nodes.append({
                'node_id': nid,
                'type': 'comment',
                'author': c.author,
                'score': c.score,
                'created_utc': c.created_utc,
                'depth': depth,
            })
            edges.append({
                'source': parent_id,
                'target': nid,
                'time_delta': c.created_utc - parent_utc,
            })
            for reply in node.replies:
                _recurse(reply, nid, c.created_utc, depth + 1)

        root_id = f't3_{sub.id}'
        for top_node in self.comments:
            _recurse(top_node, root_id, sub.created_utc, 1)

        return nodes, edges

    def to_author_graph(self):
        """
        Build an author-interaction graph for this thread.

        Returns ``(node_stats, edge_stats)`` — accumulator dicts suitable
        for merging across many threads before writing to CSV.

        ``node_stats`` is keyed by author username::

            {author: {comment_count, total_score, first_seen_utc, last_seen_utc}}

        ``edge_stats`` is keyed by ``(source_author, target_author)`` where
        *source* replied to *target*::

            {(src, tgt): {weight, first_interaction_utc}}

        Deleted/unknown authors (``"[deleted]"`` or empty string) are
        excluded from nodes and edges.

        Example::

            node_stats, edge_stats = thread.to_author_graph()
        """
        sub = self.submission
        node_stats: dict = {}
        edge_stats: dict = {}

        def _update_author(author: str, score: int, ts: int) -> None:
            if not author or author == '[deleted]':
                return
            if author not in node_stats:
                node_stats[author] = {
                    'comment_count': 0,
                    'total_score': 0,
                    'first_seen_utc': ts,
                    'last_seen_utc': ts,
                }
            s = node_stats[author]
            s['comment_count'] += 1
            s['total_score'] += score
            if ts < s['first_seen_utc']:
                s['first_seen_utc'] = ts
            if ts > s['last_seen_utc']:
                s['last_seen_utc'] = ts

        def _update_edge(src: str, tgt: str, ts: int) -> None:
            if not src or not tgt or src == '[deleted]' or tgt == '[deleted]':
                return
            key = (src, tgt)
            if key not in edge_stats:
                edge_stats[key] = {'weight': 0, 'first_interaction_utc': ts}
            e = edge_stats[key]
            e['weight'] += 1
            if ts < e['first_interaction_utc']:
                e['first_interaction_utc'] = ts

        # Seed OP as a node (they may or may not comment in their own thread)
        op = sub.author
        if op and op != '[deleted]':
            node_stats[op] = {
                'comment_count': 0,
                'total_score': sub.score,
                'first_seen_utc': sub.created_utc,
                'last_seen_utc': sub.created_utc,
            }

        def _recurse(node: "CommentNode", parent_author: str) -> None:
            c = node.comment
            _update_author(c.author, c.score, c.created_utc)
            _update_edge(c.author, parent_author, c.created_utc)
            for reply in node.replies:
                _recurse(reply, c.author)

        for top_node in self.comments:
            _recurse(top_node, op)

        return node_stats, edge_stats


@dataclass
class CommentNode:
    """A comment with its nested replies, forming a tree structure."""
    
    comment: Comment
    replies: List["CommentNode"] = field(default_factory=list)
    
    def flatten(self) -> List[Comment]:
        """Get this comment and all nested replies as a flat list."""
        result = [self.comment]
        for reply in self.replies:
            result.extend(reply.flatten())
        return result
    
    def walk(self, depth: int = 0) -> Iterator[Tuple[Comment, int]]:
        """Walk this node and all children, yielding (comment, depth)."""
        yield self.comment, depth
        for reply in self.replies:
            yield from reply.walk(depth + 1)
    
    def to_dict(self) -> dict:
        """Convert to nested dictionary."""
        return {
            "comment": self.comment.to_dict(),
            "replies": [reply.to_dict() for reply in self.replies]
        }
