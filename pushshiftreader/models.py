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
