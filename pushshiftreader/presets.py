"""
Built-in signal detector presets for common Reddit patterns.

Provides :func:`get_detectors`, a factory that returns ready-to-use
:class:`~pushshiftreader.Detector` instances for common analysis scenarios.

Available presets:

- ``'general'`` — mod/admin actions and thread-dynamics signals that work
  with any subreddit
- ``'cmv'`` / ``'changemyview'`` — general preset plus delta-award detection
  for r/ChangeMyView
- ``'aita'`` / ``'amitheasshole'`` — general preset plus verdict-keyword
  detection for r/AmITheAsshole

Example::

    from pushshiftreader import get_detectors, SignalDetector

    sd = SignalDetector("./extracted/ChangeMyView",
                        detectors=get_detectors('cmv'))
    sd.run_all_months()
"""

from __future__ import annotations

import re
from typing import List, Optional

from .models import Comment, Submission, Thread
from .signals import Detector, AuthorIsOPDetector


# ---------------------------------------------------------------------------
# Mod / admin action detectors
# ---------------------------------------------------------------------------

class StickiedCommentDetector(Detector):
    """Fires when a comment has been stickied by a moderator."""

    def detect_comment(self, comment: Comment, thread: Thread, depth: int = 0) -> bool:
        return bool(comment.stickied)


class ModDistinguishedDetector(Detector):
    """
    Fires when a comment or submission is distinguished (mod, admin, etc.).

    The ``distinguished`` field is set to ``'moderator'``, ``'admin'``,
    ``'special'``, or similar strings; ``None`` means not distinguished.
    """

    def detect_comment(self, comment: Comment, thread: Thread, depth: int = 0) -> bool:
        return comment.distinguished is not None

    def detect_submission(self, submission: Submission, thread: Thread, depth: int = 0) -> bool:
        return submission.distinguished is not None


# ---------------------------------------------------------------------------
# Content state detectors
# ---------------------------------------------------------------------------

class ContentRemovedDetector(Detector):
    """
    Fires when the body of a comment or submission has been removed by
    a moderator or Reddit's systems.

    Checks the ``is_removed`` property (body is ``"[removed]"`` or
    ``removed_by_category`` is set).
    """

    def detect_comment(self, comment: Comment, thread: Thread, depth: int = 0) -> bool:
        return comment.is_removed

    def detect_submission(self, submission: Submission, thread: Thread, depth: int = 0) -> bool:
        return submission.is_removed


class AuthorDeletedDetector(Detector):
    """
    Fires when the author of a comment or submission has deleted their
    account or their content.

    Checks the ``is_deleted`` property (author is ``"[deleted]"`` or body
    is ``"[deleted]"``).
    """

    def detect_comment(self, comment: Comment, thread: Thread, depth: int = 0) -> bool:
        return comment.is_deleted

    def detect_submission(self, submission: Submission, thread: Thread, depth: int = 0) -> bool:
        return submission.is_deleted


# ---------------------------------------------------------------------------
# Thread-dynamics detectors
# ---------------------------------------------------------------------------

class TopLevelCommentDetector(Detector):
    """
    Fires when a comment is a direct (top-level) reply to the submission —
    i.e. its parent is the submission itself, not another comment.
    """

    def detect_comment(self, comment: Comment, thread: Thread, depth: int = 0) -> bool:
        return comment.is_top_level


class DepthDetector(Detector):
    """
    Fires when a comment's depth in the reply tree falls within the
    specified range.

    Depth 0 = direct reply to the submission (top-level).
    Depth 1 = reply to a top-level comment, and so on.

    Either ``min_depth`` or ``max_depth`` (or both) must be provided.

    Example::

        # Comments deeper than 5 levels
        DepthDetector("deep_reply", min_depth=6)

        # Exactly second-level replies
        DepthDetector("second_level", min_depth=1, max_depth=1)
    """

    def __init__(
        self,
        name: str,
        min_depth: Optional[int] = None,
        max_depth: Optional[int] = None,
    ):
        """
        Args:
            name: Signal identifier.
            min_depth: Inclusive minimum depth.  ``None`` = no lower bound.
            max_depth: Inclusive maximum depth.  ``None`` = no upper bound.
        """
        super().__init__(name)
        if min_depth is None and max_depth is None:
            raise ValueError("At least one of min_depth or max_depth must be set")
        self._min = min_depth
        self._max = max_depth

    def detect_comment(self, comment: Comment, thread: Thread, depth: int = 0) -> bool:
        if self._min is not None and depth < self._min:
            return False
        if self._max is not None and depth > self._max:
            return False
        return True


# ---------------------------------------------------------------------------
# Community-specific detectors
# ---------------------------------------------------------------------------

class DeltaAwardedDetector(Detector):
    """
    Fires when a comment contains a delta award token (r/ChangeMyView).

    Detects both ``Δ`` (Unicode delta) and ``!delta`` (text token),
    which are the two recognised ways to award a delta on CMV.
    """

    def detect_comment(self, comment: Comment, thread: Thread, depth: int = 0) -> bool:
        body = comment.body or ''
        return 'Δ' in body or '!delta' in body.lower()


class AITAVerdictDetector(Detector):
    """
    Fires when a comment contains a recognised AITA verdict keyword
    (r/AmITheAsshole).

    Recognised verdict tokens (whole-word, case-insensitive):

    - ``NTA`` — Not The Asshole
    - ``YTA`` — You're The Asshole
    - ``ESH`` — Everyone Sucks Here
    - ``NAH`` — No Assholes Here
    - ``INFO`` — More Information Needed
    """

    _PATTERN = re.compile(r'\b(NTA|YTA|ESH|NAH|INFO)\b', re.IGNORECASE)

    def detect_comment(self, comment: Comment, thread: Thread, depth: int = 0) -> bool:
        return bool(self._PATTERN.search(comment.body or ''))


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def _general_detectors() -> List[Detector]:
    return [
        StickiedCommentDetector("stickied_comment"),
        ModDistinguishedDetector("mod_distinguished"),
        ContentRemovedDetector("content_removed"),
        AuthorDeletedDetector("author_deleted"),
        TopLevelCommentDetector("top_level_comment"),
        AuthorIsOPDetector("op_comment"),
    ]


def get_detectors(preset: str = 'general') -> List[Detector]:
    """
    Return a ready-to-use list of :class:`~pushshiftreader.Detector` instances.

    Args:
        preset: Which preset to load.  One of:

            - ``'general'`` *(default)*: mod/admin actions and thread-dynamics
              signals applicable to any subreddit.
            - ``'cmv'`` / ``'changemyview'``: general preset plus
              :class:`DeltaAwardedDetector` for r/ChangeMyView delta tracking.
            - ``'aita'`` / ``'amitheasshole'``: general preset plus
              :class:`AITAVerdictDetector` for r/AmITheAsshole verdict
              detection.

    Returns:
        List of :class:`~pushshiftreader.Detector` instances with unique names,
        ready to pass to :class:`~pushshiftreader.SignalDetector`.

    Raises:
        ValueError: If ``preset`` is not a recognised name.

    Example::

        from pushshiftreader import get_detectors, SignalDetector

        sd = SignalDetector("./extracted/ChangeMyView",
                            detectors=get_detectors('cmv'))
        sd.run_all_months()
    """
    normalised = preset.strip().lower()

    if normalised == 'general':
        return _general_detectors()

    if normalised in ('cmv', 'changemyview'):
        return _general_detectors() + [
            DeltaAwardedDetector("delta_awarded"),
        ]

    if normalised in ('aita', 'amitheasshole'):
        return _general_detectors() + [
            AITAVerdictDetector("aita_verdict"),
        ]

    known = "'general', 'cmv'/'changemyview', 'aita'/'amitheasshole'"
    raise ValueError(f"Unknown preset {preset!r}. Known presets: {known}")
