"""Shared review verdict parsing helpers."""

import re

_VERDICT_TOKEN = r"(APPROVED|CHANGES_REQUESTED|NEEDS_DISCUSSION)"

# Inline formats:
# - **Verdict: APPROVED**
# - **Verdict**: CHANGES_REQUESTED
# - Verdict: NEEDS_DISCUSSION
_INLINE_VERDICT_PATTERN = re.compile(
    rf"\*{{0,2}}Verdict\*{{0,2}}:\s*\*{{0,2}}{_VERDICT_TOKEN}\*{{0,2}}",
    re.IGNORECASE,
)

# Heading format:
# ## Verdict
#
# **CHANGES_REQUESTED**
_HEADING_VERDICT_PATTERN = re.compile(
    rf"#{{2,6}}\s+Verdict\s*\n+\s*\*{{0,2}}{_VERDICT_TOKEN}\*{{0,2}}",
    re.IGNORECASE,
)


def parse_review_verdict(content: str | None) -> str | None:
    """Extract a normalized review verdict from markdown content."""
    if not content:
        return None

    inline_match = _INLINE_VERDICT_PATTERN.search(content)
    if inline_match:
        return inline_match.group(1).upper()

    heading_match = _HEADING_VERDICT_PATTERN.search(content)
    if heading_match:
        return heading_match.group(1).upper()

    return None
