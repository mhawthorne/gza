#!/usr/bin/env bash
# Generate release notes using Claude from git commits between two tags
# Usage: ./bin/generate-release-notes.sh <from_tag> <to_tag>

set -e

# Check if two arguments are provided
if [ $# -ne 2 ]; then
    echo "Usage: $0 <from_tag> <to_tag>"
    echo "Example: $0 v1.0.0 v1.1.0"
    exit 1
fi

FROM_TAG=$1
TO_TAG=$2

# Verify the tags exist
if ! git rev-parse "$FROM_TAG" >/dev/null 2>&1; then
    echo "Error: Tag '$FROM_TAG' not found"
    exit 1
fi

if ! git rev-parse "$TO_TAG" >/dev/null 2>&1; then
    echo "Error: Tag '$TO_TAG' not found"
    exit 1
fi

# Get the list of commits between the tags
COMMITS=$(git log --pretty=format:"%h - %s (%an, %ar)" "$FROM_TAG".."$TO_TAG")

# Check if there are any commits
if [ -z "$COMMITS" ]; then
    echo "No commits found between $FROM_TAG and $TO_TAG"
    exit 1
fi

# Get detailed commit information for better context
DETAILED_COMMITS=$(git log --pretty=format:"%h - %s%n%b%n---" "$FROM_TAG".."$TO_TAG")

# Create the prompt for Claude
PROMPT="Generate release notes in markdown format for the changes between $FROM_TAG and $TO_TAG.

Here are the commits:

$DETAILED_COMMITS

Please create well-structured release notes that:
1. Start with a header: # Release Notes: $FROM_TAG → $TO_TAG
2. Include a brief summary paragraph of the overall changes
3. Group changes into categories (e.g., Features, Bug Fixes, Improvements, Documentation, etc.)
4. Use bullet points for each change
5. Be concise but informative
6. Highlight breaking changes if any are evident
7. Use proper markdown formatting

Output ONLY the markdown release notes, no additional commentary."

# Use Claude to generate the release notes
echo "Generating release notes from $FROM_TAG to $TO_TAG..."
echo ""

# Create output directory if it doesn't exist
OUTPUT_DIR="docs/release-notes"
mkdir -p "$OUTPUT_DIR"

# Sanitize the tag name for use as a filename (replace / with -)
OUTPUT_FILE="$OUTPUT_DIR/${TO_TAG//\//-}.md"

if command -v claude &> /dev/null; then
    echo "$PROMPT" | claude --print > "$OUTPUT_FILE"
    echo "Release notes written to: $OUTPUT_FILE"
else
    echo "Error: 'claude' CLI not found. Install it with: npm install -g @anthropic-ai/claude-code"
    exit 1
fi
