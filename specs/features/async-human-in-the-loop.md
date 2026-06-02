# Async Human-in-the-Loop for Gza Tasks

## Problem

When running Claude in print mode (`-p`), the `AskUserQuestion` tool is unavailable. This means Claude cannot ask clarifying questions mid-task - it must either guess or fail.

For long-running background tasks, this is limiting. Sometimes Claude genuinely needs human input to proceed correctly (e.g., "Should I use approach A or B?", "What's the API endpoint for X?").

## Proposed Solution

Enable an async question-answer flow:

1. Claude outputs a structured "question marker" when it needs input
2. Gza detects this, pauses the task, and stores the question
3. User is notified and can view the question
4. User provides an answer via CLI
5. Gza resumes the session with the answer injected as a new user message

## Design

### 1. Question Marker Format

Claude will output a JSON marker in its text response when it needs input:

```
<<GZA_QUESTION>>
{
  "question": "Should I use Redis or Memcached for the caching layer?",
  "context": "Both are available in the project dependencies. Redis offers more features but Memcached is simpler.",
  "options": ["Redis (recommended)", "Memcached", "Other"]
}
<</GZA_QUESTION>>
```

Fields:
- `question` (required): The question to ask the user
- `context` (optional): Additional context to help the user decide
- `options` (optional): Suggested answers (user can always provide freeform)

### 2. Task State Changes

Add new task status: `waiting_input`

New fields in task state (`.gza/tasks/<id>/state.json`):
```json
{
  "status": "waiting_input",
  "session_id": "abc123",
  "pending_question": {
    "question": "Should I use Redis or Memcached?",
    "context": "...",
    "options": ["Redis", "Memcached"],
    "asked_at": "2024-01-15T10:30:00Z"
  }
}
```

### 3. Detection Logic

In `ClaudeProvider._run_with_output_parsing()`, detect the marker pattern:

```python
import re

QUESTION_PATTERN = re.compile(
    r'<<GZA_QUESTION>>\s*(\{.*?\})\s*<</GZA_QUESTION>>',
    re.DOTALL
)

def parse_claude_output(line: str, data: dict) -> None:
    # ... existing parsing ...

    # Check accumulated text for question marker
    if "accumulated_text" not in data:
        data["accumulated_text"] = ""

    # For text content, accumulate it
    if content.get("type") == "text":
        data["accumulated_text"] += content.get("text", "")

        # Check for question marker
        match = QUESTION_PATTERN.search(data["accumulated_text"])
        if match:
            try:
                question_data = json.loads(match.group(1))
                data["pending_question"] = question_data
            except json.JSONDecodeError:
                pass
```

When a question is detected and Claude's turn ends, mark the task as `waiting_input` instead of `running`.

### 4. Prompt Injection

Add instructions to the task prompt template so Claude knows how to ask questions:

```markdown
## Asking Questions

If you need clarification or user input to proceed correctly, you can ask a question
by outputting this exact format:

<<GZA_QUESTION>>
{"question": "Your question here", "options": ["Option A", "Option B"]}
<</GZA_QUESTION>>

After outputting this marker, stop and wait. The user will provide an answer and
your session will be resumed with their response.

Only use this for genuine blockers where guessing would likely lead to wrong results.
For minor uncertainties, make a reasonable choice and document your assumption.
```

### 5. New CLI Commands

#### `gza status` enhancement

Show waiting tasks prominently:

```
$ gza status

WAITING FOR INPUT:
  #42 add-caching (waiting 2h)
      Q: Should I use Redis or Memcached for the caching layer?
      Run: gza answer 42 "Redis"

RUNNING:
  #43 fix-auth-bug (5 turns, $0.12)

COMPLETED TODAY:
  #41 update-deps ✓
```

#### `gza show <task-id>` enhancement

Display full question context:

```
$ gza show 42

Task #42: add-caching
Status: waiting_input
Branch: feature/add-caching
Started: 2h ago
Turns: 12
Cost: $0.45

PENDING QUESTION (asked 2h ago):
  Should I use Redis or Memcached for the caching layer?

  Context:
  Both are available in the project dependencies. Redis offers more
  features (pub/sub, persistence, data structures) but Memcached is
  simpler and slightly faster for pure key-value caching.

  Suggested options:
    1. Redis (recommended)
    2. Memcached
    3. Other

  Answer with: gza answer 42 "your response"
```

#### `gza answer <task-id> <response>`

Provide an answer and resume the task:

```
$ gza answer 42 "Use Redis, we'll need pub/sub later"

Resuming task #42 with your answer...
  [turn 13, 245k tokens]
  → Edit src/cache/redis_client.py
  ...
```

Implementation:
```python
def answer_task(task_id: str, response: str):
    task = load_task(task_id)

    if task.status != "waiting_input":
        print(f"Error: Task {task_id} is not waiting for input")
        return

    session_id = task.session_id
    question = task.pending_question["question"]

    # Format the resume prompt
    resume_prompt = f"User answered your question.\n\nQuestion: {question}\nAnswer: {response}\n\nPlease continue with the task."

    # Clear pending question, set status back to running
    task.pending_question = None
    task.status = "running"
    save_task(task)

    # Resume Claude with the answer
    provider.run(
        config=config,
        prompt=resume_prompt,
        log_file=task.log_file,
        work_dir=task.work_dir,
        resume_session_id=session_id,
    )
```

### 6. Notifications (optional enhancement)

For better UX, notify the user when a task needs input:

- **Terminal bell**: `print("\a")` when question detected
- **macOS notification**: `osascript -e 'display notification "Task #42 needs input" with title "gza"'`
- **Webhook**: POST to a configured URL (for Slack/Discord integration)

Configuration in `gza.toml`:
```toml
[notifications]
terminal_bell = true
macos_notification = true
webhook_url = "https://hooks.slack.com/..."
```

### 7. Timeout Handling

If a task waits too long for input, it should not block forever:

- Default timeout: 24 hours (configurable)
- After timeout, mark task as `failed` with reason `input_timeout`
- User can still answer and resume manually: `gza answer --force 42 "response"`

### 8. Edge Cases

**Multiple questions in one response**: Only capture the first one. Claude should stop after asking a question.

**Question in middle of work**: Claude might do some work, then ask a question. This is fine - the session resumes where it left off.

**Invalid JSON in marker**: Log a warning, treat as no question asked. Claude continues (or hits max_turns).

**User answers with empty string**: Reject with error message.

**Task already completed/failed**: `gza answer` should error clearly.

## Implementation Plan

1. Add `waiting_input` status and `pending_question` field to task state
2. Add question detection in `ClaudeProvider._run_with_output_parsing()`
3. Update prompt template with question instructions
4. Implement `gza answer` command
5. Update `gza status` and `gza show` to display pending questions
6. Add basic notification (terminal bell)
7. Add timeout handling
8. Documentation

## Open Questions

1. **Should questions have IDs?** If Claude asks multiple questions across resumes, might be useful to track them.

2. **Should we support multiple pending questions?** Current design: one at a time. Claude asks, waits, gets answer, continues.

3. **Should `gza answer` be blocking or background?** Probably should match original `gza run` behavior (background by default, `--wait` to block).

4. **What if Claude doesn't stop after asking?** It might continue working. We'd capture the question but the task might complete or hit max_turns anyway. Probably fine - the question is stored, user can still answer and resume if needed.
