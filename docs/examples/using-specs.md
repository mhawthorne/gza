# Using Spec Files for Context

Provide design documents or requirements as context for tasks.

## What are spec files?

Spec files are markdown documents that describe requirements, designs, or technical context. When you reference a spec file, its contents are included in the task prompt, giving the AI detailed context for implementation.

## Create a spec file

```markdown
# specs/user-preferences.md

## Overview

Add a user preferences system allowing users to customize their experience.

## Requirements

1. Store preferences in the database (not cookies)
2. Support these preference types:
   - Theme (light/dark/system)
   - Notification settings (email, push, in-app)
   - Language preference
3. Preferences should sync across devices
4. Default values for new users

## API Design

### GET /api/preferences
Returns current user's preferences.

### PATCH /api/preferences
Updates one or more preferences. Partial updates allowed.

## Database Schema

```sql
CREATE TABLE user_preferences (
    user_id INTEGER PRIMARY KEY REFERENCES users(id),
    theme VARCHAR(20) DEFAULT 'system',
    notifications_email BOOLEAN DEFAULT true,
    notifications_push BOOLEAN DEFAULT true,
    language VARCHAR(10) DEFAULT 'en',
    updated_at TIMESTAMP
);
```

## Edge Cases

- Handle missing preferences (use defaults)
- Validate theme values
- Rate limit preference updates
```

## Reference the spec in a task

```bash
$ gza add --spec specs/user-preferences.md \
  "Implement the user preferences API endpoints"

Created task: 20260108-implement-the-user-preferences
Spec: specs/user-preferences.md
```

The spec content is stored with the task and included when the AI runs.

## Spec with plan → implement workflow

For larger features, use a spec to guide the planning phase:

```bash
# Create a plan task with the spec
$ gza add --type plan --spec specs/user-preferences.md \
  "Design the implementation approach for user preferences"

Created task #1: 20260108-design-the-implementation (plan)

# Run the plan
$ gza work 1

# Review the plan in .gza/plans/
$ cat .gza/plans/20260108-design-the-implementation.md

# Create implementation based on the plan (use task ID, not slug)
# The plan already has the spec context, so no need to pass --spec again
$ gza add --type implement --based-on 1 \
  "Implement user preferences per the plan"

Created task #2: 20260108-implement-user-preferences (implement)
Based on: #1
```

## Bulk import with shared spec

When importing multiple tasks, set a spec for all of them:

```yaml
# tasks.yaml
group: user-preferences
spec: specs/user-preferences.md

tasks:
  - prompt: "Implement the preferences database schema and migrations"
    type: implement
    review: true

  - prompt: "Implement the GET /api/preferences endpoint"
    type: implement
    depends_on: 1
    review: true

  - prompt: "Implement the PATCH /api/preferences endpoint"
    type: implement
    depends_on: 1
    review: true

  - prompt: "Add integration tests for the preferences API"
    type: task
    depends_on: [2, 3]
```

```bash
$ gza import tasks.yaml
Imported 4 tasks to group: user-preferences
All tasks include spec: specs/user-preferences.md
```

## Tips for effective specs

1. **Be specific** - Include concrete requirements, not vague goals
2. **Include examples** - Sample API requests/responses, SQL schemas
3. **Define edge cases** - What should happen in error scenarios?
4. **Keep it focused** - One spec per feature, not a monolithic document
5. **Version with code** - Store specs in `specs/` and commit them

## Viewing a task's spec

```bash
$ gza show 2
Task #2: 20260108-implement-user-preferences
Status: pending
Spec: specs/user-preferences.md
Based on: #1
...
```
