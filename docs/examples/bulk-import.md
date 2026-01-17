# Bulk Task Import

Import multiple related tasks from a YAML file.

## Create the import file

```yaml
# tasks.yaml
group: api-v2
spec: specs/api-v2-design.md

tasks:
  - prompt: "Design the new REST API v2 endpoints"
    type: plan

  - prompt: "Implement user endpoints for API v2"
    type: implement
    depends_on: 1
    review: true

  - prompt: "Implement product endpoints for API v2"
    type: implement
    depends_on: 1
    review: true

  - prompt: "Add API v2 integration tests"
    type: task
    depends_on: [2, 3]
```

## Preview the import

```bash
$ gza import tasks.yaml --dry-run
Dry run - no tasks will be created

Would create 5 tasks:
  1. [plan] Design the new REST API v2 endpoints
  2. [implement] Implement user endpoints for API v2
       depends_on: 1
       auto-creates review task
  3. [implement] Implement product endpoints for API v2
       depends_on: 1
       auto-creates review task
  4. [task] Add API v2 integration tests
       depends_on: 2, 3
```

## Import the tasks

```bash
$ gza import tasks.yaml
Imported 5 tasks to group: api-v2

Created:
  20260108-design-the-new-rest-api (plan)
  20260108-implement-user-endpoints (implement)
  20260108-review-implement-user-endpoints (review)
  20260108-implement-product-endpoints (implement)
  20260108-review-implement-product-endpoints (review)
  20260108-add-api-v2-integration-tests (task)
```

## Run all tasks

Run sequentially:

```bash
$ gza work --count 6
```

Or spawn background workers for parallel execution:

```bash
$ for i in {1..3}; do gza work --background; done
```

## Import file format reference

```yaml
# Optional: group all imported tasks
group: my-group

# Optional: spec file to include as context for all tasks
spec: path/to/spec.md

tasks:
  - prompt: "Task description"        # Required
    type: task                        # Optional: task|explore|plan|implement|review
    depends_on: 1                     # Optional: index of task this depends on
    review: true                      # Optional: auto-create review task
    branch_type: feature              # Optional: branch type hint
```

See the [Configuration Reference](../configuration.md#import) for all import options.
