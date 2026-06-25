# Docker + Git Worktree Isolation

## Summary

The old Docker/worktree failure mode is closed by architecture now. Provider containers no
longer receive an implicit bind mount of the canonical repository's shared `.git`
directory, and provider-assisted rebase resolution now runs in a private checkout with its
own real `.git/` directory. As a result, provider-side git can no longer mutate the
canonical repo's `.git/worktrees` registry for sibling tasks.

## Status

- **Fixed in architecture**: normal Docker-backed task workspaces no longer get accidental
  git access via a shared host `.git` mount.
- **Rebase exception is isolated**: when agent-side git is required for provider-assisted
  rebase conflict resolution, gza creates a private checkout with its own `.git/` and later
  imports the resulting tip back into the canonical repo host-side.
- **Host remains authoritative**: publication, force-with-lease checks, and canonical branch
  updates still happen in host task context after import.

AGENTS.md still tells agents not to treat git as a general-purpose sandbox capability, but
that instruction is now a policy boundary rather than the primary protection against
cross-task worktree damage.

## Historical Failure Mode

The original bug had two related forms:

1. A Docker-mounted git worktree exposed a `.git` **file** containing host-only absolute
   paths, so sandboxed `git status` and similar commands failed with "not a git repository".
2. Later mitigation work briefly mounted the canonical shared `.git` directory into Docker
   so sandboxed git would function, but that exposed `.git/worktrees` for the whole repo.
   A `git worktree prune` or similar operation inside one container could then unregister a
   different in-progress task's worktree.

The fixed design avoids both failure modes at once by making the default Docker path
"no shared gitdir" and the rebase path "private gitdir".

## Fixed Architecture

### Ordinary Docker-backed tasks

- The task workspace is mounted into the container.
- The provider does **not** get an implicit bind mount of the canonical repository's shared
  `.git` directory.
- A worktree `.git` file therefore does not accidentally become a live view into the host
  worktree registry.
- Host-owned git operations continue to run outside the container where canonical branch and
  worktree state is authoritative.

This makes "sandboxed git mutates another task's worktree registration" structurally
unreachable for the default provider path.

### Provider-assisted rebases

- Rebase conflict resolution sometimes needs agent-side git to run `git rebase --continue`,
  inspect rebase state, and apply conflict edits.
- For that path, gza creates a **private checkout** with a real `.git/` directory owned only
  by that checkout.
- The private checkout imports the needed local refs from the canonical repo, performs the
  provider-assisted rebase there, and never shares the canonical `.git/worktrees`
  registration.
- After provider success, the host imports the rewritten tip back into the canonical branch
  with an expected-old-SHA guard, then performs host-side publication.

That means a `git worktree prune` inside the private rebase checkout can only affect that
checkout's own registry, not sibling task worktrees in the canonical repo.

## Ownership Boundary

- Provider or agent git may operate only inside an explicitly prepared private checkout.
- Canonical branch movement, publication, and cross-task worktree lifecycle decisions remain
  host responsibilities.
- The canonical repo's `.git/worktrees` registry is not a provider-visible shared resource.

## Historical Note

This file is kept because earlier incidents and reviews refer to "the Docker worktree bug".
Its current role is to document the final architecture and the boundary that prevents that
bug class from recurring.
