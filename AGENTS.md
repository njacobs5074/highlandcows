# Agent Guidelines

## Branching

For any non-trivial change — new features, bug fixes, refactoring, or significant
documentation updates — create a new git branch before making edits.

Branch naming should follow the conventional commits style:

| Type | Example |
|------|---------|
| New feature | `feat/range-search` |
| Bug fix | `fix/leaf-merge-underflow` |
| Refactor | `refactor/btree-ordering` |
| Documentation | `docs/readme-api-section` |
| Chore / tooling | `chore/update-dependencies` |

Trivial changes (e.g. fixing a typo in a comment) may be made directly on the
current branch at your discretion.
