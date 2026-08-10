# Contributing

This project uses a short-lived branch + pull request workflow. `main` is
protected — all changes land through a reviewed PR, no direct pushes.

## Workflow

1. **Branch off `main`.** One branch per unit of work, named `<type>/<short-description>`:
   - `feat/…` — new functionality
   - `fix/…` — bug fix
   - `chore/…` — repo/tooling maintenance, no application behavior change
   - `docs/…` — documentation only
   - `test/…` — tests only
2. **Commit as you go.** Small, focused commits with a clear message
   (`type: what changed`, e.g. `fix: handle coordinator timeout in chaos_test.py`).
3. **Push and open a PR into `main`.** Describe what changed, why, and how it
   was tested. Link the related issue if there is one.
4. **Get it reviewed.** At least one review pass before merging — either a
   collaborator or a self-review pass (documented in the PR) when working solo.
   Leave review comments on anything that should change; push follow-up
   commits to the same branch to address them rather than force-pushing over
   history mid-review.
5. **Merge via squash** once the PR is approved and any review comments are
   resolved. Delete the branch after merge.

## Local setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
docker compose up --build
```

## Running tests

```bash
pytest tests/
```
