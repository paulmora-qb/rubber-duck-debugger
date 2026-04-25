# /pr-polish

Polish and land a pull request end-to-end. Run this after pushing a branch and opening (or preparing to open) a PR.

## Steps

Work through the following phases in order. Complete each phase fully before moving to the next.

---

### Phase 1 — Open or update the PR with a filled-out template

1. Check whether a PR already exists for the current branch:
   ```
   gh pr view --json number,title,body,state 2>/dev/null
   ```

2. Read the PR template from `.github/pull_request_template.md`.

3. Inspect the branch diff and commit history to understand the change:
   ```
   git log main..HEAD --oneline
   git diff main...HEAD --stat
   ```

4. If no PR exists yet, create one using `gh pr create`. Pass the body via `--body` (use a heredoc so newlines are preserved). Fill out **every section** of the template with substantive content — no placeholder text left behind.

5. If a PR already exists, read its current body. Update any sections that are still placeholder text using `gh pr edit --body "$(cat <<'EOF' ... EOF)"`.

6. Set a clear, concise title (under 70 characters) that describes the *what*, not the implementation detail.

---

### Phase 2 — Link GitHub issues

1. Search for related issues using branch name tokens and recent commit messages:
   ```
   gh issue list --state open --limit 50 --json number,title,body
   ```
   Also check closed issues if the branch name suggests a fix:
   ```
   gh issue list --state closed --limit 20 --json number,title,body
   ```

2. Look for issue references already in commit messages or branch name (`#NNN`, `fix-NNN`, `feature/NNN`):
   ```
   git log main..HEAD --format="%s %b"
   ```

3. For each issue that is clearly related:
   - Use `Closes #N` if this PR fully resolves it.
   - Use `Related to #N` if it is connected but does not close it.

4. If you found linkable issues, update the PR body via `gh pr edit --body` to add the `Closes`/`Related to` lines under the **Linked issues** section of the template.

5. Also set the issue link on the PR directly if the repo uses GitHub Projects:
   ```
   gh pr edit --add-assignee @me
   ```

---

### Phase 3 — Run CI locally

Run each CI check locally in the order they appear in `.github/workflows/ci.yml`:

```bash
# Ruff lint
uv run ruff check .

# Ruff format check
uv run ruff format --check --diff .

# uv lock check
uv lock --check

# pre-commit hooks
uv run pre-commit run end-of-file-fixer --all-files
uv run pre-commit run trailing-whitespace --all-files

# Tests (run last — slowest)
uv run pytest
```

If any check fails:
- Fix the root cause (auto-fix where safe: `uv run ruff check --fix .` and `uv run ruff format .`).
- Re-run the failing check to confirm it passes before moving on.
- Never use `--no-verify` or skip hooks.

---

### Phase 4 — Monitor GitHub Actions

After pushing and opening the PR, watch the remote CI run:

1. Get the most recent run for this PR:
   ```
   gh run list --branch $(git branch --show-current) --limit 5
   ```

2. If a run is in progress, tail its output:
   ```
   gh run watch
   ```

3. If a job fails, fetch the full log for that job:
   ```
   gh run view <run-id> --log-failed
   ```

4. Fix the failure, commit, and push. Then go back to step 1 and repeat until all checks are green.

**If no run has started yet:** wait up to 2 minutes (check every 30 s) before concluding there is a problem. GitHub Actions can be slow to queue. If a run still has not appeared after 2 minutes, check whether the workflow triggers on this branch (`on: pull_request: branches: [main]`) and whether the PR targets the right base branch.

---

### Phase 5 — Wait for Copilot review and address comments

1. After CI is green, wait for GitHub Copilot to post its review. Poll every 60 seconds for up to 5 minutes:
   ```
   gh pr view --json reviews,comments
   gh api repos/{owner}/{repo}/pulls/{pr_number}/comments
   ```
   Replace `{owner}`, `{repo}`, and `{pr_number}` using:
   ```
   gh repo view --json owner,name
   gh pr view --json number
   ```

2. Once Copilot comments appear (look for `user.login` containing `copilot` or `github-advanced-security`), read each one carefully.

3. For each comment:
   - **Agree and fix**: Apply the suggested change. Commit with a clear message like `fix: address Copilot suggestion in <file>`. Then resolve the comment thread:
     ```
     gh api repos/{owner}/{repo}/pulls/{pr_number}/comments/{comment_id}/replies \
       -f body="Fixed in the latest commit."
     ```
   - **Disagree**: Reply with a concise explanation of why the existing code is correct, then mark it resolved the same way.
   - **Nitpick / style-only**: Apply if the change is clearly better; otherwise reply and resolve.

4. After addressing all comments, push the commits and confirm CI is still green (re-run Phase 4 quickly).

---

### Done

Report back:
- PR URL
- Issues linked (if any)
- Whether CI passed locally and on GitHub Actions
- Number of Copilot comments addressed
