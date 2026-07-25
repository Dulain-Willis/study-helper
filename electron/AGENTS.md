# Agent notes — electron/

## Smoke-testing the app

To actually run the app end-to-end (not just typecheck/lint) instead of
opening a real window and clicking around by hand, drive it headlessly with
`playwright-core`'s `_electron` launcher:

```bash
npm run build   # scripts/smoke.mjs launches out/main/index.js
node scripts/smoke.mjs
```

`scripts/smoke.mjs` launches the built app with an isolated
`--user-data-dir` (so it never touches your real dev DB at
`~/Library/Application Support/electron/study-helper.db`), drives the
Groups/Sets/Cards UI via DOM clicks and `page.fill`, and asserts on
`page.content()`. Extend it in place when verifying new UI flows instead of
writing a one-off script each time.

Screenshots go to `/tmp/shots-*.png` if you add `page.screenshot()` calls —
useful when a click doesn't do what you expect and you need to see the
actual window state.
