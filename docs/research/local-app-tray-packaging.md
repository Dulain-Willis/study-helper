# Research: packaging a local-only Django app as a tray/menubar app

Ticket: #5 (child of #1, informs packaging decision #4)

## 1. Is pystray the standard choice?

Yes, for cross-platform (macOS + Linux) it's the default pick. [pystray](https://github.com/moses-palmer/pystray) supports macOS, Linux (Xorg/GNOME/Ubuntu AppIndicator), and Windows from one API — the only mainstream library that covers both target platforms. [PyPI](https://pypi.org/project/pystray/) / [docs](https://pystray.readthedocs.io/en/latest/usage.html).

Alternatives and when they beat pystray:
- **rumps** ([jaredks/rumps](https://github.com/jaredks/rumps)) — macOS-only, but a noticeably nicer/more Pythonic API (no PyObjC boilerplate) and is exactly what comparable local tools use (see Q2). Worth it if Linux support is ever dropped, not worth it while Linux is a stated goal.
- **pywebview** — not a tray library, a native window wrapper around a web view; ships its own HTTP-server-optional mode. Useful if you want the whole app to *be* a native window rather than a browser tab + tray icon. [r0x0r/pywebview](https://github.com/r0x0r/pywebview)
- **flaskwebgui** — thin layer that opens the system browser in "app mode" pointed at your Flask/Django dev server and kills it on window close; no tray icon at all, just launch/window-lifecycle glue.
- **infi.systray** — Windows-only, not relevant here.

Recommendation: pystray, since both macOS and Linux are in scope.

## 2. How do comparable local tools actually package themselves?

- **ollama-bar** (IBM, archived May 2025, superseded by Ollama's own desktop app) is the closest precedent: a macOS menubar wrapper built on **rumps**, distributed via `make install-app` which builds and drops a `.app` bundle straight into `/Applications`. [github.com/IBM/ollama-bar](https://github.com/IBM/ollama-bar)
- Ollama's now-official desktop app follows the same shape: menubar status icon, background server process, `.app` in Applications, no visible terminal. Third-party clones (My Mac LLAMA, ModelPiper) copy this pattern rather than reinventing it.
- LM Studio (Electron-based, not Python, so a different stack) still validates the *interaction* pattern worth copying: a status/tray icon, a toggle to run the local server in the background, and the app minimizing to tray instead of quitting the server. [LM Studio docs](https://lmstudio.ai/docs/app)

Common pattern across all of them: **tray/menubar icon + a bundled or subprocess-launched local server + an `.app` (mac) or equivalent bundle you double-click**, never "open Terminal and run a command."

## 3. venv + launcher vs. full bundling (PyInstaller/py2app/Briefcase)

For a solo-maintained, single-machine tool, the well-known middle ground is: **don't freeze/bundle Python at all** — write a tiny native-feeling launcher that activates one pre-built venv and execs the tray script.

- **macOS**: [Platypus](https://sveinbjorn.org/platypus) ([source](https://github.com/sveinbjornt/Platypus)) is the standard tool for exactly this: wraps a shell/Python script in a real `.app` bundle, no Xcode, no code-signing required for local personal use, and has a built-in "status menu" app-type mode so it can host the tray icon too. This avoids Gatekeeper pain because you're not distributing to other users — a locally-built, unsigned `.app` run on your own machine only needs one "right-click > Open" the first time (or none, if you `xattr -d com.apple.quarantine` it, since it never touched the internet as a download). Actively maintained (v5.5.0, Dec 2025).
- **Linux**: a `.desktop` file with `Terminal=false` and `Exec=` pointing at a wrapper script that does `source /path/to/venv/bin/activate && exec python tray.py` is the standard, low-effort pattern — confirmed by multiple community writeups and by the existence of helper packages like [PyShortcuts](https://newville.github.io/pyshortcuts/) and `desktop-app` on PyPI that automate generating exactly this file. [gist example](https://gist.github.com/nathakits/7efb09812902b533999bda6793c5e872)
- Full bundling (PyInstaller/py2app/Briefcase) is the standard choice only when **shipping to other people's machines** where you can't assume a venv or a known Python. For a single-user, single-machine tool it's strictly more maintenance (rebuild on every dependency bump, larger artifact, its own class of packaging bugs) for no real benefit — the "one-time venv setup, launcher forever after" approach is what indie/solo tools in this exact shape (local LLM UIs, personal dashboards) actually ship.

Recommendation: one-time `venv` setup + Platypus-built `.app` (mac) / hand-written `.desktop` + wrapper script (Linux). Skip PyInstaller/py2app/Briefcase entirely — no distribution requirement to justify the overhead.

## 4. Gotchas wrapping `runserver` in a tray process

- **Reloader must be off.** Django's autoreloader re-execs the process and runs your startup code on a second thread/process; this is what causes commands to appear to run twice ([ticket #8413](https://code.djangoproject.com/ticket/8413), [django-users thread](https://groups.google.com/g/django-users/c/8qR4hQTdIbc)). Call `runserver` (or `django.core.management.execute_from_command_line`) with `use_reloader=False` / `--noreload`. You don't want live-reload in a packaged tray app anyway.
- **pystray's `icon.run()` must own the main thread on macOS** — its Cocoa backend requires the app runloop on the main thread ([pystray reference](https://pystray.readthedocs.io/en/latest/reference.html)). This means Django has to run somewhere else, not the other way around.
- **Subprocess vs. in-thread for the Django server**: prefer **subprocess** (`subprocess.Popen([sys.executable, "manage.py", "runserver", "--noreload", ...])`). Reasons, backed by real reports:
  - pystray has a documented bug where the tray icon fails to render if `pystray` is imported in a parent process before forking a subprocess ([pystray issue #87](https://github.com/moses-palmer/pystray/issues/87)) — so import pystray only in the tray-owning process, and launch Django as a separate OS process, not via `multiprocessing.fork` after pystray is already loaded.
  - Running a WSGI dev server in-thread (people hit this with Flask, same server internals Django's `runserver` uses) leads to real shutdown deadlocks: the dev server thread ignores normal interrupt/join signals, so "Quit" from the tray menu hangs waiting to join a thread that never exits ([pystray issue #91](https://github.com/moses-palmer/pystray/issues/91)). A subprocess sidesteps this — `proc.terminate()` (SIGTERM) from the tray's "Quit" handler is a clean, reliable kill that a stuck in-process thread cannot offer.
  - Subprocess also gives you free port-already-in-use handling: check the port (or just attempt the bind and catch the `OSError` from the child's stderr/exit code) before/at spawn, and show a tray notification/menu state ("already running" / "port busy") instead of trying to coordinate that inside a shared-process reloader.
- **Clean shutdown sequence**: tray "Quit" handler should (1) `proc.terminate()` the Django subprocess, (2) wait briefly with `proc.wait(timeout=...)`, (3) `proc.kill()` if it didn't exit, (4) then `icon.stop()` to unblock `icon.run()` and let the process exit. Do this in that order — stopping the icon first while the server is still shutting down is exactly the scenario in issue #91 above.

## Recommendation summary (for ticket #4)

- Tray icon: **pystray** (cross-platform requirement rules out rumps).
- Server process: Django `runserver --noreload` launched as a **subprocess** from the tray script, killed via `terminate()`/`kill()` on Quit — not run in-thread.
- Packaging: **no PyInstaller/py2app/Briefcase**. One-time venv setup, then:
  - macOS: **Platypus**-built `.app` in a "status menu" style, dropped in `/Applications`.
  - Linux: hand-rolled `.desktop` file (`Terminal=false`) + a wrapper shell script that activates the venv and execs the tray script.
