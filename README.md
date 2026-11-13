# Memory AI for Claude Code

<img src="memory-ai/logo.png" width="96" align="right" alt="Memory AI">

Your Claude Code sessions on your phone. Every session on your Mac becomes a chat in
[Memory AI](https://appmemory.io) → **Chats**:

- **Watch it live.** What Claude is doing right now, its steps, plan, model, images and files.
- **Get a push** when a session finishes or needs you.
- **Reply from the phone.** Your message reaches the session even while it's idle.
- **Approve or decline** a permission request or a plan from the phone. In the Claude desktop
  app and VS Code the request also stays on the Mac, and whichever you answer first counts; it
  reaches the phone after 30 seconds without keyboard or mouse. In the terminal it goes to the
  phone when you're away (2 minutes), and back at the Mac the normal dialog shows instead.
  Questions with options stay on the Mac for now.
- **Stop** a session from the phone. Claude wraps up in a sentence and stops.
- **Open a project to your team.** Share a folder with a Memory AI group: teammates see its
  sessions, write to Claude and press Stop. Permissions on your Mac stay yours.

## Install

In Claude Code (terminal or the desktop app's Code tab), run:

```
/plugin marketplace add ivanoff19/memory-ai-claude
/plugin install memory-ai@memory-ai
```

or from a shell:

```bash
claude plugin marketplace add ivanoff19/memory-ai-claude && claude plugin install memory-ai@memory-ai
```

Then start a new session (or run `/reload-plugins`).

## Connect

Nothing to type. In the first session after install, the plugin asks Memory AI to link this Mac
using the email you're signed in to Claude with:

1. Your phone gets **"Claude Code wants access to Memory AI"**.
2. The session shows a number. Pick the same number on the phone.
3. Done: this Mac's sessions show up in Chats.

Sign in to Memory AI with a different email? Run `/memory-ai:connect you@example.com` instead.
If you decline on the phone, the plugin doesn't ask again on its own for 30 days.

| Command | What it does |
| --- | --- |
| `/memory-ai:connect <email>` | Link this Mac to that Memory AI account |
| `/memory-ai:status` | Is this Mac linked, and is a session waiting for your reply |
| `/memory-ai:disconnect` | Unlink this Mac. It won't link itself again |

## What leaves your Mac

Only after you approve the link on your phone, and only to your Memory AI account
(`memory.memoryai.club`):

- each session's title, folder, model and plan;
- your prompts, Claude's replies and short lines for its steps (the last 30 lines of a group),
  read from the session transcript;
- images in your prompts and files Claude sends you, up to 10 MB each;
- the Mac's name, the plugin version, and whether you're at the Mac (idle time, not activity).

Secrets are masked before sending: private keys, Anthropic, OpenAI, GitHub, Slack, AWS, Google
and Telegram keys, JWTs, and `password=` / `token=` style values. Files under `~/.ssh`, `~/.aws`,
`~/.gnupg`, `.env*`, `.netrc`, `*.pem`, `*.key` and similar are sent by name only.

Your email is used once, to find your account for the link request. The plugin keeps its state
in `~/.memory-ai` (the device token is readable only by you). Remove a session's chat in the app
to delete it; `/memory-ai:disconnect` stops all sending.
[Privacy policy](https://appmemory.io/privacy).

## Settings

Environment variables for Claude Code (for example in `~/.claude/settings.json` → `env`):

| Variable | Effect |
| --- | --- |
| `MEMORYAI_EMAIL` | Link to this email instead of your Claude account email |
| `MEMORYAI_AUTO=0` | Don't link on your own; use `/memory-ai:connect` |
| `MEMORYAI_OFF=1` | Pause the plugin completely |
| `MEMORYAI_LANG=en` / `ru` | Language of the plugin's messages |

## Requirements

- macOS with `/usr/bin/python3` (comes with the Xcode Command Line Tools)
- Claude Code (terminal or the desktop app)
- [Memory AI](https://appmemory.io) on your iPhone or Mac

## License

[MIT](LICENSE)
