---
description: Connect this Mac's Claude Code sessions to Memory AI → Chats (confirm on your phone)
argument-hint: <your Memory AI email>
allowed-tools: Bash(/usr/bin/python3 ${CLAUDE_PLUGIN_ROOT}/scripts/memoryai.py:*)
---
Link this Mac to the user's Memory AI account so their Claude Code sessions show up in Memory AI → Chats.

1. If no email was given in `$ARGUMENTS`, ask the user for the email they sign in to Memory AI with, then continue.
2. Run `/usr/bin/python3 ${CLAUDE_PLUGIN_ROOT}/scripts/memoryai.py connect start $ARGUMENTS`.
3. If the output has a `NUMBER:` line, tell the user that number in one short sentence and that Memory AI on their phone now asks "Claude Code wants access to Memory AI": they open it and pick that number. Do this before the next step.
4. Run `/usr/bin/python3 ${CLAUDE_PLUGIN_ROOT}/scripts/memoryai.py connect wait` with a 600000 ms timeout.
5. Tell the user the result in their language, in one or two sentences, from the script's output. Don't print the token or the config file.
