# joytree-mcp-server

The official JoyTree MCP server. Connect it once in Claude (or any other
MCP-compatible client) and a developer can deploy, manage databases, and
generate APIs on JoyTree just by asking — no CLI, no copy-pasting commands,
no leaving the chat.

This exists because Claude's own sandboxed code environment (claude.ai
chat) has a fixed, restrictive network allowlist and cannot reach
`joytree.site` directly. This server sidesteps that entirely: it's
infrastructure *JoyTree* hosts, so when Claude calls one of its tools, the
request goes to this server (not through Claude's sandbox), and this server
talks to the real JoyTree API with no restrictions at all.

Claude Code (which runs on a developer's own machine, not in a sandbox)
doesn't need this — the existing `joytree` CLI already works there directly.
This server is specifically what makes the same experience possible from
claude.ai chat, Claude Cowork, or any other MCP client that can't run local
processes.

## What it exposes

18 tools covering the core "code with Claude, ship it" loop:

- **Identity & projects** — `joytree_whoami`, `joytree_list_projects`, `joytree_get_project`, `joytree_delete_project`
- **Deploy** — `joytree_deploy_from_github`, `joytree_list_deployments`, `joytree_runtime_logs`, `joytree_list_github_repos`
- **Environment variables** — `joytree_env_list`, `joytree_env_set`, `joytree_env_delete`
- **Databases** — `joytree_list_databases`, `joytree_create_database`, `joytree_get_database`, `joytree_database_lifecycle`
- **Realtime API Builder** — `joytree_create_api_from_prompt`, `joytree_list_generated_apis`, `joytree_dockerize_api`

Every tool is a thin, direct wrapper around JoyTree's existing REST API —
no new backend logic, no duplicated business rules. If the API changes,
update the tool here.

## Auth

Each request must carry the caller's own JoyTree API key as a bearer token:

```
Authorization: Bearer jtk_xxxxxxxxxxxxxxxxxxxx
```

There is no shared/server-wide credential anywhere in this service — every
tool call is scoped to whichever key the client sent with *that* request.
A fresh `McpServer` instance is built per request (stateless mode), so
there's no session state where one caller's context could leak into
another's.

This is intentionally the simplest auth model that's still secure (a
personal API key, same as the CLI already uses) rather than a full OAuth
authorization flow. Upgrading to OAuth later — so a new developer can
connect with a login button instead of copy-pasting an API key — is a
natural next step and shouldn't require changing any tool logic, just the
auth layer in `src/server.js`.

## Running locally

```bash
npm install
npm start          # listens on :8787 (set PORT to change)
```

Point `JOYTREE_BASE_URL` at a different environment if needed (defaults to
`https://joytree.site`).

## Verifying it works

```bash
npm test
```

This starts the server and runs `test/protocol-check.js` against it — a
real MCP client that checks: an invalid/missing key is cleanly rejected,
a valid handshake correctly lists all 18 tools, and an actual tool call
makes it through the full pipeline (parse → auth → dispatch → real HTTP
call → clean error/result), not just that the server boots.

## Deploying

Container-based, same shape as JoyTree's own services — build the
`Dockerfile` and run it anywhere that can reach `joytree.site` (which is
anywhere except Claude's own sandboxed code environment — that's the
entire reason this exists). A `docker-compose.yml` alongside JoyTree's
main service, or its own small VPS/container, both work fine.

Once it's live at a public URL (e.g. `https://mcp.joytree.site/mcp`),
submit it to Anthropic's MCP directory so it shows up as a one-click
connector for any Claude user — the same way Vercel, Google Compute
Engine, and other platforms already have. It's also usable immediately as
a custom connector by URL before that, for anyone who wants to try it
early.
