'use strict';

const { z } = require('zod');

function textResult(value) {
  const text = typeof value === 'string' ? value : JSON.stringify(value, null, 2);
  return { content: [{ type: 'text', text }] };
}

function errorResult(err) {
  return {
    content: [{ type: 'text', text: `Error: ${err.message}` }],
    isError: true,
  };
}

/**
 * Registers every JoyTree tool on the given McpServer instance.
 * `getClient(extra)` must return a ready JoyTreeClient for the current
 * request (it reads the caller's API key out of the MCP request context).
 */
function registerJoyTreeTools(server, getClient) {
  const tool = (name, config, handler) => {
    server.registerTool(name, config, async (args, extra) => {
      try {
        const client = getClient(extra);
        return await handler(args, client);
      } catch (err) {
        return errorResult(err);
      }
    });
  };

  // ── Identity ────────────────────────────────────────────────────────
  tool('joytree_whoami', {
    title: 'Who am I',
    description: 'Confirm the connected JoyTree account and API key scope. Use this first to verify the connection is working.',
    inputSchema: {},
  }, async (_args, client) => textResult(await client.get('/api/auth/me')));

  // ── Projects & deployments ─────────────────────────────────────────
  tool('joytree_list_projects', {
    title: 'List projects',
    description: 'List all of the current user\'s JoyTree projects, with status, live URL, and last deploy time.',
    inputSchema: {},
  }, async (_args, client) => textResult(await client.get('/api/projects')));

  tool('joytree_get_project', {
    title: 'Get project details',
    description: 'Get full details for one project by its ID (from joytree_list_projects).',
    inputSchema: { projectId: z.string().describe('The project ID') },
  }, async (args, client) => textResult(await client.get(`/api/projects/${encodeURIComponent(args.projectId)}`)));

  tool('joytree_deploy_from_github', {
    title: 'Deploy a GitHub repo',
    description: 'Deploy a project straight from a GitHub repository. This is the main "ship it" tool — call this once code is pushed and ready to go live. Framework/build settings are auto-detected if omitted.',
    inputSchema: {
      name: z.string().describe('Project name — also becomes the <name>.joytree.site subdomain unless a custom subdomain is given'),
      repoUrl: z.string().describe('GitHub repository URL, e.g. https://github.com/you/my-app'),
      branch: z.string().optional().describe('Branch to deploy (default: main)'),
      subdomain: z.string().optional().describe('Custom subdomain, if different from the project name'),
      buildCmd: z.string().optional().describe('Override the auto-detected build command'),
      startCmd: z.string().optional().describe('Override the auto-detected start command (server apps only)'),
      outputDir: z.string().optional().describe('Override the auto-detected output directory'),
      siteType: z.enum(['static', 'server']).optional().describe('Force static vs. server app instead of auto-detecting'),
    },
  }, async (args, client) => textResult(await client.post('/api/deploy', {
    name: args.name,
    subdomain: args.subdomain || args.name,
    repoUrl: args.repoUrl,
    branch: args.branch || 'main',
    buildCmd: args.buildCmd,
    startCmd: args.startCmd,
    outputDir: args.outputDir,
    siteType: args.siteType,
  })));

  tool('joytree_list_deployments', {
    title: 'List deployment history',
    description: 'List recent deployments across all projects (or filter by project), with build status.',
    inputSchema: { projectId: z.string().optional().describe('Optionally scope to one project ID') },
  }, async (args, client) => {
    const qs = args.projectId ? `?projectId=${encodeURIComponent(args.projectId)}` : '';
    return textResult(await client.get(`/api/deployments${qs}`));
  });

  tool('joytree_runtime_logs', {
    title: 'Get runtime logs',
    description: 'Fetch recent runtime logs for a project — use this to debug a live site or check a deploy actually worked.',
    inputSchema: { projectId: z.string().describe('The project ID') },
  }, async (args, client) => textResult(await client.get(`/api/projects/${encodeURIComponent(args.projectId)}/runtime-logs`)));

  tool('joytree_delete_project', {
    title: 'Delete a project',
    description: 'Permanently delete a project and take it offline. Irreversible — confirm with the user before calling this.',
    inputSchema: { projectId: z.string().describe('The project ID to delete') },
  }, async (args, client) => textResult(await client.del(`/api/projects/${encodeURIComponent(args.projectId)}`)));

  // ── Environment variables ──────────────────────────────────────────
  tool('joytree_env_list', {
    title: 'List environment variables',
    description: 'List the environment variable keys set on a project (values are not returned by the API for security).',
    inputSchema: { projectId: z.string() },
  }, async (args, client) => textResult(await client.get(`/api/projects/${encodeURIComponent(args.projectId)}/env`)));

  tool('joytree_env_set', {
    title: 'Set environment variables',
    description: 'Set one or more environment variables on a project. Takes effect on the next deploy.',
    inputSchema: {
      projectId: z.string(),
      variables: z.record(z.string()).describe('Key/value pairs to set, e.g. { "DATABASE_URL": "postgres://..." }'),
    },
  }, async (args, client) => textResult(await client.post(`/api/projects/${encodeURIComponent(args.projectId)}/env`, { variables: args.variables })));

  tool('joytree_env_delete', {
    title: 'Delete an environment variable',
    description: 'Remove a single environment variable from a project.',
    inputSchema: { projectId: z.string(), key: z.string() },
  }, async (args, client) => textResult(await client.del(`/api/projects/${encodeURIComponent(args.projectId)}/env/${encodeURIComponent(args.key)}`)));

  // ── Databases ───────────────────────────────────────────────────────
  tool('joytree_list_databases', {
    title: 'List databases',
    description: 'List all managed databases in the account.',
    inputSchema: {},
  }, async (_args, client) => textResult(await client.get('/api/databases')));

  tool('joytree_create_database', {
    title: 'Create a database',
    description: 'Provision a new managed database (PostgreSQL, MySQL, MariaDB, MongoDB, or Redis).',
    inputSchema: {
      type: z.enum(['postgres', 'mysql', 'mariadb', 'mongodb', 'redis']).describe('Database engine'),
      name: z.string().describe('Database name'),
      linkProjectId: z.string().optional().describe('If given, auto-injects DATABASE_URL into this project\'s env vars'),
    },
  }, async (args, client) => textResult(await client.post('/api/databases', {
    type: args.type,
    name: args.name,
    linkProjectId: args.linkProjectId,
  })));

  tool('joytree_get_database', {
    title: 'Get database details',
    description: 'Get connection strings and status for a database by ID.',
    inputSchema: { databaseId: z.string() },
  }, async (args, client) => textResult(await client.get(`/api/databases/${encodeURIComponent(args.databaseId)}`)));

  tool('joytree_database_lifecycle', {
    title: 'Start/stop/restart/delete a database',
    description: 'Change a database\'s running state.',
    inputSchema: {
      databaseId: z.string(),
      action: z.enum(['start', 'stop', 'restart', 'delete']),
    },
  }, async (args, client) => textResult(await client.post(`/api/databases/${encodeURIComponent(args.databaseId)}/${args.action}`)));

  // ── Realtime API Builder (prompt-to-API) ──────────────────────────
  tool('joytree_create_api_from_prompt', {
    title: 'Generate a REST API from a text prompt',
    description: 'Describe an API in plain English and get back a live REST endpoint, generated and hosted instantly — JoyTree\'s signature feature. Good for mock data, quick backends, or prototyping without writing a server by hand.',
    inputSchema: {
      prompt: z.string().describe('Plain-language description of the API, e.g. "A todo list API: create, list, complete, delete"'),
      aiVersion: z.enum(['v1', 'v2', 'v3', 'v4']).optional().describe('Generation engine (default v1 — free for everyone; v2 needs a paid plan; v3/v4 are currently admin-only)'),
    },
  }, async (args, client) => textResult(await client.post('/api/developer/flows/from-text', {
    prompt: args.prompt,
    aiVersion: args.aiVersion || 'v1',
  })));

  tool('joytree_list_generated_apis', {
    title: 'List generated APIs',
    description: 'List every API previously generated with joytree_create_api_from_prompt.',
    inputSchema: {},
  }, async (_args, client) => textResult(await client.get('/api/developer/apis')));

  tool('joytree_dockerize_api', {
    title: 'Dockerize a generated API',
    description: 'Turn a generated API flow into a persistent, standalone container with its own subdomain (rather than the lightweight shared runtime it starts on).',
    inputSchema: { flowId: z.string() },
  }, async (args, client) => textResult(await client.post(`/api/developer/flows/${encodeURIComponent(args.flowId)}/dockerize`)));

  // ── GitHub helper ───────────────────────────────────────────────────
  tool('joytree_list_github_repos', {
    title: 'List connected GitHub repos',
    description: 'List repositories available through the user\'s connected GitHub account — useful to look up the right repoUrl before calling joytree_deploy_from_github.',
    inputSchema: {},
  }, async (_args, client) => textResult(await client.get('/api/github/repos')));
}

module.exports = { registerJoyTreeTools };
