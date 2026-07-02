'use strict';

const express = require('express');
const { McpServer } = require('@modelcontextprotocol/sdk/server/mcp.js');
const { StreamableHTTPServerTransport } = require('@modelcontextprotocol/sdk/server/streamableHttp.js');
const { JoyTreeClient } = require('./joytree-client');
const { registerJoyTreeTools } = require('./tools');

const PORT = process.env.PORT || 8787;

function extractApiKey(req) {
  const auth = String(req.headers['authorization'] || '');
  if (auth.startsWith('Bearer ')) return auth.slice(7).trim();
  // Fallback for MCP clients that only support query-string auth during setup
  if (req.query && typeof req.query.api_key === 'string') return req.query.api_key;
  return null;
}

function buildServer(apiKey) {
  const server = new McpServer(
    { name: 'joytree', version: '1.0.0' },
    { capabilities: { tools: {} } }
  );

  registerJoyTreeTools(server, () => new JoyTreeClient(apiKey));
  return server;
}

const app = express();
app.use(express.json({ limit: '2mb' }));

// Health check — for the platform hosting this (e.g. JoyTree itself) to
// verify the process is alive.
app.get('/health', (_req, res) => res.status(200).json({ ok: true }));

// Stateless MCP endpoint: every request gets its own McpServer + transport,
// scoped to whichever API key was sent with THAT request. Nothing about one
// caller's session is ever visible to another.
app.post('/mcp', async (req, res) => {
  const apiKey = extractApiKey(req);
  if (!apiKey || !apiKey.startsWith('jtk_')) {
    res.status(401).json({
      jsonrpc: '2.0',
      error: { code: -32001, message: 'Missing or invalid JoyTree API key. Connect with Authorization: Bearer jtk_... (find yours at joytree.site/dashboard/account).' },
      id: null,
    });
    return;
  }

  try {
    const server = buildServer(apiKey);
    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
    res.on('close', () => { transport.close(); server.close(); });
    await server.connect(transport);
    await transport.handleRequest(req, res, req.body);
  } catch (err) {
    console.error('[joytree-mcp] request failed:', err);
    if (!res.headersSent) {
      res.status(500).json({
        jsonrpc: '2.0',
        error: { code: -32603, message: 'Internal server error' },
        id: null,
      });
    }
  }
});

// GET/DELETE on /mcp are part of the streamable-HTTP spec for server-initiated
// notifications and session teardown — not used in stateless mode, but the
// spec expects a response rather than a hard 404.
app.get('/mcp', (_req, res) => res.status(405).json({ error: 'Method not allowed in stateless mode' }));
app.delete('/mcp', (_req, res) => res.status(405).json({ error: 'Method not allowed in stateless mode' }));

app.listen(PORT, () => {
  console.log(`[joytree-mcp] listening on :${PORT}`);
});
