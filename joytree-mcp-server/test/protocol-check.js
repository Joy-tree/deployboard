'use strict';

const { Client } = require('@modelcontextprotocol/sdk/client/index.js');
const { StreamableHTTPClientTransport } = require('@modelcontextprotocol/sdk/client/streamableHttp.js');

async function run() {
  // ── Test 1: no API key at all → should be cleanly rejected ──────────
  console.log('--- Test 1: connecting with NO Authorization header ---');
  try {
    const noAuthTransport = new StreamableHTTPClientTransport(new URL('http://localhost:8787/mcp'));
    const noAuthClient = new Client({ name: 'test-client', version: '1.0.0' });
    await noAuthClient.connect(noAuthTransport);
    console.log('UNEXPECTED: connected without a key');
  } catch (err) {
    console.log('Correctly rejected:', err.message.slice(0, 150));
  }

  // ── Test 2: valid-shaped key → full handshake + list tools ──────────
  console.log('\n--- Test 2: connecting with a valid-shaped jtk_ key ---');
  const transport = new StreamableHTTPClientTransport(new URL('http://localhost:8787/mcp'), {
    requestInit: { headers: { Authorization: 'Bearer jtk_test_key_for_protocol_verification_only' } },
  });
  const client = new Client({ name: 'test-client', version: '1.0.0' });
  await client.connect(transport);
  console.log('Connected + initialized OK');

  const tools = await client.listTools();
  console.log(`\nRegistered tools (${tools.tools.length}):`);
  tools.tools.forEach(t => console.log('  -', t.name));

  // ── Test 3: actually call a tool — expect it to reach out to
  // joytree.site (blocked from THIS sandbox) and surface a clean error,
  // proving the whole pipeline (parse -> auth -> tool -> HTTP call ->
  // error handling) works end to end, not just the listing. ──────────
  console.log('\n--- Test 3: calling joytree_whoami (expect a network-level error, not a crash) ---');
  const result = await client.callTool({ name: 'joytree_whoami', arguments: {} });
  console.log('isError:', result.isError);
  console.log('content:', result.content[0].text.slice(0, 200));

  await client.close();
  process.exit(0);
}

run().catch(err => {
  console.error('TEST SCRIPT FAILED:', err);
  process.exit(1);
});
