'use strict';

const BASE_URL = process.env.JOYTREE_BASE_URL || 'https://joytree.site';

/**
 * Thin wrapper around the JoyTree REST API. Every call is authenticated with
 * the caller's own jtk_ API key (never a server-wide credential) — the key
 * is passed in per-request from the MCP tool handler, which reads it out of
 * the Authorization header the MCP client sent when connecting.
 */
class JoyTreeClient {
  constructor(apiKey) {
    if (!apiKey || !apiKey.startsWith('jtk_')) {
      throw new Error('A JoyTree API key (starting with jtk_) is required. Find yours at joytree.site/dashboard/account, or run `joytree apikey show`.');
    }
    this.apiKey = apiKey;
  }

  async request(method, path, body) {
    const res = await fetch(`${BASE_URL}${path}`, {
      method,
      headers: {
        Authorization: `Bearer ${this.apiKey}`,
        'Content-Type': 'application/json',
      },
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });

    const text = await res.text();
    let data;
    try { data = text ? JSON.parse(text) : {}; } catch { data = { raw: text }; }

    if (!res.ok) {
      const message = (data && (data.error || data.message)) || `HTTP ${res.status}`;
      const err = new Error(message);
      err.status = res.status;
      err.data = data;
      throw err;
    }
    return data;
  }

  get(path)        { return this.request('GET', path); }
  post(path, body)  { return this.request('POST', path, body || {}); }
  patch(path, body) { return this.request('PATCH', path, body || {}); }
  del(path)         { return this.request('DELETE', path); }
}

module.exports = { JoyTreeClient, BASE_URL };
