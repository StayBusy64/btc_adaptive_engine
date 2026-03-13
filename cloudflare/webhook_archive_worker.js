/**
 * Cloudflare Worker — TradingView Webhook Archival Bridge
 *
 * Flow:
 *   TradingView alert  →  this Worker  →  POST /webhooks/tradingview/archive
 *                                           on the FastAPI backend
 *
 * The Worker:
 *   1. Validates the shared secret supplied by TradingView in ?secret=
 *   2. Reads the raw JSON body sent by the Pine alert
 *   3. Forwards it to the backend archive endpoint, passing the SIGNAL_KEY
 *      as the X-SIGNAL-KEY header so the backend can authorise the request
 *   4. Returns the backend's response (or an error) to TradingView
 *
 * Required Worker secrets (set via `wrangler secret put` or the dashboard):
 *   TV_WEBHOOK_SECRET   — the secret TradingView embeds in ?secret=
 *   BACKEND_SIGNAL_KEY  — the TRADINGVIEW_INGEST_SIGNAL_KEY used by FastAPI
 *   BACKEND_BASE_URL    — base URL of the deployed FastAPI backend
 *                         e.g. https://your-app.onrender.com
 *
 * TradingView alert webhook URL:
 *   https://<your-worker>.workers.dev/?secret=<TV_WEBHOOK_SECRET>
 *
 * To deploy:
 *   wrangler deploy cloudflare/webhook_archive_worker.js
 */

export default {
  /**
   * @param {Request} request
   * @param {{ TV_WEBHOOK_SECRET: string, BACKEND_SIGNAL_KEY: string, BACKEND_BASE_URL: string }} env
   * @returns {Promise<Response>}
   */
  async fetch(request, env) {
    // ------------------------------------------------------------------ //
    // Only accept POST requests
    // ------------------------------------------------------------------ //
    if (request.method !== "POST") {
      return new Response(JSON.stringify({ error: "method not allowed" }), {
        status: 405,
        headers: { "Content-Type": "application/json" },
      });
    }

    // ------------------------------------------------------------------ //
    // Validate the shared secret from the query string
    // ------------------------------------------------------------------ //
    const url = new URL(request.url);
    const secret = url.searchParams.get("secret");
    if (!secret || secret !== env.TV_WEBHOOK_SECRET) {
      return new Response(JSON.stringify({ error: "unauthorized" }), {
        status: 401,
        headers: { "Content-Type": "application/json" },
      });
    }

    // ------------------------------------------------------------------ //
    // Read the raw body — TradingView sends the Pine alert() body as JSON
    // ------------------------------------------------------------------ //
    let rawBody;
    try {
      rawBody = await request.text();
    } catch (err) {
      return new Response(JSON.stringify({ error: "failed to read body" }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      });
    }

    if (!rawBody || rawBody.trim() === "") {
      return new Response(JSON.stringify({ error: "empty body" }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      });
    }

    // ------------------------------------------------------------------ //
    // Forward to the FastAPI archive endpoint
    // ------------------------------------------------------------------ //
    const backendUrl =
      (env.BACKEND_BASE_URL || "").replace(/\/$/, "") +
      "/webhooks/tradingview/archive";

    let backendResponse;
    try {
      backendResponse = await fetch(backendUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-SIGNAL-KEY": env.BACKEND_SIGNAL_KEY || "",
          "X-Forwarded-By": "cloudflare-archive-worker",
        },
        body: rawBody,
      });
    } catch (err) {
      return new Response(
        JSON.stringify({ error: "failed to reach backend", detail: String(err) }),
        {
          status: 502,
          headers: { "Content-Type": "application/json" },
        }
      );
    }

    // Surface the backend's status and body back to TradingView
    const responseBody = await backendResponse.text();
    return new Response(responseBody, {
      status: backendResponse.status,
      headers: { "Content-Type": "application/json" },
    });
  },
};
