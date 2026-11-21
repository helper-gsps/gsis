// v1 Roster public WhatsApp menu with restricted Urgent Message (ported from #182).
// Real built Workers/D1 + signed WhatsApp webhook + synthetic WhatsApp provider.
import assert from "node:assert/strict";
import { productSuite, origin } from "./v1-product/suite.mjs";
import {
  WHATSAPP_PUBLIC_MENU_TEXT,
  publicMenuReplyFor,
} from "../v1/services/roster/inbound.mjs";

// 1. Exact owner notice & 5-item menu text + public selection routing.
assert(WHATSAPP_PUBLIC_MENU_TEXT.includes("*This number is no longer actively manned by staff member*"));
assert(WHATSAPP_PUBLIC_MENU_TEXT.includes("1. Report Absence"));
assert(WHATSAPP_PUBLIC_MENU_TEXT.includes("2. Urgent Message"));
assert(WHATSAPP_PUBLIC_MENU_TEXT.includes("3. School Layout"));
assert(WHATSAPP_PUBLIC_MENU_TEXT.includes("4. Collect / Return ICT"));
assert(WHATSAPP_PUBLIC_MENU_TEXT.includes("5. Call General Office"));
assert.equal(publicMenuReplyFor("1", null, origin).action, "report-absence");
assert.equal(publicMenuReplyFor("2", null, origin).action, "urgent-restricted");
assert.equal(publicMenuReplyFor("3", null, origin).action, "layout");
assert.equal(publicMenuReplyFor("4", null, origin).action, "devices");
assert.equal(publicMenuReplyFor("5", null, origin).action, "office");
assert.equal(publicMenuReplyFor("hello", null, origin).action, "menu");

const signingSecret = "synthetic-wa-menu-secret";
const phoneId = "987654321";
const sentMessages = [];

const suite = await productSuite({
  bindings: {
    SCHEDULER_OWNER: "v1",
    WEBHOOK_OWNER: "v1",
    SEND_OWNER: "v1",
    WHATSAPP_APP_SECRET: signingSecret,
    WHATSAPP_PHONE_NUMBER_ID: phoneId,
    WHATSAPP_ACCESS_TOKEN: "synthetic-wa-token",
    PUBLIC_ORIGIN: origin,
  },
  providerFetch: async (request) => {
    const url = new URL(request.url);
    if (url.host === "graph.facebook.com") {
      const payload = await request.json();
      sentMessages.push({
        to: payload.to,
        body: payload.text?.body || "",
      });
      return Response.json({
        messages: [{ id: "wamid.out-" + sentMessages.length }],
      });
    }
    throw Error("Unexpected provider call: " + request.url);
  },
});

try {
  const admin = await suite.seedAccount("wa-menu-admin");
  // Enable roster.inbound service grant so Core verifies webhook & returns RECIPIENT_UNMATCHED for unknown sender.
  await suite.call(admin, "core", "/admin/service-grants", {
    operationId: "wa-menu-grant-inbound",
    service: "roster",
    capability: "roster.inbound",
    enabled: true,
    revision: 0,
  });

  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(signingSecret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  async function postWhatsAppMessage(msgId, from, text) {
    const raw = JSON.stringify({
      entry: [
        {
          changes: [
            {
              value: {
                metadata: { phone_number_id: phoneId },
                messages: [
                  {
                    id: msgId,
                    from,
                    timestamp: String(Math.floor(Date.now() / 1000)),
                    type: "text",
                    text: { body: text },
                  },
                ],
              },
            },
          ],
        },
      ],
    });
    const bytes = new Uint8Array(
      await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(raw)),
    );
    const sig = [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
    return suite.mf.dispatchFetch(origin + "/webhooks/whatsapp", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-hub-signature-256": "sha256=" + sig,
      },
      body: raw,
    });
  }

  // 2. Unknown sender sends "Hi" -> receives exact 5-item public menu notice.
  const r1 = await postWhatsAppMessage("wamid.in.1", "6598765432", "Hi");
  assert.equal(r1.status, 200, await r1.text());
  await suite.trigger("roster");
  await suite.trigger("roster");
  assert(
    sentMessages.some(
      (m) =>
        m.to.includes("98765432") &&
        m.body.includes("*This number is no longer actively manned by staff member*") &&
        m.body.includes("1. Report Absence") &&
        m.body.includes("5. Call General Office"),
    ),
    "unknown sender receives exact 5-item public menu: " + JSON.stringify(sentMessages),
  );

  // 3. Unknown sender selects "3" -> receives School Layout link.
  const r2 = await postWhatsAppMessage("wamid.in.2", "6598765432", "3");
  assert.equal(r2.status, 200, await r2.text());
  await suite.trigger("roster");
  await suite.trigger("roster");
  assert(
    sentMessages.some(
      (m) =>
        m.to.includes("98765432") &&
        m.body.includes(origin + "/layout/"),
    ),
    "unknown sender receives School Layout link: " + JSON.stringify(sentMessages),
  );

  // 4. Unknown sender selects "2" (Urgent Message) -> refused cleanly, ZERO SMC notifications queued.
  const r3 = await postWhatsAppMessage("wamid.in.3", "6598765432", "2");
  assert.equal(r3.status, 200, await r3.text());
  await suite.trigger("roster");
  await suite.trigger("roster");
  assert(
    sentMessages.some(
      (m) =>
        m.to.includes("98765432") &&
        m.body.includes("Urgent Message is restricted"),
    ),
    "unknown sender refused Urgent Message: " + JSON.stringify(sentMessages),
  );
  const rosterDb = await suite.db("roster");
  const smcJobs = (
    await rosterDb
      .prepare(
        "SELECT COUNT(*) AS n FROM product_outbox WHERE type='roster-notification'",
      )
      .first()
  ).n;
  assert.equal(smcJobs, 0, "unknown sender cannot queue any duty-SMC notification");

  console.log(
    "PASS v1 Roster public WhatsApp menu (#182 port): unknown senders receive 5-item notice & public links while Urgent Message stays strictly restricted",
  );
} finally {
  await suite.close();
}
