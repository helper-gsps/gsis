import Operations from "../../../shared/roster-operations.js";
import { canonical, digest, one, statement } from "../../runtime/store.mjs";
import { requireValue } from "../../runtime/errors.mjs";
import {
  loadState,
  recordChanges,
  scopeRevision,
  scopeChange,
  localDate,
} from "./repository.mjs";

export const WHATSAPP_PUBLIC_MENU_TEXT =
  "*This number is no longer actively manned by staff member*\n\n" +
  "Gongshang Primary WhatsApp Notifier. Please select an action:\n\n" +
  "1. Report Absence\n" +
  "2. Urgent Message\n" +
  "3. School Layout\n" +
  "4. Collect / Return ICT\n" +
  "5. Call General Office";

export function publicMenuReplyFor(rawText, selection, origin) {
  const token = String(selection || rawText || "").trim().toLowerCase();
  if (token === "1" || token === "report-absence" || token === "report absence") {
    return {
      action: "report-absence",
      text: "Report your absence at " + origin + "/roster/#report",
    };
  }
  if (token === "2" || token === "urgent" || token === "urgent message") {
    return {
      action: "urgent-restricted",
      text:
        "Urgent Message is restricted to registered school staff and engaged relief teachers. Please contact the General Office directly.\n\n" +
        WHATSAPP_PUBLIC_MENU_TEXT,
    };
  }
  if (token === "3" || token === "layout" || token === "school layout") {
    return {
      action: "layout",
      text: "School layout: " + origin + "/layout/",
    };
  }
  if (
    token === "4" ||
    token === "devices" ||
    token === "ict" ||
    token === "collect / return ict"
  ) {
    return {
      action: "devices",
      text: "Collect / Return ICT (LORD): " + origin + "/lord/",
    };
  }
  if (
    token === "5" ||
    token === "office" ||
    token === "call general office" ||
    token === "general office"
  ) {
    return {
      action: "office",
      text: "Please contact the General Office through its usual contact channel.",
    };
  }
  return {
    action: "menu",
    text: WHATSAPP_PUBLIC_MENU_TEXT,
  };
}

export async function webhookProof(ctx) {
  const { env, body, now } = ctx,
    row = await one(
      env.DB,
      "SELECT * FROM roster_provider_events WHERE school_id=? AND event_id=?",
      [env.SCHOOL_ID, body.eventId],
    );
  requireValue(
    row && row.provider === "whatsapp" && row.event_kind === "message",
    "WEBHOOK_EVENT_INVALID",
    403,
  );
  const event = JSON.parse(row.payload_json),
    command = { operationId: "inbound:" + body.eventId, eventId: body.eventId };
  requireValue(
    body.operationId === command.operationId &&
      body.commandDigest === (await digest(command)) &&
      event.at > now - 86400000,
    "WEBHOOK_EVENT_INVALID",
    403,
  );
  const receipt = await one(
    env.DB,
    "SELECT state FROM product_operations WHERE school_id=? AND operation_id=?",
    [env.SCHOOL_ID, "event:" + body.eventId],
  );
  requireValue(
    receipt?.state === "committed",
    "WEBHOOK_EVENT_UNCOMMITTED",
    409,
  );
  return {
    schemaVersion: 1,
    schoolId: env.SCHOOL_ID,
    verified: true,
    provider: "whatsapp",
    eventId: body.eventId,
    senderMobile: event.sender,
    intendedStaffId: null,
    action: "roster.inbound",
    operationId: command.operationId,
    bodyDigest: body.commandDigest,
    resourceId: body.eventId,
    expiresAt: event.at + 86400000,
  };
}
export async function processInbound(
  job,
  env,
  now,
  { context, prepared, commit },
) {
  const eventId = job.payload.eventId,
    row = await one(
      env.DB,
      "SELECT * FROM roster_provider_events WHERE school_id=? AND event_id=?",
      [env.SCHOOL_ID, eventId],
    );
  requireValue(row, "WEBHOOK_EVENT_MISSING", 503);
  if (row.provider !== "whatsapp")
    return { state: "delivered", code: "INBOX_ONLY" };
  const event = JSON.parse(row.payload_json),
    body = { operationId: "inbound:" + eventId, eventId };
  requireValue(
    env.CORE_WEBHOOK?.authorizeEvent,
    "WEBHOOK_AUTHORITY_NOT_CONFIGURED",
    503,
  );
  const origin = env.PUBLIC_ORIGIN;
  requireValue(
    /^https:\/\/[^\s/?#]+$/.test(origin || ""),
    "PUBLIC_ORIGIN_NOT_CONFIGURED",
    503,
  );
  let principal;
  try {
    principal = await env.CORE_WEBHOOK.authorizeEvent({
      schoolId: env.SCHOOL_ID,
      eventId,
      operationId: body.operationId,
      bodyDigest: await digest(body),
      now,
    });
  } catch (err) {
    if (err?.code === "RECIPIENT_UNMATCHED") {
      // Public WhatsApp menu (#182): show the 5-item notice/menu to any sender
      // without an identity gate, while keeping Urgent Message strictly restricted.
      const reply = publicMenuReplyFor(event.text, event.buttonId, origin);
      const replyJobId = "public-menu-reply:" + eventId;
      await statement(
        env.DB,
        "UPDATE roster_inbox SET state='open',revision=revision+1,payload_json=? WHERE school_id=? AND message_id=? AND staff_id IS NULL",
        [
          canonical({
            provider: "whatsapp",
            sender: event.sender,
            text: event.text,
            buttonId: event.buttonId,
            identityVerified: false,
            actionState: reply.action,
          }),
          env.SCHOOL_ID,
          "whatsapp:" + event.id,
        ],
      ).run();
      await statement(
        env.DB,
        "INSERT OR IGNORE INTO product_outbox(school_id,job_id,operation_id,type,payload_json,state,due_at,updated_at) VALUES(?,?,?,?,?,'queued',?,?)",
        [
          env.SCHOOL_ID,
          replyJobId,
          body.operationId,
          "roster-public-menu-reply",
          canonical({
            to: event.sender,
            text: reply.text,
            action: reply.action,
            inReplyTo: "whatsapp:" + event.id,
            publicMenu: true,
          }),
          now,
          now,
        ],
      ).run();
      return { state: "delivered", providerId: replyJobId };
    }
    throw err;
  }
  const ctx = { env, principal, now, body },
    date = localDate(now),
    scope = "day:" + date,
    revision = await scopeRevision(env.DB, env.SCHOOL_ID, scope),
    range = { from: date, to: date, dates: [date] },
    loaded = await loadState(env.DB, env.SCHOOL_ID, range),
    c = await context(ctx, loaded, range),
    op = await prepared(ctx, "roster.inbound", body);
  if (op.receipt) return { state: "delivered", providerId: op.operationId };
  const next = structuredClone(loaded.state),
    result = Operations.inboundMessage(
      next,
      {
        ...c,
        service: true,
        inboxMode: true,
        urgentSMCIds: Operations.dutySMC(next, c, date),
      },
      {
        messageId: event.id,
        from: event.sender,
        timestamp: event.at,
        text: event.text,
        type: "text",
        selection: event.buttonId || undefined,
      },
    ),
    changes = await recordChanges(
      env.SCHOOL_ID,
      loaded.before,
      next,
      loaded.records,
    ),
    sc = scopeChange(env.SCHOOL_ID, scope, revision);
  changes.guards.push(sc.guard);
  changes.writes.push(sc.write, {
    sql: "UPDATE roster_inbox SET staff_id=?,state='open',revision=revision+1,payload_json=? WHERE school_id=? AND message_id=? AND staff_id IS NULL",
    args: [
      principal.staffId,
      canonical({
        provider: "whatsapp",
        sender: event.sender,
        text: event.text,
        buttonId: event.buttonId,
        identityVerified: true,
        actionState: result.reply?.action || "recorded",
      }),
      env.SCHOOL_ID,
      "whatsapp:" + event.id,
    ],
  });
  const text = {
    menu: WHATSAPP_PUBLIC_MENU_TEXT,
    "report-absence": "Report your absence at " + origin + "/roster/#report",
    layout: "School layout: " + origin + "/layout/",
    devices: "Collect / Return ICT (LORD): " + origin + "/lord/",
    office:
      "Please contact the General Office through its usual contact channel.",
    "urgent-prompt":
      "Reply with your urgent message. It will be sent to the current duty SMC.",
    "urgent-confirm":
      "Your urgent message has been recorded and queued for duty SMC.",
    "urgent-no-recipient":
      "Your message was recorded. No current duty SMC recipient is configured; please contact the school office directly.",
    "rate-limited": "Please wait before sending another request.",
    unsupported: "Please send your urgent message as text.",
  }[result.reply?.action];
  const old = new Set(loaded.state.outbox.map((j) => j.id)),
    outbox = next.outbox
      .filter((j) => !old.has(j.id))
      .map((j) => ({
        id: j.id,
        type: "roster-notification",
        payload: JSON.parse(JSON.stringify(j)),
        dueAt: j.dueAt || now,
      }));
  if (text)
    outbox.push({
      id: "inbound-reply:" + eventId,
      type: "roster-inbox-reply",
      payload: {
        staffId: principal.staffId,
        text,
        inReplyTo: "whatsapp:" + event.id,
      },
      dueAt: now,
    });
  await commit(ctx, op, {
    dependencies: c.dependencies,
    guards: [...changes.guards, ...c.localGuards],
    writes: changes.writes,
    result: {
      action: result.reply?.action || "recorded",
      eventId,
      staffId: principal.staffId,
    },
    audit: {
      eventId,
      staffId: principal.staffId,
      action: result.reply?.action || "recorded",
    },
    outbox,
  });
  return { state: "delivered", providerId: op.operationId };
}
