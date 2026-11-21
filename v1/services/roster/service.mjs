import { statisticsReport } from "./statistics.mjs";
import {
  createRosterContact,
  updateRosterContactMobile,
} from "./contact-create.mjs";
import {updateCapacityWarnings,refreshCapacityMessage,refreshNewSummaries,capacityTemplateData} from "./capacity-notifications.mjs";
import { forecastCapacity, layeredHealth } from "./capacity.mjs";
import { allocationSnapshot } from "./allocation-snapshot.mjs";
import { hasDatedReliefActivation, isReliefNotificationKind } from "./eligibility.mjs";
import { compactOpeningSave } from './opening-save.mjs';
import {
  applyConfirmedReplacementToState,
  handleAbsenceIntakeOperation,
  prepareSaveAbsenceClashCheck,
  queueAbsenceIntakeClash,
} from "./absence-clash.mjs";
import {
  handleDeliveryReviewOperation,
  handleWhatsappMediaRoute,
  handleWhatsappReportRoute,
} from "./delivery-review.mjs";
import { handleBackupLedger } from "./backup-ledger.mjs";
import { handleArchiveColdOutput, handleEdgeArchive } from "./archive.mjs";
import Stapler from "../../../shared/roster-stapler.js";
import {
  exportDrive,
  reconcileDrive,
  burrowsPackage,
  exportsView,
  daySource,
  readableDay,
} from "./exports.mjs";
import { webhookProof, processInbound } from "./inbound.mjs";
import { external, externalProof, issueLink } from "./external.mjs";
export { external };
import Operations from "../../../shared/roster-operations.js";
import Roster from "../../../shared/roster.js";
import Allocation from "../../../shared/roster-allocation.js";
import { ApiError, requireValue, identifier } from "../../runtime/errors.mjs";
import {
  canonical,
  digest,
  query,
  one,
  statement,
  prepareOperation,
  renewPreparedOperation,
  commitOperation,
  abortOperation,
  operationStatus,
} from "../../runtime/store.mjs";
import {
  admit,
  resolveAdmission,
  serviceCall,
  inspectDecision,
  requireMembership,
} from "../../runtime/security.mjs";
import { outboxHealth, drainOutbox } from "../../runtime/jobs.mjs";
import { dispatchProvider } from "../../runtime/providers.mjs";
import { recordProviderReceipt } from "../../runtime/receipts.mjs";
import {
  loadState,
  recordChanges,
  compactAdministrativeSave,
  accountingChanges,
  weekOf,
  scopeRevision,
  scopeChange,
  requestedDates,
  localDate,
  dateOnly,
  writeArtifact,
  readArtifact,
} from "./repository.mjs";
import { context, baselines, workloadOperations } from "./context.mjs";
import {
  completePublishedDuties,
  correctionPlan,
  dutiesReport,
  ledgerCSV,
  dutySnapshot,
} from "./ledger.mjs";
import {
  allocate,
  runEngine,
  defaultPolicy,
  validatePolicy,
  resolveLayer,
} from "../../allocator/index.mjs";
const ownReads = new Set([
  "currentDuty",
  "teacherDailyUpdate",
  "history",
  "acknowledgementStatus",
  "receiptStatus",
  "staplerDates",
  "stapler",
  "dailyTimetable",
  "timetableDay",
  "timetableBatch",
  "nextSchoolDay",
  "directory",
]);
const reads = new Set([
  "capacityForecast",
  ...Operations.READS,
  ...ownReads,
  "allocationVersion",
  "statistics",
  "statisticsRPercent",
  "statisticsCalendar",
  "calculations",
  "allocationLearning",
  "inboxList",
  "inboxThread",
  "inboxTemplates",
  "archivedItem",
  "archiveItems",
  "deliveryReview",
]);
const ownWrites = new Set([
  "saveAbsence",
  "saveInstructions",
  "deleteAbsences",
  "requestMutual",
  "respondMutual",
  "acknowledge",
  "requestStapler",
  "resolveAbsenceIntake",
  "retryAbsenceIntake",
]);
const adminWrites = new Set([
  "saveSystemConfig",
  "saveTypes",
  "saveOpeningBalances",
  "importRHistory",
  "verifyRYear",
  "saveNameBridge",
  "deleteNameBridge",
  "saveDailyEmploymentSettings",
  "prune",
  "cancelPendingNotifications",
  "archiveColdOutput",
]);
const stapleWrites = new Set([
  "createStapler",
  "importStapler",
  "saveStapler",
  "confirmStapler",
  "publishStapler",
  "resetStapler",
  "renameStapler",
  "moveStapler",
  "deleteStapler",
  "restoreStapler",
  "decideStapler",
]);
const writes = new Set([
  ...ownWrites,
  ...adminWrites,
  ...stapleWrites,
  "saveAdhoc",
  "deleteAdhoc",
  "saveRecessSettings",
  "saveRecess",
  "deleteRecess",
  "saveCustomInstruction",
  "deleteCustomInstruction",
  "setCustomInstructionEnabled",
  "publish",
  "notifyDaily",
  "sendDailyEmploymentReport",
  "assign",
  "combine",
  "undo",
  "unassign",
  "returnAutomatic",
  "removeAssignment",
  "setNoRelief",
  "noRelief",
  "reliefBatch",
  "autoAllocate",
  "optimise",
  "setDutySMC",
  "correctRelief",
  "confirmCompleted",
  "cancelAllocationRun",
  "inboxReply",
  "inboxAssign",
  "inboxSetHandled",
  "contactCreate",
  "adHocUpdateMobile",
  "dismissDeliveryIssues",
]);
const leaseOperations = new Map([
  ["acquireStaplerLock", "acquire"],
  ["renewStaplerLock", "renew"],
  ["releaseStaplerLock", "release"],
]);
const ownViews = new Set([
  "report",
  "reportForm",
  "entry",
  "mine",
  "duties",
  "currentDuty",
  "teacherDailyUpdate",
  "allocationLearning",
  "stapler",
  "staplerDates",
]);
function operation(body, path) {
  return (
    body.operation ||
    (path === "/read"
      ? "read"
      : path === "/operations"
        ? "read"
        : path.slice(1))
  );
}
export function policy(method, path, body = {}) {
  if (path === "/external")
    return {
      public: true,
      action: "roster.external",
      write: method !== "GET",
      resourceId: "external",
    };
  if (path.startsWith("/internal/"))
    return {
      internal: true,
      action: "internal.roster",
      write: false,
      resourceId: body.operationId || body.date || "internal",
    };
  if (path === "/reference")
    return { action: "roster.admin", write: false, resourceId: "reference" };
  if (path === "/read")
    return {
      action: "roster.own",
      write: false,
      resourceId: body.id || body.date || "baseline",
    };
  if (["/profile", "/operations"].includes(path)) {
    const op = operation(body, path);
    if (path === "/profile")
      return { action: "roster.own", write: false, resourceId: "profile" };
    if (leaseOperations.has(op))
      return { action: "roster.own", write: true, resourceId: body.date };
    if (reads.has(op) || op === "read")
      return {
        action:
          ownReads.has(op) || (op === "read" && ownViews.has(body.view))
            ? "roster.own"
            : "roster.read",
        write: false,
        resourceId: body.date || body.id || op,
      };
    requireValue(writes.has(op), "UNKNOWN_OPERATION", 404);
    return {
      action: adminWrites.has(op)
        ? "roster.admin"
        : ownWrites.has(op) || (stapleWrites.has(op) && op !== "decideStapler")
          ? "roster.own"
          : op === "publish"
            ? "roster.publish"
            : ["autoAllocate", "optimise"].includes(op)
              ? "roster.allocate"
              : "roster.write",
      write: true,
      resourceId: body.date || body.id || op,
    };
  }
  if (["/upload", "/update", "/remove"].includes(path))
    return {
      action: "roster.admin",
      write: true,
      resourceId: body.id || "baseline",
    };
  if (path === "/duties" || path === "/duties/export")
    return { action: "roster.own", write: false, resourceId: "duties" };
  if (path === "/duties/correct")
    return { action: "roster.write", write: true, resourceId: body.dutyId };
  if (path === "/layers")
    return {
      action: method === "GET" ? "roster.read" : "roster.admin",
      write: method !== "GET",
      resourceId: "layers",
    };
  if (path === "/lease")
    return { action: "roster.own", write: true, resourceId: body.date };
  if (path === "/health")
    return { action: "roster.admin", write: false, resourceId: "health" };
  if (["/exports", "/exports/download"].includes(path))
    return {
      action: "roster.read",
      write: false,
      resourceId: body.id || "exports",
    };
  if (path === "/exports/reconcile")
    return {
      action: "roster.admin",
      write: true,
      resourceId: body.publicationId,
    };
  if (["/offline/preview", "/offline/confirm"].includes(path))
    return {
      action: "roster.own",
      write: path.endsWith("/confirm"),
      resourceId: body.basePublicationId,
    };
  if (path === "/source-status")
    return {
      action: "roster.read",
      write: false,
      resourceId: body.operationId,
    };
  if (path.startsWith("/whatsapp/reports/"))
    return {
      action: "roster.read",
      write: false,
      resourceId: path.slice("/whatsapp/reports/".length) || "report",
    };
  if (path.startsWith("/whatsapp/media/"))
    return {
      action: "roster.read",
      write: false,
      resourceId: path.slice("/whatsapp/media/".length) || "media",
    };
  if (path === "/backup-ledger")
    return {
      action: "roster.admin",
      write: ["verify", "restore"].includes(body.operation),
      resourceId: body.manifestFileId || "backup-ledger",
    };
  if (path === "/edge/archive")
    return {
      action:
        body.view === "openingBalances"
          ? "roster.admin"
          : body.view === "statistics"
            ? "roster.read"
            : "roster.own",
      write: false,
      resourceId: body.view || "archive",
    };
  throw new ApiError("UNKNOWN_OPERATION", 404, "Unknown Roster operation.");
}
function assertScope(ctx, descriptor) {
  requireValue(
    ctx.principal && ctx.principal.schoolId === ctx.env.SCHOOL_ID,
    "SCHOOL_MISMATCH",
    403,
  );
  requireValue(
    !descriptor.write || !ctx.principal.impersonating,
    "IMPERSONATION_DENIED",
    403,
  );
  requireValue(
    ctx.principal.capabilities.includes("admin") ||
      ctx.principal.capabilities.includes(descriptor.action),
    "CAPABILITY_DENIED",
    403,
  );
}
function safeLegacyError(e) {
  if (e instanceof ApiError) return e;
  return new ApiError(
    e.code || "ROSTER_RULE_REJECTED",
    e.status || 400,
    e.message,
    { retryable: e.status === 409 || e.status >= 500 },
  );
}
async function prepared(ctx, action, intent) {
  let op = await prepareOperation(ctx.env.DB, {
    schoolId: ctx.principal.schoolId,
    actorId: ctx.principal.actorId,
    operationId: identifier(
      ctx.body.operationId || ctx.body.requestId,
      "operationId",
    ),
    action,
    intent,
    now: ctx.now,
    validUntil: ctx.principal.validUntil,
    authorization: ctx.principal,
  });
  if (
    op.state === "prepared" &&
    (op.validUntil <= ctx.now || op.decisionId !== ctx.principal.decisionId)
  )
    op = await renewPreparedOperation(ctx.env.DB, op, ctx.principal, ctx.now);
  return op;
}
async function commit(
  ctx,
  op,
  {
    dependencies = [],
    guards = [],
    writes = [],
    result,
    audit = {},
    outbox = [],
  },
) {
  const admission = await admit(
    ctx.env,
    ctx.env.DB,
    op,
    ctx.principal,
    dependencies,
    ctx.now,
  );
  try {
    const commitNow = ctx.clock?.() ?? Date.now();
    guards = guards.map((g) =>
      g.deadlineAt !== undefined
        ? {
            sql: "SELECT MAX(?,CAST((julianday('now')-2440587.5)*86400000 AS INTEGER)) < ? AS ok",
            args: [commitNow, g.deadlineAt],
          }
        : g,
    );
    const receipt = await commitOperation(ctx.env.DB, op, {
      now: commitNow,
      validUntil: admission.validUntil,
      guards,
      writes,
      result,
      audit,
      outbox,
      admission,
    });
    await resolveAdmission(
      ctx.env,
      ctx.env.DB,
      op,
      admission,
      receipt,
      ctx.now,
    ).catch(() => {});
    return { ...receipt.result, receipt };
  } catch (e) {
    if (e.status === 409) {
      const outcome = await abortOperation(ctx.env.DB, op, e.code, ctx.now);
      await resolveAdmission(
        ctx.env,
        ctx.env.DB,
        op,
        admission,
        outcome,
        ctx.now,
      ).catch(() => {});
    }
    throw e;
  }
}
function parameters(ctx) {
  return {
    ...Object.fromEntries(
      new URL(ctx.request?.url || "https://test/").searchParams,
    ),
    ...(ctx.path === "/external" &&
    ctx.request?.headers.get("x-gsis-external-token")
      ? { token: ctx.request.headers.get("x-gsis-external-token") }
      : {}),
    ...ctx.body,
  };
}
async function recordContext(ctx, loaded, range) {
  const people = [];
  const ids = new Set();
  for (const a of [...loaded.state.absences, ...loaded.state.assignments])
    for (const id of [a.staffId, a.absentId])
      if (id && !ids.has(id)) {
        ids.add(id);
        people.push({
          id,
          name: a.staffId === id ? a.name || id : a.absentName || id,
          active: false,
          mobile: "",
        });
      }
  return {
    user: {
      staffId: ctx.principal.staffId,
      email: ctx.principal.email || ctx.principal.actorId,
      smc: ctx.principal.capabilities.includes("roster.read"),
      admin:
        ctx.principal.capabilities.includes("admin") ||
        ctx.principal.capabilities.includes("roster.admin"),
      accessVerified: true,
    },
    people,
    days: {},
    timetableEntries: [],
    today: localDate(ctx.now),
    now: ctx.now,
    iso: new Date(ctx.now).toISOString(),
    uuid: () => crypto.randomUUID(),
    smcIds: [],
    adminRecipients: [],
    notificationsConfigured: false,
    dependencies: [],
    calendarByDate: {},
    entries: [],
  };
}
function recordOnly(op, body) {
  return (
    [
      "history",
      "audit",
      "receiptStatus",
      "archiveItems",
      "archivedItem",
      "statistics",
      "statisticsCalendar",
    ].includes(op) ||
    (op === "read" && ["duties", "mine", "prune"].includes(body.view))
  );
}
async function bootstrap(ctx) {
  const range = requestedDates(ctx.body, ctx.now),
    loaded = await loadState(ctx.env.DB, ctx.principal.schoolId, range),
    c = await context(ctx, loaded, range),
    revision = await scopeRevision(
      ctx.env.DB,
      ctx.principal.schoolId,
      "baselines",
    ),
    selected = Roster.currentOrNext(c.entries, localDate(ctx.now));
  return {
    schema: 1,
    user: c.user,
    revision,
    entries: c.entries.map(({ modelKey, digest, mapping, ...e }) => ({ ...e, ...publicBaseline(e) })),
    active:
      selected.selection === "current" ? publicBaseline(selected.entry) : null,
    upcoming:
      selected.selection === "upcoming" ? publicBaseline(selected.entry) : null,
    selected: publicBaseline(selected.entry),
    selection: selected.selection,
    types: loaded.state.types,
    historyCoverage: Operations.historyCoverage(loaded.state),
    people: c.people.map((p) => ({
      id: p.id,
      name: p.name,
      active: p.active,
      role: p.role,
      employment: p.employment,
    })),
    days: c.days,
    calendarByDate: c.calendarByDate,
    reportForm: Operations.reportFormView(loaded.state, c),
  };
}
async function sourceStatus(ctx) {
  const op = await operationStatus(
    ctx.env.DB,
    ctx.env.SCHOOL_ID,
    ctx.body.operationId,
  );
  if (!op) return { state: "unknown", operationId: ctx.body.operationId };
  const prepared = await one(
    ctx.env.DB,
    "SELECT * FROM roster_prepared_publications WHERE school_id=? AND operation_id=?",
    [ctx.env.SCHOOL_ID, ctx.body.operationId],
  );
  return {
    ...op,
    ...(prepared
      ? {
          sourceDomain: "roster",
          sourceOperationId: op.operationId,
          sourceDigest: op.digest,
          sourceFence: op.fence,
          projection: JSON.parse(prepared.projection_json),
        }
      : {}),
  };
}
export async function handle(ctx) {
  ctx = { ...ctx, body: parameters(ctx) };
  if (ctx.path === "/external")
    return external(ctx, { context, prepared, commit, captureObservedQuotas });
  if (ctx.path.startsWith("/internal/")) {
    await requireMembership(ctx.env, ctx.request);
    requireValue(
      !ctx.body.schoolId || ctx.body.schoolId === ctx.env.SCHOOL_ID,
      "SCHOOL_MISMATCH",
      403,
    );
    if (
      ["/internal/source-status", "/internal/operation-status"].includes(
        ctx.path,
      )
    )
      return sourceStatus(ctx);
    if (ctx.path === "/internal/webhook-proof") return webhookProof(ctx);
    if (ctx.path === "/internal/external-proof") return externalProof(ctx);
    if (ctx.path === "/internal/adopt-publication")
      return adoptPublication(ctx);
    if (ctx.path === "/internal/source-projection-chunk") {
      const op = await operationStatus(
          ctx.env.DB,
          ctx.env.SCHOOL_ID,
          ctx.body.operationId,
        ),
        row = await one(
          ctx.env.DB,
          "SELECT * FROM roster_projection_chunks WHERE school_id=? AND operation_id=? AND chunk_index=?",
          [ctx.env.SCHOOL_ID, ctx.body.operationId, ctx.body.index],
        );
      requireValue(op && row, "SOURCE_CHUNK_NOT_FOUND", 404);
      return {
        schoolId: ctx.env.SCHOOL_ID,
        operationId: op.operationId,
        digest: op.digest,
        fence: op.fence,
        index: row.chunk_index,
        chunkDigest: row.chunk_digest,
        rows: await readArtifact(
          ctx.env.ARTIFACTS,
          row.artifact_key,
          row.chunk_digest,
        ),
      };
    }
    if (ctx.path === "/internal/source-current") {
      await inspectDecision(ctx.env, ctx.body.decisionId, {
        schoolId: ctx.env.SCHOOL_ID,
        actions: ["venues.read", "venues.book", "venues.admin"],
      });
      const saved = await one(
        ctx.env.DB,
        "SELECT projection_json FROM roster_prepared_publications WHERE school_id=? AND operation_id=?",
        [ctx.env.SCHOOL_ID, ctx.body.sourceOperationId],
      );
      if (!saved) return { current: false };
      const projection = JSON.parse(saved.projection_json),
        revision =
          projection.sourceKind === "baseline"
            ? await scopeRevision(ctx.env.DB, ctx.env.SCHOOL_ID, "baselines")
            : (
                await one(
                  ctx.env.DB,
                  "SELECT MAX(sequence) AS revision FROM roster_publications WHERE school_id=? AND service_date=?",
                  [ctx.env.SCHOOL_ID, projection.date],
                )
              )?.revision;
      return {
        current:
          revision ===
          (projection.sourceKind === "baseline"
            ? projection.revision
            : projection.sequence),
        revision,
      };
    }
    if (ctx.path === "/internal/current-occupancy") {
      await inspectDecision(ctx.env, ctx.body.decisionId, {
        schoolId: ctx.env.SCHOOL_ID,
        actions: [
          "venues.read",
          "venues.write",
          "venues.reserve",
          "venues.book",
          "venues.admin",
        ],
      });
      const from = dateOnly(ctx.body.from),
        to = dateOnly(ctx.body.to),
        rows = await query(
          ctx.env.DB,
          "SELECT * FROM roster_publications p WHERE school_id=? AND service_date BETWEEN ? AND ? AND sequence=(SELECT MAX(q.sequence) FROM roster_publications q WHERE q.school_id=p.school_id AND q.service_date=p.service_date) ORDER BY service_date",
          [ctx.env.SCHOOL_ID, from, to],
        );
      return {
        schoolId: ctx.env.SCHOOL_ID,
        sourceDomain: "roster",
        asOf: ctx.now,
        publications: rows.map((r) => JSON.parse(r.payload_json).projection),
        coverageComplete:
          rows.length ===
          Math.floor((Date.parse(to) - Date.parse(from)) / 86400000) + 1,
      };
    }
    throw new ApiError("UNKNOWN_OPERATION", 404);
  }
  const desc = policy(ctx.method, ctx.path, ctx.body);
  assertScope(ctx, desc);
  if (ctx.path === "/profile") return bootstrap(ctx);
  if (ctx.path === "/reference") return reference(ctx);
  if (ctx.path === "/read") return baselineRead(ctx);
  if (ctx.path === "/health")
    return {
      schemaVersion: 1,
      owner: "roster",
      outbox: await outboxHealth(ctx.env.DB, ctx.env.SCHOOL_ID, ctx.now),
      runs: await query(
        ctx.env.DB,
        "SELECT run_id,state,service_date,updated_at,progress_json FROM roster_runs WHERE school_id=? AND state<>'committed' ORDER BY updated_at LIMIT 100",
        [ctx.env.SCHOOL_ID],
      ),
      scopeRevisions: await query(
        ctx.env.DB,
        "SELECT scope,revision FROM roster_scopes WHERE school_id=? ORDER BY scope LIMIT 512",
        [ctx.env.SCHOOL_ID],
      ),
    };
  if (ctx.path === "/source-status") return sourceStatus(ctx);
  if (ctx.path === "/exports")
    return exportsView(ctx.env, ctx.principal.schoolId);
  if (ctx.path === "/exports/download") {
    const row = await one(
      ctx.env.DB,
      "SELECT * FROM roster_exports WHERE school_id=? AND export_id=?",
      [ctx.principal.schoolId, ctx.body.id],
    );
    requireValue(row, "EXPORT_NOT_FOUND", 404);
    const value = await readArtifact(
      ctx.env.ARTIFACTS,
      row.artifact_key,
      row.digest,
    );
    return new Response(canonical(value), {
      headers: {
        "content-type": "application/json",
        "content-disposition": 'attachment; filename="BURROWS-readiness.json"',
        "cache-control": "no-store",
      },
    });
  }
  if (ctx.path === "/exports/reconcile") {
    const op = await prepared(ctx, "roster.export-reconcile", ctx.body);
    if (op.receipt) return op.receipt;
    const result = await reconcileDrive(
      ctx.env,
      ctx.principal.schoolId,
      ctx.body.publicationId,
    );
    return commit(ctx, op, {
      writes:
        result.state === "delivered"
          ? [
              {
                sql: "UPDATE roster_drive_exports SET state='delivered',last_error=NULL,last_success_at=?,updated_at=? WHERE school_id=? AND publication_id=? AND content_digest=?",
                args: [
                  ctx.now,
                  ctx.now,
                  ctx.principal.schoolId,
                  ctx.body.publicationId,
                  result.contentDigest,
                ],
              },
            ]
          : [],
      result,
      audit: { publicationId: ctx.body.publicationId, state: result.state },
    });
  }
  if (ctx.path.startsWith("/offline/")) return offlineEdits(ctx);
  if (["/duties", "/duties/export"].includes(ctx.path)) {
    const report = await dutiesReport(
      ctx.env.DB,
      ctx.principal.schoolId,
      ctx.body,
      ctx.principal,
      ctx.now,
    );
    return ctx.path.endsWith("/export")
      ? new Response(ledgerCSV(report), {
          headers: {
            "content-type": "text/csv; charset=utf-8",
            "content-disposition": 'attachment; filename="duties.csv"',
            "cache-control": "no-store",
          },
        })
      : report;
  }
  if (ctx.path === "/duties/correct") {
    const op = await prepared(ctx, "roster.correct", ctx.body);
    if (op.receipt) return { ...op.receipt.result, receipt: op.receipt };
    const plan = await correctionPlan(
      ctx.env.DB,
      ctx.principal.schoolId,
      ctx.body,
      ctx.principal,
      ctx.now,
    );
    let dependencies = [];
    if (ctx.body.staffId) {
      const contacts = await serviceCall(
        ctx.env,
        "contacts",
        "/internal/projection",
        {
          schoolId: ctx.principal.schoolId,
          decisionId: ctx.principal.decisionId,
          purpose: "roster",
          staffIds: [ctx.body.staffId],
        },
      );
      requireValue(
        (contacts.people || contacts.staff).some(
          (p) => (p.staffId || p.id) === ctx.body.staffId,
        ),
        "STAFF_NOT_FOUND",
        404,
      );
      dependencies = contacts.dependencies || [];
    }
    return commit(ctx, op, {
      ...plan,
      dependencies,
      audit: { dutyId: ctx.body.dutyId, reason: ctx.body.reason },
    });
  }
  if (ctx.path.startsWith("/whatsapp/reports/"))
    return handleWhatsappReportRoute(ctx);
  if (ctx.path.startsWith("/whatsapp/media/"))
    return handleWhatsappMediaRoute(ctx);
  if (ctx.path === "/backup-ledger")
    return handleBackupLedger(ctx, { prepared, commit });
  if (ctx.path === "/edge/archive")
    return handleEdgeArchive(ctx, { recordContext });
  if (ctx.path === "/layers") return layerSettings(ctx);
  if (["/upload", "/update", "/remove"].includes(ctx.path))
    return baselineWrite(ctx);
  if (ctx.path === "/lease") return lease(ctx);
  if (ctx.path === "/operations" && leaseOperations.has(ctx.body.operation))
    return lease(ctx, leaseOperations.get(ctx.body.operation));
  return operations(ctx);
}
async function layerSettings(ctx) {
  const schoolId = ctx.principal.schoolId,
    date = dateOnly(ctx.body.date || localDate(ctx.now)),
    row = await one(
      ctx.env.DB,
      "SELECT payload_json,revision FROM roster_records WHERE school_id=? AND collection='layerPolicy' AND record_id='scalar'",
      [schoolId],
    ),
    loaded = await loadState(ctx.env.DB, schoolId, { from: date, to: date }),
    stored = row ? JSON.parse(row.payload_json).data : defaultPolicy();
  const value = {
    ...stored,
    noReliefSubjects: loaded.state.systemConfig.noReliefSubjects,
    consecutiveBreakSubjects:
      loaded.state.systemConfig.consecutiveCountExclusions,
    sharedCoverSubjects: loaded.state.systemConfig.sharedSubjectCoverEnabled
      ? loaded.state.systemConfig.sharedSubjectCoverSubjects
      : [],
    employmentRestrictedPeriods: Object.fromEntries(
      loaded.state.systemConfig.employmentPeriodRestrictions.map((x) => [
        x.employment,
        x.periods,
      ]),
    ),
  };
  if (ctx.method === "GET") {
    const refs = await serviceCall(
        ctx.env,
        "contacts",
        "/internal/projection",
        {
          schoolId,
          decisionId: ctx.principal.decisionId,
          purpose: "roster",
          date,
        },
      ),
      entry = Roster.active(await baselines(ctx.env, schoolId), date),
      baselineIds = new Set(Object.values(entry?.mapping || {}));
    const resolvedTeachers = (refs.people || refs.staff).map((p) => {
      const id = p.staffId || p.id;
      return {
        staffId: id,
        name: p.name,
        layerId: resolveLayer(
          {
            ...p,
            staffId: id,
            appointments: p.appointments || [p.role].filter(Boolean),
            dailyAdhoc: loaded.state.adhoc.some(
              (a) => a.date === date && a.staffId === id && !a.deletedAt,
            ),
            hasBaseline: baselineIds.has(id),
          },
          value,
        ),
      };
    });
    return {
      revision: row?.revision || 0,
      policy: value,
      globalsReadOnly: true,
      resolvedTeachers,
      date,
    };
  }
  const policyValue = validatePolicy({
      ...ctx.body.policy,
      noReliefSubjects: value.noReliefSubjects,
      consecutiveBreakSubjects: value.consecutiveBreakSubjects,
      sharedCoverSubjects: value.sharedCoverSubjects,
      employmentRestrictedPeriods: value.employmentRestrictedPeriods,
    }),
    revision = row?.revision || 0;
  requireValue(ctx.body.revision === revision, "REVISION_CONFLICT", 409);
  const op = await prepared(ctx, "roster.layers", ctx.body);
  if (op.receipt) return { ...op.receipt.result, receipt: op.receipt };
  const storedNext = {
    schemaVersion: policyValue.schemaVersion,
    version: policyValue.version,
    layers: policyValue.layers,
    maxComparisons: policyValue.maxComparisons,
  };
  return commit(ctx, op, {
    guards: [
      {
        sql: "SELECT COALESCE((SELECT revision FROM roster_records WHERE school_id=? AND collection='layerPolicy' AND record_id='scalar'),0)=? AS ok",
        args: [schoolId, revision],
      },
    ],
    writes: [
      {
        sql: "INSERT INTO roster_records(school_id,collection,record_id,revision,payload_json) VALUES(?,'layerPolicy','scalar',?,?) ON CONFLICT(school_id,collection,record_id) DO UPDATE SET revision=excluded.revision,payload_json=excluded.payload_json",
        args: [
          schoolId,
          revision + 1,
          canonical({ kind: "scalar", data: storedNext }),
        ],
      },
    ],
    result: {
      revision: revision + 1,
      policy: policyValue,
      globalsReadOnly: true,
    },
    audit: { before: stored, after: storedNext },
  });
}
function publicBaseline(entry) {
  if (!entry) return null;
  const { id, name, from, to, priority, revision, pruned } = entry;
  const unmappedVenueLabels = Object.entries(entry.venueMapping || {})
    .filter(([, value]) => Array.isArray(value) && value.length === 0).map(([label]) => label);
  return { id, name, from, to, priority, revision, pruned,
    unmappedVenueLabels, occupancyComplete: unmappedVenueLabels.length === 0 };
}
async function baselineRead(ctx) {
  const schoolId = ctx.principal.schoolId,
    date = dateOnly(ctx.body.date || localDate(ctx.now)),
    entries = await baselines(ctx.env, schoolId),
    selected = Roster.currentOrNext(entries, date),
    manager = ctx.principal.capabilities.some((c) =>
      ["admin", "roster.read", "roster.write"].includes(c),
    ),
    entry = ctx.body.id
      ? entries.find((e) => e.id === ctx.body.id)
      : selected.entry;
  requireValue(
    !ctx.body.id || manager || entry?.id === selected.entry?.id,
    "CAPABILITY_DENIED",
    403,
  );
  const revision = await scopeRevision(ctx.env.DB, schoolId, "baselines"),
    base = {
      revision,
      active:
        ctx.body.id && manager
          ? publicBaseline(entry)
          : selected.selection === "current"
            ? publicBaseline(selected.entry)
            : null,
      selected: publicBaseline(entry),
      selection: ctx.body.id ? "selected" : selected.selection,
      upcoming:
        selected.selection === "upcoming" ? publicBaseline(entry) : null,
    };
  if (!entry) return { ...base, model: null };
  if (ctx.body.knownModelIds?.includes(entry.id))
    return { ...base, modelId: entry.id, modelUnchanged: true };
  return {
    ...base,
    modelId: entry.id,
    model: await readArtifact(ctx.env.ARTIFACTS, entry.modelKey, entry.digest),
  };
}

async function reference(ctx) {
  const schoolId = ctx.principal.schoolId,
    [contacts, venues] = await Promise.all([
      serviceCall(ctx.env, "contacts", "/internal/projection", {
        schoolId,
        decisionId: ctx.principal.decisionId,
        purpose: "roster",
      }),
      serviceCall(ctx.env, "venues", "/internal/catalogue", {
        schoolId,
        decisionId: ctx.principal.decisionId,
        purpose: "roster",
      }),
    ]);
  const loaded = await loadState(ctx.env.DB, schoolId, requestedDates({}, ctx.now));
  return {
    schoolId,
    staff: (contacts.people || contacts.staff).map((p) => ({
      staffId: p.staffId || p.id,
      name: p.name,
      active: p.active,
    })),
    venues: venues.venues,
    nameBridges: (loaded.state.nameBridges || []).map(({ timetableName, staffId }) => ({ timetableName, staffId })),
    contactsRevision: contacts.revision,
    revision: await scopeRevision(ctx.env.DB, schoolId, "baselines"),
  };
}
async function stageOccupancy(ctx, op, projection) {
  if ((projection.occupancy?.length || 0) <= 128) return projection;
  const chunks = [],
    rows = projection.occupancy;
  for (let index = 0; index * 256 < rows.length; index++) {
    const part = rows.slice(index * 256, (index + 1) * 256),
      artifact = await writeArtifact(
        ctx.env.ARTIFACTS,
        ctx.principal.schoolId,
        "occupancy",
        part,
      );
    await statement(
      ctx.env.DB,
      "INSERT OR IGNORE INTO roster_projection_chunks(school_id,operation_id,chunk_index,chunk_digest,artifact_key,row_count) VALUES(?,?,?,?,?,?)",
      [
        ctx.principal.schoolId,
        op.operationId,
        index,
        artifact.digest,
        artifact.key,
        part.length,
      ],
    ).run();
    chunks.push({ index, digest: artifact.digest, count: part.length });
  }
  const totalCount = rows.length,
    manifestDigest = await digest({
      schemaVersion: 1,
      sourceKind: projection.sourceKind,
      coverage: projection.coverage,
      chunks,
      totalCount,
    });
  delete projection.occupancy;
  projection.staged = { schemaVersion: 1, manifestDigest, chunks, totalCount };
  return projection;
}
async function holdOccupancy(ctx, op, projection) {
  const schoolId = ctx.principal.schoolId;
  await statement(
    ctx.env.DB,
    "INSERT OR IGNORE INTO roster_prepared_publications(school_id,operation_id,digest,fence,projection_json) VALUES(?,?,?,?,?)",
    [schoolId, op.operationId, op.digest, op.fence, canonical(projection)],
  ).run();
  const old = await one(
    ctx.env.DB,
    "SELECT projection_json FROM roster_prepared_publications WHERE school_id=? AND operation_id=?",
    [schoolId, op.operationId],
  );
  requireValue(
    old.projection_json === canonical(projection),
    "SOURCE_PROJECTION_CHANGED",
    409,
  );
  const scopes = projection.staged
      ? undefined
      : projection.occupancy.map(({ venueId, start, end }) => ({
          venueId,
          start,
          end,
        })),
    proof = await serviceCall(ctx.env, "venues", "/internal/source-begin", {
      schoolId,
      decisionId: ctx.principal.decisionId,
      sourceDomain: "roster",
      sourceOperationId: op.operationId,
      sourceDigest: op.digest,
      sourceFence: op.fence,
      scopes,
      projection,
    });
  requireValue(
    proof.state === "held" ||
      proof.state === "pending" ||
      proof.state === "committed",
    "VENUE_FENCE_UNCONFIRMED",
    409,
  );
  await statement(
    ctx.env.DB,
    "UPDATE roster_prepared_publications SET venue_proof_json=? WHERE school_id=? AND operation_id=? AND digest=? AND fence=?",
    [canonical(proof), schoolId, op.operationId, op.digest, op.fence],
  ).run();
  return {
    proof,
    guard: {
      sql: "SELECT digest=? AND fence=? AND json_extract(venue_proof_json,'$.sealed')=1 AS ok FROM roster_prepared_publications WHERE school_id=? AND operation_id=?",
      args: [op.digest, op.fence, schoolId, op.operationId],
    },
    outbox: {
      id: "venues:" + op.operationId,
      type: "roster-venues-complete",
      payload: {
        schoolId,
        decisionId: ctx.principal.decisionId,
        sourceDomain: "roster",
        sourceOperationId: op.operationId,
        sourceDigest: op.digest,
        sourceFence: op.fence,
      },
      dueAt: ctx.now,
    },
  };
}
function mappedVenues(venueMapping, label) {
  const value = venueMapping[label];
  requireValue(typeof value === "string" || Array.isArray(value), "VENUE_MAPPING_REQUIRED", 409);
  const ids = typeof value === "string" ? [value] : value;
  requireValue(ids.length <= 32 && ids.every(id => typeof id === "string") &&
    new Set(ids).size === ids.length, "VENUE_MAPPING_INVALID", 409);
  return ids;
}
async function occupancyRows(day, date, venueMapping, validIds, { allowUnmapped = false } = {}) {
  const result = new Map();
  for (const [teacherIndex, teacher] of (day.teachers || []).entries())
    for (let period = 0; period < (teacher.cells || []).length; period++) {
      const cell = teacher.cells[period],
        timing = day.periods.find((p) => p.period === period);
      if (!cell?.[2] || !cell.some((x) => String(x).trim()) || !timing)
        continue;
      const ids = mappedVenues(venueMapping, cell[2]);
      requireValue(
        (allowUnmapped || ids.length) && ids.every(id => validIds.has(id)),
        "VENUE_MAPPING_REQUIRED",
        409,
        { label: cell[2] },
      );
      for (const venueId of ids) {
        const start =
          Date.parse(date + "T00:00:00+08:00") + timing.startMinutes * 60000,
          end = Date.parse(date + "T00:00:00+08:00") + timing.endMinutes * 60000,
          lesson = {
            classOrTeachingGroup: String(cell[1] || "").trim(),
            group: String(cell[3] || "").trim(),
            subject: String(cell[0] || "").trim(),
          },
          identified = !!(lesson.classOrTeachingGroup || lesson.group),
          key = canonical({ venueId, start, end, lesson,
            // Without a target, equal subjects do not prove a shared lesson.
            ...(!identified ? { row: [teacher.staffId || null,
              teacher.sourceRow || teacher.timetableName || teacher.staffId || String(teacherIndex)] } : {}),
          });
        let row = result.get(key);
        if (!row) {
          row = { venueId, start, end, period, displayName: cell[1] || cell[3] || cell[0],
            lesson, demandEvidence: identified ? "identified" : "unresolved", sourceLabels: [], staffIds: [] };
          result.set(key, row);
        }
        if (!row.sourceLabels.includes(cell[2])) row.sourceLabels.push(cell[2]);
        if (teacher.staffId && !row.staffIds.includes(teacher.staffId)) row.staffIds.push(teacher.staffId);
      }
    }
  for (const [key, row] of result) {
    row.sourceId = "lesson:" + await digest(key);
    row.sourceLabels.sort();
    row.staffIds.sort();
  }
  return [...result.values()].sort(
    (a, b) =>
      a.venueId.localeCompare(b.venueId) || a.start - b.start || a.end - b.end ||
      a.sourceId.localeCompare(b.sourceId),
  );
}
async function baselineWrite(ctx) {
  const schoolId = ctx.principal.schoolId,
    op = await prepared(ctx, "roster.baseline", ctx.body);
  if (op.receipt) return { ...op.receipt.result, receipt: op.receipt };
  const id = ctx.body.id || "baseline:" + op.operationId,
    revision = await scopeRevision(ctx.env.DB, schoolId, "baselines"),
    entries = await baselines(ctx.env, schoolId),
    old = entries.find((e) => e.id === id);
  requireValue(ctx.body.revision === revision, "REVISION_CONFLICT", 409);
  const sc = scopeChange(schoolId, "baselines", revision),
    command = ctx.body.operation || ctx.body.action || "save";
  requireValue(
    !["remove", "prune", "restore", "activate"].includes(command) || old,
    "BASELINE_NOT_FOUND",
    404,
  );
  const metadata = Roster.metadata({
      name: ctx.body.name ?? old?.name,
      from: ctx.body.from ?? old?.from,
      to: ctx.body.to ?? old?.to,
    }),
    model = ctx.body.model
      ? Roster.validate(ctx.body.model)
      : old
        ? await readArtifact(ctx.env.ARTIFACTS, old.modelKey, old.digest)
        : null;
  requireValue(model, "TIMETABLE_REQUIRED");
  const mapping = ctx.body.staffMapping || old?.mapping || {},
    venueMapping = ctx.body.venueMapping || old?.venueMapping || {},
    weeks = ["single", "odd", "even"].filter((k) => model[k]),
    names = [
      ...new Set(weeks.flatMap((k) => model[k].blocks.map((b) => b.name))),
    ],
    labels = [
      ...new Set(
        weeks.flatMap((k) =>
          model[k].blocks.flatMap((b) =>
            b.cells.map((c) => c[2]).filter(Boolean),
          ),
        ),
      ),
    ];
  requireValue(
    names.every((n) => typeof mapping[n] === "string") &&
      new Set(names.map((n) => mapping[n])).size === names.length,
    "STAFF_MAPPING_REQUIRED",
    409,
  );
  const [contacts, catalogue] = await Promise.all([
      serviceCall(ctx.env, "contacts", "/internal/projection", {
        schoolId,
        decisionId: ctx.principal.decisionId,
        purpose: "roster",
        staffIds: names.map((n) => mapping[n]),
      }),
      serviceCall(ctx.env, "venues", "/internal/catalogue", {
        schoolId,
        decisionId: ctx.principal.decisionId,
        purpose: "roster",
      }),
    ]),
    ids = new Set(
      (contacts.people || contacts.staff).map((p) => p.staffId || p.id),
    ),
    venueIds = new Set(catalogue.venues.map((v) => v.venueId || v.id));
  requireValue(
    names.every((n) => ids.has(mapping[n])),
    "STAFF_MAPPING_INVALID",
    409,
  );
  requireValue(
    labels.every((label) => mappedVenues(venueMapping, label).every(id => venueIds.has(id))),
    "VENUE_MAPPING_REQUIRED",
    409,
  );
  const artifact = await writeArtifact(
      ctx.env.ARTIFACTS,
      schoolId,
      "baselines",
      model,
    ),
    pruned = ctx.path === "/remove" || ["prune", "remove"].includes(command),
    next = {
      id,
      ...metadata,
      revision: (old?.revision || 0) + 1,
      priority: op.createdAt,
      modelKey: artifact.key,
      digest: artifact.digest,
      mapping,
      venueMapping,
      pruned,
    },
    all = entries.filter((e) => e.id !== id).concat(next),
    active = all.filter((e) => !e.pruned && e.to >= localDate(ctx.now));
  const from = active.length
      ? active.map((e) => e.from).sort()[0]
      : metadata.from,
    to = active.length
      ? active
          .map((e) => e.to)
          .sort()
          .at(-1)
      : metadata.to;
  requireValue(
    Date.parse(to) - Date.parse(from) < 366 * 86400000,
    "BASELINE_RANGE_BOUNDS",
    413,
  );
  const cal = await serviceCall(ctx.env, "calendar", "/internal/dates", {
      schoolId,
      decisionId: ctx.principal.decisionId,
      from,
      to,
    }),
    calendar = new Map(cal.rows.map((r) => [r.date, r])),
    confirmed = await query(
      ctx.env.DB,
      "SELECT DISTINCT service_date FROM roster_publications WHERE school_id=? AND service_date BETWEEN ? AND ?",
      [schoolId, from, to],
    ),
    excluded = new Set(confirmed.map((r) => r.service_date)),
    models = new Map([[id, model]]);
  let periods = [],
    chunkRows = [],
    chunks = [],
    totalCount = 0;
  async function flushChunk() {
    if (!chunkRows.length) return;
    requireValue(chunks.length < 2000, "BASELINE_OCCUPANCY_BOUNDS", 413);
    const rows = chunkRows.splice(0, 256),
      artifact = await writeArtifact(
        ctx.env.ARTIFACTS,
        schoolId,
        "occupancy",
        rows,
      ),
      index = chunks.length;
    await statement(
      ctx.env.DB,
      "INSERT OR IGNORE INTO roster_projection_chunks(school_id,operation_id,chunk_index,chunk_digest,artifact_key,row_count) VALUES(?,?,?,?,?,?)",
      [
        schoolId,
        op.operationId,
        index,
        artifact.digest,
        artifact.key,
        rows.length,
      ],
    ).run();
    chunks.push({ index, digest: artifact.digest, count: rows.length });
    totalCount += rows.length;
  }
  for (let t = Date.parse(from); t <= Date.parse(to); t += 86400000) {
    const date = new Date(t).toISOString().slice(0, 10),
      row = calendar.get(date),
      entry = Roster.active(all, date);
    if (excluded.has(date) || !entry) continue;
    requireValue(row?.available === true, "CALENDAR_UNAVAILABLE", 503);
    if (!row.schoolDay) continue;
    let value = models.get(entry.id);
    if (!value) {
      value = await readArtifact(
        ctx.env.ARTIFACTS,
        entry.modelKey,
        entry.digest,
      );
      models.set(entry.id, value);
    }
    const week =
      value[value.layoutMode === "single-week" ? "single" : row.parity];
    requireValue(week, "TIMETABLE_PARITY_UNAVAILABLE", 409);
    const weekday = (new Date(t).getUTCDay() + 6) % 7;
    if (weekday >= 5) continue;
    const timing = Array.from({ length: value.maxPeriod + 1 }, (_, period) => ({
      period,
      startMinutes:
        value.timing.startMinutes + period * value.timing.periodMinutes,
      endMinutes:
        value.timing.startMinutes + (period + 1) * value.timing.periodMinutes,
    }));
    const declared = timing.map((p) => ({
      id: String(p.period),
      start: p.startMinutes,
      end: p.endMinutes,
    }));
    requireValue(
      !periods.length || canonical(periods) === canonical(declared),
      "BASELINE_TIMING_CONFLICT",
      409,
    );
    periods = declared;
    chunkRows.push(
      ...await occupancyRows(
        {
          periods: timing,
          teachers: week.blocks.map((b) => ({
            staffId: entry.mapping[b.name],
            sourceRow: b.name,
            cells: b.cells.slice(
              weekday * week.perDay,
              (weekday + 1) * week.perDay,
            ),
          })),
        },
        date,
        entry.venueMapping,
        venueIds,
        { allowUnmapped: true },
      ),
    );
    while (chunkRows.length >= 256) await flushChunk();
  }
  if (!periods.length)
    periods = Array.from({ length: model.maxPeriod + 1 }, (_, period) => ({
      id: String(period),
      start: model.timing.startMinutes + period * model.timing.periodMinutes,
      end:
        model.timing.startMinutes + (period + 1) * model.timing.periodMinutes,
    }));
  while (chunkRows.length) await flushChunk();
  const coverage = { from, to, venueIds: [...venueIds], periods },
    manifestDigest = await digest({
      schemaVersion: 1,
      sourceKind: "baseline",
      coverage,
      chunks,
      totalCount,
    });
  const projection = {
      schemaVersion: 1,
      schoolId,
      sourceDomain: "roster",
      sourceKind: "baseline",
      sourceOperationId: op.operationId,
      sourceDigest: op.digest,
      sourceFence: op.fence,
      revision: revision + 1,
      sequence: revision + 1,
      excludedDates: [...excluded],
      coverage,
      staged: { schemaVersion: 1, manifestDigest, chunks, totalCount },
    },
    held = await holdOccupancy(ctx, op, projection);
  if (held.proof.sealed !== true)
    return {
      state: "pending",
      phase: "venue-validation",
      operationId: op.operationId,
      validatedChunks: held.proof.validatedChunks || 0,
      totalChunks: chunks.length,
    };
  return commit(ctx, op, {
    dependencies: [
      ...(contacts.dependencies || []),
      ...(cal.dependencies || []),
    ],
    guards: [sc.guard, held.guard],
    writes: [
      sc.write,
      {
        sql: "INSERT INTO roster_baselines(school_id,baseline_id,revision,name,from_date,to_date,priority,model_key,digest,mapping_json,venue_mapping_json,pruned) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(school_id,baseline_id) DO UPDATE SET revision=excluded.revision,name=excluded.name,from_date=excluded.from_date,to_date=excluded.to_date,priority=excluded.priority,model_key=excluded.model_key,digest=excluded.digest,mapping_json=excluded.mapping_json,venue_mapping_json=excluded.venue_mapping_json,pruned=excluded.pruned",
        args: [
          schoolId,
          id,
          next.revision,
          metadata.name,
          metadata.from,
          metadata.to,
          op.createdAt,
          artifact.key,
          artifact.digest,
          canonical(mapping),
          canonical(venueMapping),
          pruned ? 1 : 0,
        ],
      },
    ],
    result: {
      id,
      savedId: id,
      revision: revision + 1,
      ...metadata,
      pruned,
      entries: all.map(publicBaseline),
      unmappedVenueLabels: labels.filter(label => !mappedVenues(venueMapping, label).length),
      occupancyComplete: labels.every(label => mappedVenues(venueMapping, label).length > 0),
      confirmedDailyPlansPreserved: true,
    },
    audit: { baselineId: id, sourceDigest: artifact.digest },
    outbox: [
      held.outbox,
      {
        id: "layout-timed:" + op.operationId,
        type: "roster-layout-timed",
        payload: {
          schoolId,
          decisionId: ctx.principal.decisionId,
          sourceDomain: "roster",
          sourceOperationId: op.operationId,
          sourceDigest: op.digest,
          year: Number(from.slice(0, 4)),
          from,
          to,
          sequence: revision + 1,
        },
        dueAt: ctx.now,
      },
    ],
  });
}
async function offlineEdits(ctx) {
  const schoolId = ctx.principal.schoolId,
    { row, value } = await daySource(
      ctx.env,
      schoolId,
      ctx.body.basePublicationId,
    ),
    date = value.date,
    range = { from: date, to: date, dates: [date] },
    revision = await scopeRevision(ctx.env.DB, schoolId, "day:" + date),
    loaded = await loadState(ctx.env.DB, schoolId, range),
    c = await context(ctx, loaded, range),
    latest = await one(
      ctx.env.DB,
      "SELECT publication_id FROM roster_publications WHERE school_id=? AND service_date=? ORDER BY sequence DESC LIMIT 1",
      [schoolId, date],
    );
  requireValue(
    Operations.staplerPermission(loaded.state, c, date),
    "CAPABILITY_DENIED",
    403,
  );
  requireValue(
    latest?.publication_id === value.publicationId,
    "OFFLINE_BASE_CHANGED",
    409,
  );
  requireValue(
    Array.isArray(ctx.body.commands) &&
      ctx.body.commands.length > 0 &&
      ctx.body.commands.length <= 500,
    "OFFLINE_COMMAND_BOUNDS",
    413,
  );
  const record = loaded.state.staplerPlans.find((p) => p.date === date),
    publishedRevision = value.day.staplerRevision || 0;
  requireValue(
    !record || record.revision === publishedRevision,
    "OFFLINE_DRAFT_CHANGED",
    409,
  );
  let plan = Operations.staplerView(loaded.state, c, { date }).plan;
  requireValue(plan, "OFFLINE_SOURCE_UNAVAILABLE", 409);
  if (!record) {
    const frozen = Stapler.create(value.day, {
      date,
      venues: c.raw.venues[date] || [],
    });
    requireValue(
      canonical(plan.baseline) === canonical(frozen.baseline),
      "OFFLINE_SOURCE_LAYOUT_CHANGED",
      409,
    );
  }
  const before = plan;
  for (const command of ctx.body.commands) plan = Stapler.apply(plan, command);
  Stapler.validate(plan);
  const previewId = await digest({
      schoolId,
      actorId: ctx.principal.actorId,
      publicationId: value.publicationId,
      revision,
      commands: ctx.body.commands,
      plan,
    }),
    result = {
      date,
      basePublicationId: value.publicationId,
      revision,
      staplerRevision: record?.revision || 0,
      previewId,
      commands: ctx.body.commands,
      changes: Stapler.changes(plan),
      conflicts: [],
      before,
      plan,
    };
  if (ctx.path.endsWith("/preview")) return result;
  requireValue(
    ctx.body.previewId === previewId &&
      ctx.body.revision === revision &&
      ctx.body.confirm === true,
    "OFFLINE_PREVIEW_STALE",
    409,
  );
  return operations({
    ...ctx,
    path: "/operations",
    body: {
      operation: "saveStapler",
      operationId: ctx.body.operationId,
      requestId: ctx.body.operationId,
      date,
      revision,
      staplerRevision: record?.revision || 0,
      plan,
      fence: ctx.body.fence,
    },
  });
}
function leaseHolder(ctx) {
  const tab = ctx.body.leaseSessionId;
  return tab === undefined ? ctx.principal.actorId
    : ctx.principal.actorId + "/tab/" + identifier(tab, "leaseSessionId");
}
function lockView(ctx, date, row) {
  const active = !!row && row.expires_at > ctx.now;
  const mine = active && row.actor_id === leaseHolder(ctx);
  return { date, active, mine, fence: row?.fence || 0,
    expiresAt: row?.expires_at || 0, ownerName: mine ? "This planner" : active ? "Another planner" : "" };
}
async function lease(ctx, action = ctx.body.action) {
  const schoolId = ctx.principal.schoolId,
    date = dateOnly(ctx.body.date), holder = leaseHolder(ctx);
  requireValue(
    ["acquire", "renew", "release"].includes(action),
    "LEASE_ACTION_INVALID",
  );
  const op = await prepared(ctx, "roster.lease", ctx.body);
  if (op.receipt) return { ...op.receipt.result, receipt: op.receipt };
  const row = await one(
    ctx.env.DB,
    "SELECT * FROM roster_leases WHERE school_id=? AND service_date=?",
    [schoolId, date],
  );
  const revision = await scopeRevision(ctx.env.DB, schoolId, "day:" + date);
  const loaded = await loadState(ctx.env.DB, schoolId, {
      from: date,
      to: date,
    }),
    c = await context(ctx, loaded, { from: date, to: date, dates: [date] });
  requireValue(
    Operations.staplerPermission(loaded.state, c, date),
    "CAPABILITY_DENIED",
    403,
  );
  const planRevision = loaded.state.staplerPlans.find(p => p.date === date)?.revision || 0;
  requireValue(ctx.body.staplerRevision === undefined || ctx.body.staplerRevision === planRevision,
    "STAPLER_CONFLICT", 409);
  const owner = row?.actor_id === holder,
    available = !row || row.expires_at <= ctx.now;
  requireValue(
    action === "acquire"
      ? owner || available
      : owner && row.fence === (ctx.body.leaseFence ?? ctx.body.fence) && row.expires_at > ctx.now,
    "LEASE_CONFLICT",
    409,
  );
  const fence = action === "acquire" ? (row?.fence || 0) + 1 : row.fence,
    expiresAt =
      action === "release"
        ? ctx.now
        : Math.min(ctx.now + 120000, ctx.principal.validUntil);
  return commit(ctx, op, {
    dependencies: c.dependencies,
    guards: [
      ...(c.localGuards || []),
      { sql: "SELECT COALESCE((SELECT revision FROM roster_scopes WHERE school_id=? AND scope=?),0)=? AS ok",
        args: [schoolId, "day:" + date, revision] },
      {
        sql: row
          ? "SELECT actor_id=? AND fence=? AND expires_at=? AS ok FROM roster_leases WHERE school_id=? AND service_date=?"
          : "SELECT NOT EXISTS(SELECT 1 FROM roster_leases WHERE school_id=? AND service_date=?) AS ok",
        args: row ? [row.actor_id, row.fence, row.expires_at, schoolId, date] : [schoolId, date],
      },
      ...(action === "acquire" ? [] : [{ deadlineAt: row.expires_at }]),
    ],
    writes: [
      {
        sql: "INSERT INTO roster_leases(school_id,service_date,actor_id,fence,expires_at) VALUES(?,?,?,?,?) ON CONFLICT(school_id,service_date) DO UPDATE SET actor_id=excluded.actor_id,fence=excluded.fence,expires_at=excluded.expires_at",
        args: [schoolId, date, holder, fence, expiresAt],
      },
    ],
    result: { date, fence, expiresAt, holder: ctx.principal.actorId,
      lock: lockView(ctx, date, { actor_id: holder, fence, expires_at: expiresAt }) },
  });
}
async function captureObservedQuotas(ctx, loaded, next, c) {
  const oldIds = new Set(loaded.state.entitlements.map((r) => r.id));
  if (c.workloadEvidence?.quality === "estimated") next.entitlements = next.entitlements.filter(
    r => oldIds.has(r.id) || r.mode !== "observed");
  const
    added = next.entitlements.filter(
      (r) => r.mode === "observed" && !oldIds.has(r.id),
    ),
    guards = [],
    writes = [];
  for (const r of added) {
    const source = (c.publication?.rows || []).find(
      (x) => x.staffId === r.staffId,
    );
    requireValue(
      source?.quality === "verified" &&
        c.publication.publicationId &&
        Number.isSafeInteger(c.publication.publicationSequence),
      "WORKLOAD_UNVERIFIED",
      503,
    );
    guards.push({
      sql: "SELECT NOT EXISTS(SELECT 1 FROM roster_weekly WHERE school_id=? AND staff_id=? AND week=?) AS ok",
      args: [ctx.principal.schoolId, r.staffId, r.week],
    });
    writes.push({
      sql: "INSERT INTO roster_weekly(school_id,staff_id,week,publication_id,publication_sequence,calculation_version,quota,excluded,exclusion_reason,captured_at,evidence_json) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
      args: [
        ctx.principal.schoolId,
        r.staffId,
        r.week,
        c.publication.publicationId,
        c.publication.publicationSequence,
        c.publication.calculationVersion,
        r.quota,
        r.excluded ? 1 : 0,
        r.exclusion || "",
        ctx.now,
        canonical({
          source: "observed-owner-contract",
          row: source,
          applicability: r,
        }),
      ],
    });
  }
  const oldOpenings = new Map(
    (loaded.state.openingBalances || []).map((r) => [
      r.staffId + "|" + r.year,
      r,
    ]),
  );
  for (const r of next.openingBalances || []) {
    const old = oldOpenings.get(r.staffId + "|" + r.year);
    if (old && canonical(old) === canonical(r)) continue;
    const supplied = ctx.body.rows?.find(
      (x) => x.staffId === r.staffId,
    )?.completedPeriods;
    requireValue(
      supplied === undefined ||
        (Number.isSafeInteger(supplied) && supplied >= 0 && supplied <= 99999),
      "OPENING_PERIOD_COUNT_INVALID",
    );
    guards.push({
      sql: "SELECT COALESCE((SELECT revision FROM roster_openings WHERE school_id=? AND staff_id=? AND year=?),0)=? AS ok",
      args: [ctx.principal.schoolId, r.staffId, r.year, old?.version || 0],
    });
    writes.push({
      sql: "INSERT INTO roster_openings(school_id,staff_id,year,revision,cutoff,period_count,charge,source_digest,operation_id) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(school_id,staff_id,year) DO UPDATE SET revision=excluded.revision,cutoff=excluded.cutoff,period_count=excluded.period_count,charge=excluded.charge,source_digest=excluded.source_digest,operation_id=excluded.operation_id",
      args: [
        ctx.principal.schoolId,
        r.staffId,
        r.year,
        r.version,
        r.asOfDate,
        supplied ?? null,
        r.cumulativeDone,
        await digest({ source: "owner-entered-opening", row: r }),
        ctx.body.operationId || ctx.body.requestId,
      ],
    });
  }
  const remove = new Set(added.map((r) => r.id));
  next.entitlements = next.entitlements.filter((r) => !remove.has(r.id));
  return { guards, writes };
}
// Per-layer controls supersede these editors, but imported/manual paths still
// retain their saved values. Never erase them as a side effect of an active edit.
const retainedSettingsFields = [
  "allocationRuleEnabled",
  "allocationPreferences",
  "maxDailyPeriods",
  "maxConsecutivePeriods",
  "maxDailyReliefPeriods",
  "adhocReliefMaxPeriods",
  "consecutiveSubjectRules",
];
function settingsCompatibility(state, context, body) {
  const saved = state.systemConfig,
    input = { ...body };
  for (const key of retainedSettingsFields) {
    requireValue(
      !Object.hasOwn(body, key) ||
        canonical(body[key]) === canonical(saved[key]),
      "LAYER_SETTINGS_READ_ONLY",
      400,
      "Use Allocation layers to edit current limits and preferences. Retained settings cannot be changed here.",
    );
    input[key] = structuredClone(saved[key]);
  }
  // Match the shared domain's private subjectNormal exactly. Its exported norm
  // lowercases instead; that is not equivalent for every Unicode subject code.
  const subjectKey = (value) =>
      String(value || "")
        .normalize("NFKC")
        .trim()
        .replace(/\s+/g, " ")
        .toUpperCase(),
    known = new Set(
      [...(context.subjects || []), ...saved.sharedSubjectCoverSubjects].map(
        subjectKey,
      ),
    ),
    selected =
      body.sharedSubjectCoverSubjects ?? saved.sharedSubjectCoverSubjects;
  requireValue(
    Array.isArray(selected) &&
      selected.length <= 200 &&
      selected.every(
        (value) =>
          typeof value === "string" &&
          value.length <= 100 &&
          value.trim() &&
          known.has(subjectKey(value)),
      ),
    "ROSTER_RULE_REJECTED",
    400,
    "Choose shared lesson subjects from the baseline timetable.",
  );
  const retainedRules = input.consecutiveSubjectRules;
  input.consecutiveSubjectRules =
    Allocation.normalizeConsecutiveSubjectRules(retainedRules);
  return {
    input,
    retainedRules,
    // Only legacy validation sees these retained names. Do not turn them into
    // baseline cells, current subject evidence, dependencies or response data.
    context: {
      ...context,
      subjects: [
        ...new Set([
          ...(context.subjects || []),
          ...input.consecutiveSubjectRules.map((rule) => rule.subject),
        ]),
      ],
    },
  };
}
async function operations(ctx) {
  const schoolId = ctx.principal.schoolId,
    body = ctx.body,
    opName = operation(body, ctx.path);
  if (["resolveAbsenceIntake", "retryAbsenceIntake"].includes(opName))
    return handleAbsenceIntakeOperation(ctx, opName, {
      context,
      prepared,
      commit,
      captureObservedQuotas,
      safeLegacyError,
    });
  if (
    [
      "deliveryReview",
      "dismissDeliveryIssues",
      "cancelPendingNotifications",
    ].includes(opName)
  )
    return handleDeliveryReviewOperation(ctx, opName, { prepared, commit });
  if (opName === "archiveColdOutput")
    return handleArchiveColdOutput(ctx, { prepared, commit });
  if (["statistics", "statisticsRPercent"].includes(opName)) return statisticsReport(ctx, opName, recordContext);
  const readonly = reads.has(opName) || opName === "read",
    range = requestedDates(opName === "capacityForecast" ? {...body, from: body.date || localDate(ctx.now), to: Allocation.addDays(Allocation.monday(body.date || localDate(ctx.now)), 6)} : body, ctx.now),
    ids = [
      body.id,
      body.absenceId,
      ...(body.ids || []),
      ...(body.absenceIds || []),
    ].filter(Boolean),
    scope = (adminWrites.has(opName) || ["openingBalances", "systemConfig"].includes(opName) ||
      (opName === "read" && ["types", "systemConfig"].includes(body.view)))
      ? "config" : "day:" + range.from,
    revision = await scopeRevision(ctx.env.DB, schoolId, scope),
    configRevision = await scopeRevision(ctx.env.DB, schoolId, "config"),
    loaded = await loadState(ctx.env.DB, schoolId, range, { recordIds: ids });
  loaded.state.revision = revision;
  if (opName === "contactCreate")
    return createRosterContact(ctx, { revision, date: range.from });
  if (opName === "adHocUpdateMobile")
    return updateRosterContactMobile(ctx, { revision, date: range.from });
  if (opName === "receiptStatus") {
    const op = await operationStatus(
      ctx.env.DB,
      schoolId,
      body.operationId || body.requestId,
    );
    requireValue(
      !op ||
        op.actorId === ctx.principal.actorId ||
        ctx.principal.capabilities.includes("roster.read"),
      "CAPABILITY_DENIED",
      403,
    );
    return {
      accepted: op?.state === "committed",
      state: op?.state || "unknown",
      receipt: op?.receipt || null,
      requestId: body.operationId || body.requestId,
    };
  }
  if (opName === "cancelAllocationRun") return cancelRun(ctx);
  if (opName === "allocationVersion")
    return {
      revision,
      allocationRevision: loaded.state.dayRevisions[range.from] || 0,
      date: range.from,
      runs: await query(
        ctx.env.DB,
        "SELECT run_id,state,revision,progress_json FROM roster_runs WHERE school_id=? AND service_date=? ORDER BY updated_at DESC LIMIT 5",
        [schoolId, range.from],
      ),
    };
  if (opName === "statisticsCalendar") {
    const targetYear =
      Number(body.year) || Number(localDate(ctx.now).slice(0, 4));
    if (ctx.env.CALENDAR) {
      const cal = await serviceCall(
        ctx.env,
        "calendar",
        "/internal/year-structure",
        {
          schoolId,
          decisionId: ctx.principal.decisionId,
          year: targetYear,
        },
      );
      return { ...cal, revision };
    }
    const unavailable =
      "School term dates are unavailable; use an explicit date range.";
    return {
      year: targetYear,
      terms: [],
      semesters: [],
      basis: unavailable,
      semesterBasis: unavailable,
      revision,
    };
  }
  if (opName.startsWith("inbox")) return inbox(ctx, opName);
  if (["archiveItems", "archivedItem"].includes(opName)) {
    requireValue(
      ctx.principal.capabilities.includes("admin") ||
        ctx.principal.capabilities.includes("roster.admin"),
      "CAPABILITY_DENIED",
      403,
    );
    return {
      revision,
      items: loaded.state.staplerArchives || [],
      deletionEnabled: false,
    };
  }
  const c = recordOnly(opName, body)
    ? await recordContext(ctx, loaded, range)
    : await context(ctx, loaded, range, {
        needsQuota:
          (workloadOperations.has(opName) || opName === "read" &&
            ["allocation", "availability", "health", "accounting"].includes(body.view)) &&
          !(opName === "requestMutual" && body.proposalOnly),
      });
  if (opName === "capacityForecast") {
    requireValue(ctx.principal.capabilities.some(x => ["admin", "roster.write", "roster.allocate"].includes(x)), "CAPABILITY_DENIED", 403);
    requireValue(range.from >= localDate(ctx.now), "FORECAST_PAST_DATE", 400);
    const forecast = await forecastCapacity(ctx, loaded, c, range.dates, revision);
    requireValue(await scopeRevision(ctx.env.DB, schoolId, "config") === configRevision, "FORECAST_CHANGED", 409);
    for (const guard of c.localGuards || []) requireValue((await one(ctx.env.DB, guard.sql, guard.args))?.ok === 1, "FORECAST_CHANGED", 409);
    requireValue(await scopeRevision(ctx.env.DB, schoolId, scope) === revision, "FORECAST_CHANGED", 409);
    return {...forecast, revision, sourceDependencies: c.dependencies, workloadEvidence: c.workloadEvidence};
  }
  if (opName === "directory")
    return {
      people: c.people.map((p) => ({
        id: p.id,
        name: p.name,
        active: p.active,
      })),
      revision,
    };
  if (opName === "nextSchoolDay") {
    const dates = await serviceCall(ctx.env, "calendar", "/internal/dates", {
      schoolId,
      decisionId: ctx.principal.decisionId,
      from: range.from,
      to: new Date(Date.parse(range.from) + 31 * 86400000)
        .toISOString()
        .slice(0, 10),
    });
    return {
      date: dates.rows.find((d) => d.available && d.schoolDay)?.date || null,
      calendarByDate: Object.fromEntries(dates.rows.map((d) => [d.date, d])),
    };
  }
  if (readonly) {
    try {
      const result = Operations.read(loaded.state, c, { ...body, operation: opName });
      if (result.health) result.health = await layeredHealth(ctx, loaded, c, range.from, revision, result.health);
      workloadView(result, loaded.state, c, range.from, revision);
      return {...result, revision, calendarByDate: c.calendarByDate};
    } catch (e) {
      throw safeLegacyError(e);
    }
  }
  let absenceClash = null;
  if (opName === "saveAbsence") {
    const existingOpId = body.operationId || body.requestId;
    const priorOp = existingOpId
      ? await operationStatus(ctx.env.DB, schoolId, existingOpId)
      : null;
    if (priorOp?.state === "committed") {
      const replayed = await prepared(ctx, "roster." + opName, body);
      if (replayed.receipt)
        return { ...replayed.receipt.result, receipt: replayed.receipt };
    }
    requireValue(
      Number.isSafeInteger(body.revision) && body.revision === revision,
      "REVISION_CONFLICT",
      409,
    );
    absenceClash = await prepareSaveAbsenceClashCheck(
      ctx,
      loaded,
      c,
      body,
      safeLegacyError,
    );
    if (absenceClash instanceof Response) return absenceClash;
  }
  const op = await prepared(ctx, "roster." + opName, body);
  if (op.receipt) return { ...op.receipt.result, receipt: op.receipt };
  requireValue(
    Number.isSafeInteger(body.revision) && body.revision === revision,
    "REVISION_CONFLICT",
    409,
  );
  if (absenceClash?.mode === "queueIntake") {
    return queueAbsenceIntakeClash(
      ctx,
      loaded,
      c,
      range,
      scope,
      revision,
      op,
      absenceClash,
      { commit },
    );
  }
  if (["optimise", "autoAllocate"].includes(opName))
    return allocationRun(ctx, loaded, c, range, scope, revision, op);
  if (opName === "publish") assertPublicationWorkload(ctx, c);
  const beforeOutbox = new Set(loaded.state.outbox.map((j) => j.id)),
    next = structuredClone(loaded.state);
  let result;
  try {
    const compatible =
      opName === "saveSystemConfig"
        ? settingsCompatibility(next, c, body)
        : opName === "saveAbsence"
          ? { input: body, context: { ...c, resubmission: false } }
          : { input: body, context: c };
    result =
      absenceClash?.mode === "replace"
        ? applyConfirmedReplacementToState(
            next,
            c,
            body,
            absenceClash.conflicts,
            op.operationId,
            absenceClash.conflictToken,
            safeLegacyError,
          )
        : Operations.apply(next, compatible.context, {
            ...compatible.input,
            operation: opName,
            requestId: op.operationId,
          });
    if (opName === "saveSystemConfig") {
      const retainedRules = compatible.retainedRules;
      if (
        canonical(next.systemConfig.consecutiveSubjectRules) !==
        canonical(retainedRules)
      ) {
        // Legacy validation accepts equivalent annotated/cased labels. The
        // allocator normalizes them when used; retain their saved representation.
        const audit = next.audit.at(-1);
        requireValue(
          canonical(
            Allocation.normalizeConsecutiveSubjectRules(retainedRules),
          ) === canonical(next.systemConfig.consecutiveSubjectRules) &&
            audit?.operation === "saveSystemConfig" &&
            audit.config &&
            !loaded.state.audit.some((old) => old.id === audit.id),
          "STORED_SETTINGS_INVALID",
          503,
        );
        next.systemConfig.consecutiveSubjectRules =
          structuredClone(retainedRules);
        audit.config.consecutiveSubjectRules = structuredClone(retainedRules);
      }
      for (const key of retainedSettingsFields)
        requireValue(
          canonical(next.systemConfig[key]) ===
            canonical(
              key === "consecutiveSubjectRules"
                ? retainedRules
                : compatible.input[key],
            ),
          "STORED_SETTINGS_INVALID",
          503,
          "Retained settings could not be preserved. No settings were saved.",
        );
      // A saved view must not fabricate legacy migration links or persist read
      // initialization. Existing actual links remain untouched in the clone.
      const viewState = structuredClone(next);
      viewState.nameBridgesSeeded = true;
      result = { ...Operations.systemConfigView(viewState, c), ...result };
    } else if (opName === "saveTypes") {
      const view = Operations.read(structuredClone(next), c, {
        operation: "read",
        view: "types",
        date: range.from,
      });
      result = {
        date: view.date,
        types: view.types,
        periods: view.periods,
        ...result,
      };
    } else if (opName === "setDutySMC") {
      // The native duty editor consumes the accepted view immediately. Keep
      // that exact snapshot in the receipt rather than rereading after commit.
      const view = Operations.read(structuredClone(next), c, {
        operation: "dutyRoster",
        date: range.from,
      });
      result = { ...view, ...result };
    } else if (stapleWrites.has(opName)) {
      // The native planner consumes the accepted view, not just saved IDs.
      // Freeze it in the receipt so a replay cannot substitute a newer plan.
      const view = Operations.read(structuredClone(next), c, {
        operation: body.view === "staplerDates" ? "staplerDates" : "stapler",
        date: range.from,
      });
      result = { ...view, ...result };
    }
  } catch (e) {
    throw safeLegacyError(e);
  }
  await updateCapacityWarnings(ctx, loaded, next, c, range.dates, revision);
  if (c.workloadEvidence) for (const row of next.assignments)
    if (!loaded.state.assignments.some(old => old.id === row.id)) row.workloadEvidence = c.workloadEvidence;
  const quotaPlan = await captureObservedQuotas(ctx, loaded, next, c),
    change = await recordChanges(schoolId, loaded.before, next, loaded.records,
      opName === "saveOpeningBalances" ? { maxWrites: 6000 }
        : ["saveSystemConfig", "saveTypes"].includes(opName)
          ? { administrativeSave: opName } : undefined),
    sc = scopeChange(schoolId, scope, revision);
  accountingChanges(schoolId, loaded, change, {
    forceWeeks: quotaPlan.writes.length ? [weekOf(range.from)] : [],
  });
  change.guards.push(
    scopeChange(schoolId, "config", configRevision).guard,
    ...quotaPlan.guards,
  );
  change.writes.push(...quotaPlan.writes);
  change.guards.push(sc.guard);
  change.writes.push(sc.write);
  if (
    stapleWrites.has(opName) &&
    ![
      "requestStapler",
      "decideStapler",
      "createStapler",
      "importStapler",
    ].includes(opName)
  ) {
    const held = await one(ctx.env.DB,
      "SELECT actor_id,fence,expires_at FROM roster_leases WHERE school_id=? AND service_date=?",
      [schoolId, range.from]);
    requireValue(held?.actor_id === leaseHolder(ctx) && held.fence === (body.leaseFence ?? body.fence) &&
      held.expires_at > ctx.now, "LEASE_CONFLICT", 409);
    change.guards.push({
      sql: "SELECT actor_id=? AND fence=? AND expires_at>? AS ok FROM roster_leases WHERE school_id=? AND service_date=?",
      args: [leaseHolder(ctx), body.leaseFence ?? body.fence, ctx.now, schoolId, range.from],
    }, { deadlineAt: held.expires_at });
  }
  for (const assignment of next.assignments.filter(
    (a) =>
      a.status === "draft" &&
      !loaded.state.assignments.some((old) => old.id === a.id),
  )) {
    const timing = c.days[assignment.date]?.periods.find(
      (p) => p.period === assignment.period,
    );
    if (timing)
      change.guards.push({
        deadlineAt:
          Date.parse(assignment.date + "T00:00:00+08:00") +
          timing.startMinutes * 60000,
      });
  }
  let projection;
  if (["publish", "publishStapler", "confirmStapler"].includes(opName)) {
    projection = await preparePublication(
      ctx,
      op,
      next,
      c,
      range.from,
      revision + 1,
    );
    if (projection.pending) return projection.pending;
    change.writes.push(...projection.writes);
    change.guards.push(...projection.guards);
    result = { ...result, ...projection.result };
  }
  for (const j of next.outbox) {
    const old = loaded.state.outbox.find((o) => o.id === j.id);
    if (
      old &&
      old.status !== j.status &&
      ["cancelled", "queued"].includes(j.status)
    )
      change.writes.push({
        sql: "UPDATE product_outbox SET state=?,updated_at=? WHERE school_id=? AND job_id=? AND state IN ('queued','failed','cancelled')",
        args: [j.status, ctx.now, schoolId, j.id],
      });
  }
  const outbox = next.outbox
    .filter((j) => !beforeOutbox.has(j.id))
    .map((j) => ({
      id: j.id,
      type: "roster-notification",
      payload: { ...j, decisionId: ctx.principal.decisionId },
      dueAt: j.dueAt || ctx.now,
    }));
  if (projection) {
    outbox.push(...projection.outbox);
    if (next.systemConfig.driveDaily?.enabled)
      outbox.push({
        id: "drive:" + op.operationId,
        type: "roster-drive-export",
        payload: {
          publicationId: projection.result.publicationId,
          date: range.from,
          folderId: next.systemConfig.driveDaily.folderId,
        },
        dueAt: ctx.now,
      });
  }
  outbox.push(...emailJobs(loaded.state, next, ctx.now));
  const output = {
    ...result,
    ...(c.workloadEvidence ? { workloadEvidence: c.workloadEvidence,
      automaticPublicationAllowed: c.workloadEvidence.quality === "verified" } : {}),
    revision: revision + 1,
    acceptedRevision: revision + 1,
    requestId: op.operationId,
    notificationsQueued: outbox.filter((j) => j.type === "roster-notification")
      .length,
  };
  const plan = {
    dependencies: c.dependencies,
    guards: [...change.guards, ...(c.localGuards || [])],
    writes: change.writes,
    result: output,
    audit: {
      date: range.from,
      operation: opName,
      savedIds: result?.savedIds || [],
    },
    outbox,
  };
  return commit(ctx, op, opName === "saveOpeningBalances"
    ? compactOpeningSave(schoolId, plan)
    : ["saveSystemConfig", "saveTypes"].includes(opName)
      ? compactAdministrativeSave(schoolId, opName, plan) : plan);
}
function assertPublicationWorkload(ctx, c) {
  requireValue(c.workloadEvidence?.quality === "verified" ||
    !(ctx.body.automatic === true || ctx.body.scheduleKey || ctx.principal.serviceIdentity === "roster"),
    "WORKLOAD_UNVERIFIED_AUTOPUBLISH_BLOCKED", 409);
}
async function preparePublication(ctx, op, state, c, date, revision) {
  assertPublicationWorkload(ctx, c);
  const schoolId = ctx.principal.schoolId,
    day = Operations.effectiveTimetableDay(state, c, date) || c.days[date];
  requireValue(day && Array.isArray(day.periods), "TIMETABLE_UNAVAILABLE", 409);
  const last = await one(
      ctx.env.DB,
      "SELECT MAX(sequence) AS sequence FROM roster_publications WHERE school_id=? AND service_date=?",
      [schoolId, date],
    ),
    sequence = (last?.sequence || 0) + 1,
    publicationId = "day:" + op.operationId;
  const duties = [];
  for (const a of state.assignments.filter(
    (a) =>
      a.date === date &&
      ["published", "completed"].includes(a.status) &&
      !a.noRelief &&
      !a.remove,
  )) {
    const serviceDutyId =
      a.serviceDutyId ||
      "duty:" +
        (await digest({
          schoolId,
          date,
          absentId: a.absentId || a.absenceId || null,
          period: a.period,
          className: a.className || "",
          groupName: a.groupName || "",
          subject: a.subject || "",
        }));
    duties.push(
      dutySnapshot({ ...a, serviceDutyId }, date, day.periods, publicationId),
    );
  }
  requireValue(
    new Set(duties.map((d) => d.dutyId)).size === duties.length,
    "DUPLICATE_SERVICE_DUTY",
    409,
  );
  const catalogue = await serviceCall(
      ctx.env,
      "venues",
      "/internal/catalogue",
      { schoolId, decisionId: ctx.principal.decisionId, purpose: "roster" },
    ),
    venueIds = new Set(catalogue.venues.map((v) => v.venueId || v.id)),
    entry = Roster.active(c.entries, date),
    mapping = { ...(entry?.venueMapping || {}), ...(day.venueMapping || {}) };
  for (const id of venueIds) mapping[id] = id;
  const occupancy = await occupancyRows(day, date, mapping, venueIds),
    periods = day.periods.map((p) => ({
      id: String(p.period),
      start: p.startMinutes,
      end: p.endMinutes,
    }));
  const projection = {
    schemaVersion: 1,
    schoolId,
    sourceDomain: "roster",
    sourceKind: "day",
    sourceOperationId: op.operationId,
    sourceDigest: op.digest,
    sourceFence: op.fence,
    publicationId,
    revision,
    sequence,
    date,
    occupancy,
    baselineRevision: await scopeRevision(ctx.env.DB, schoolId, "baselines"),
    coverage: { from: date, to: date, venueIds: [...venueIds], periods },
  };
  await stageOccupancy(ctx, op, projection);
  const held = await holdOccupancy(ctx, op, projection);
  if (held.proof.sealed !== true)
    return {
      pending: {
        state: "pending",
        phase: "venue-validation",
        operationId: op.operationId,
        validatedChunks: held.proof.validatedChunks || 0,
        totalChunks: held.proof.totalChunks || 0,
      },
    };
  const dutyGuards = [],
    dutyWrites = [];
  for (const d of duties) {
    const existing = await one(
      ctx.env.DB,
      "SELECT * FROM roster_duties WHERE school_id=? AND duty_id=?",
      [schoolId, d.dutyId],
    );
    if (existing?.completed) {
      const original = JSON.parse(existing.payload_json);
      requireValue(
        original.staffId === d.staffId && original.charge === d.charge,
        "DUTY_CORRECTION_REQUIRED",
        409,
      );
      continue;
    }
    dutyGuards.push({
      sql: "SELECT COALESCE((SELECT completed FROM roster_duties WHERE school_id=? AND duty_id=?),0)=0 AS ok",
      args: [schoolId, d.dutyId],
    });
    dutyWrites.push({
      sql: "INSERT INTO roster_duties(school_id,duty_id,service_date,end_at,publication_id,publication_sequence,payload_json,completed) VALUES(?,?,?,?,?,?,?,0) ON CONFLICT(school_id,duty_id) DO UPDATE SET end_at=excluded.end_at,publication_id=excluded.publication_id,publication_sequence=excluded.publication_sequence,payload_json=excluded.payload_json",
      args: [
        schoolId,
        d.dutyId,
        date,
        d.endAt,
        publicationId,
        sequence,
        canonical(d),
      ],
    });
  }
  const payload = {
    schemaVersion: 1,
    workloadEvidence: c.workloadEvidence || null,
    publicationId,
    date,
    sequence,
    day,
    duties,
    projection,
  };
  const artifact = await writeArtifact(
    ctx.env.ARTIFACTS,
    schoolId,
    "days",
    payload,
  );
  return {
    result: {
      publicationId,
      sequence,
      date,
      sourceDomain: "roster",
      sourceOperationId: op.operationId,
      sourceDigest: op.digest,
      sourceFence: op.fence,
      projection,
      publishedAt: new Date(ctx.now).toISOString(),
    },
    guards: [
      ...dutyGuards,
      held.guard,
      {
        sql: "SELECT COALESCE(MAX(sequence),0)=? AS ok FROM roster_publications WHERE school_id=? AND service_date=?",
        args: [sequence - 1, schoolId, date],
      },
      {
        sql: "SELECT digest=? AND fence=? AND venue_proof_json IS NOT NULL AS ok FROM roster_prepared_publications WHERE school_id=? AND operation_id=?",
        args: [op.digest, op.fence, schoolId, op.operationId],
      },
    ],
    writes: [
      ...dutyWrites,
      {
        sql: "INSERT INTO roster_publications(school_id,publication_id,service_date,sequence,operation_id,source_digest,payload_key,payload_json,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
        args: [
          schoolId,
          publicationId,
          date,
          sequence,
          op.operationId,
          artifact.digest,
          artifact.key,
          canonical(payload),
          ctx.now,
        ],
      },
    ],
    outbox: [
      held.outbox,
      {
        id: "burrows:" + op.operationId,
        type: "roster-burrows",
        payload: {
          schoolId,
          publicationId,
          date,
          payloadKey: artifact.key,
          digest: artifact.digest,
        },
        dueAt: ctx.now,
      },
    ],
  };
}
async function allocationRun(ctx, loaded, c, range, scope, revision, op) {
  const schoolId = ctx.principal.schoolId, date = range.from;
  const { snapshot, policyValue, policyRevision, weeklyWrites, weeklyGuards, day, demands, week } =
    await allocationSnapshot(ctx, loaded, c, date, revision, op.operationId);
  const run = await one(
    ctx.env.DB,
    "SELECT * FROM roster_runs WHERE school_id=? AND run_id=?",
    [schoolId, op.operationId],
  );
  requireValue(!run || run.state !== "cancelled", "ALLOCATION_CANCELLED", 409);
  if (run) {
    const saved = JSON.parse(run.snapshot_json),
      facts = (x) => {
        const { observedAt, inputDigest, ...rest } = x;
        return rest;
      };
    requireValue(
      canonical(facts(saved)) === canonical(facts(snapshot)),
      "ALLOCATION_INPUT_CHANGED",
      409,
    );
    Object.assign(snapshot, saved);
  }
  await statement(
    ctx.env.DB,
    "INSERT OR IGNORE INTO roster_runs(school_id,run_id,operation_id,service_date,state,fence,revision,snapshot_json,progress_json,updated_at) VALUES(?,?,?,?,'running',1,?,?,?,?)",
    [
      schoolId,
      op.operationId,
      op.operationId,
      date,
      revision,
      canonical(snapshot),
      canonical({ phase: "snapshot-ready", demands: demands.length }),
      ctx.now,
    ],
  ).run();
  const mode =
      ctx.body.mode === "fill-only" || ctx.body.allocationMode === "fill-only" || ctx.body.fillOnly === true
        ? "fill-only"
        : "auto",
    proposal = runEngine(snapshot, policyValue, { mode }, allocate),
    next = structuredClone(loaded.state),
    retained = new Set(proposal.retained.map((a) => a.id));
  for (const a of next.assignments)
    if (
      a.date === date &&
      a.origin === "automatic" &&
      a.status === "draft" &&
      !retained.has(a.id)
    ) {
      a.status = "cancelled";
      a.cancelledAt = new Date(ctx.now).toISOString();
      a.cancellationReason = "Replaced by validated allocation proposal.";
    }
  for (const a of proposal.assignments) {
    const d = demands.find((d) => d.id === a.demandId),
      timing = day.periods.find((p) => p.period === a.period);
    next.assignments.push({
      ...a,
      id: crypto.randomUUID(),
      date,
      timetableId: day.id,
      absenceId: d.absenceId,
      absentId: d.absentId || d.staffId,
      absentName: d.absentName || d.name || "",
      className: d.className || "",
      groupName: d.groupName || "",
      venue: d.venue || "",
      start: timing.start,
      end: timing.end,
      startMinutes: timing.startMinutes,
      endMinutes: timing.endMinutes,
      rCost: 1,
      version: 1,
      createdAt: new Date(ctx.now).toISOString(),
      createdBy: ctx.principal.actorId,
      instructions: Operations.assignmentInstructions(next, d, c),
      publicationEvidence: {
        publicationId: c.publication.publicationId ?? null,
        sequence: c.publication.publicationSequence ?? null,
        calculationVersion: c.publication.calculationVersion,
      },
      workloadEvidence: c.workloadEvidence || null,
    });
  }
  next.dayRevisions[date] = (loaded.state.dayRevisions[date] || 0) + 1;
  next.optimisationRuns.push({
    id: op.operationId,
    date,
    key: ctx.body.scheduleKey || op.operationId,
    at: new Date(ctx.now).toISOString(),
    status: "complete",
    engineVersion: proposal.engineVersion,
    unresolved: proposal.unresolved.length,
  });
  await updateCapacityWarnings(ctx,loaded,next,c,[date],revision);
  const change = await recordChanges(
      schoolId,
      loaded.before,
      next,
      loaded.records,
    ),
    sc = scopeChange(schoolId, scope, revision);
  accountingChanges(schoolId, loaded, change, { forceWeeks: [week] });
  change.guards.push(
    sc.guard,
    ...weeklyGuards,
    {
      sql: "SELECT state='running' AND fence=1 AND revision=? AS ok FROM roster_runs WHERE school_id=? AND run_id=?",
      args: [revision, schoolId, op.operationId],
    },
    {
      sql: "SELECT COALESCE((SELECT revision FROM roster_records WHERE school_id=? AND collection='layerPolicy' AND record_id='scalar'),0)=? AS ok",
      args: [schoolId, policyRevision],
    },
  );
  for (const a of proposal.assignments) {
    const p = day.periods.find((p) => p.period === a.period);
    change.guards.push({
      deadlineAt: Date.parse(date + "T00:00:00+08:00") + p.startMinutes * 60000,
    });
  }
  change.writes.push(sc.write, ...weeklyWrites, {
    sql: "UPDATE roster_runs SET state='committed',proposal_json=?,progress_json=?,updated_at=? WHERE school_id=? AND run_id=? AND fence=1",
    args: [
      canonical(proposal),
      canonical({
        phase: "committed",
        assigned: proposal.assignments.length,
        unresolved: proposal.unresolved.length,
      }),
      ctx.now,
      schoolId,
      op.operationId,
    ],
  });
  const result = {
    ...Operations.read(next, c, { operation: "allocation", date, compact: ctx.body.compact }),
    revision: revision + 1,
    workloadEvidence: c.workloadEvidence || null,
    automaticPublicationAllowed: c.workloadEvidence?.quality === "verified",
    runId: op.operationId,
    assigned: proposal.assignments.length,
    unresolved: proposal.unresolved,
    search: proposal.search,
    engineVersion: proposal.engineVersion,
    allocationRevision: revision + 1,
  };
  result.health = await layeredHealth(ctx, { ...loaded, state: next }, c, date, revision + 1, result.health);
  workloadView(result, next, c, date, revision + 1);
  return commit(ctx, op, {
    dependencies: c.dependencies,
    guards: [...change.guards, ...(c.localGuards || [])],
    writes: change.writes,
    // Legacy read projections have optional undefined properties. Freeze the
    // same JSON representation the browser receives into the durable receipt.
    result: JSON.parse(JSON.stringify(result)),
    outbox: next.outbox.filter(j=>!loaded.state.outbox.some(old=>old.id===j.id)).map(j=>({id:j.id,type:'roster-notification',payload:j,dueAt:j.dueAt||ctx.now})),
    audit: {
      runId: op.operationId,
      inputDigest: snapshot.inputDigest,
      policyVersion: policyValue.version,
      publicationId: c.publication.publicationId ?? null,
    },
  });
}
// The retained UI exports its complete current snapshot locally. Return that
// projection with the same revision and evidence as the allocation view/receipt.
// Correct the legacy numeric-R flag only at this display boundary: its pure
// eligibility calculator historically calls any finite R "verified".
function workloadView(result, state, c, date, revision) {
  if (result.allocationModel && result.timetable?.id) {
    result.exportContext = { ...Operations.dailyExport(state, c, { date, fullTeachers: true }), revision };
  }
  if (!c.workloadEvidence) return;
  result.workloadEvidence = c.workloadEvidence;
  result.automaticPublicationAllowed = c.workloadEvidence.quality === "verified";
  if (result.exportContext) {
    result.exportContext.workloadEvidence = c.workloadEvidence;
    if (c.workloadEvidence.quality === "estimated") result.exportContext.legend =
      "Estimated workload. " + (result.exportContext.legend || "");
  }
  if (c.workloadEvidence.quality === "estimated") {
    for (const teacher of result.allocationModel?.teachers || []) {
      teacher.balance.quotaVerified = false;
      teacher.balance.workloadQuality = "estimated";
    }
  }
}
async function cancelRun(ctx) {
  const runId = identifier(ctx.body.runId || ctx.body.id),
    op = await prepared(ctx, "roster.cancel-run", ctx.body);
  if (op.receipt) return { ...op.receipt.result, receipt: op.receipt };
  const run = await one(
    ctx.env.DB,
    "SELECT * FROM roster_runs WHERE school_id=? AND run_id=?",
    [ctx.principal.schoolId, runId],
  );
  requireValue(
    run && ["queued", "running", "yielded"].includes(run.state),
    "RUN_NOT_CANCELLABLE",
    409,
  );
  return commit(ctx, op, {
    guards: [
      {
        sql: "SELECT fence=? AND state IN ('queued','running','yielded') AS ok FROM roster_runs WHERE school_id=? AND run_id=?",
        args: [run.fence, ctx.principal.schoolId, runId],
      },
    ],
    writes: [
      {
        sql: "UPDATE roster_runs SET state='cancelled',fence=fence+1,updated_at=? WHERE school_id=? AND run_id=?",
        args: [ctx.now, ctx.principal.schoolId, runId],
      },
    ],
    result: { runId, state: "cancelled" },
  });
}
async function inbox(ctx, opName) {
  requireValue(
    ctx.principal.capabilities.includes("admin") ||
      ctx.principal.capabilities.includes("roster.write"),
    "CAPABILITY_DENIED",
    403,
  );
  const schoolId = ctx.principal.schoolId;
  if (opName === "inboxTemplates")
    return { templates: JSON.parse(ctx.env.WHATSAPP_TEMPLATES_JSON || "[]") };
  if (opName === "inboxList" || opName === "inboxThread") {
    const rows = await query(
      ctx.env.DB,
      "SELECT * FROM roster_inbox WHERE school_id=?" +
        (ctx.body.id ? " AND message_id=?" : "") +
        " ORDER BY received_at DESC LIMIT 100",
      ctx.body.id ? [schoolId, ctx.body.id] : [schoolId],
    );
    return {
      messages: rows.map((r) => ({
        id: r.message_id,
        staffId: r.staff_id,
        receivedAt: r.received_at,
        state: r.state,
        assignedTo: r.assigned_to,
        revision: r.revision,
        ...JSON.parse(r.payload_json),
      })),
    };
  }
  const row = await one(
    ctx.env.DB,
    "SELECT * FROM roster_inbox WHERE school_id=? AND message_id=?",
    [schoolId, ctx.body.id],
  );
  requireValue(row, "MESSAGE_NOT_FOUND", 404);
  requireValue(row.revision === ctx.body.revision, "REVISION_CONFLICT", 409);
  const op = await prepared(ctx, "roster." + opName, ctx.body);
  if (op.receipt) return { ...op.receipt.result, receipt: op.receipt };
  const outbox = [];
  if (opName === "inboxReply") {
    requireValue(
      typeof ctx.body.text === "string" &&
        ctx.body.text.trim() &&
        ctx.body.text.length <= 4000,
      "MESSAGE_INVALID",
    );
    outbox.push({
      id: "reply:" + op.operationId,
      type: "roster-inbox-reply",
      payload: {
        staffId: row.staff_id,
        text: ctx.body.text,
        decisionId: ctx.principal.decisionId,
        inReplyTo: row.message_id,
      },
      dueAt: ctx.now,
    });
  }
  return commit(ctx, op, {
    guards: [
      {
        sql: "SELECT revision=? AS ok FROM roster_inbox WHERE school_id=? AND message_id=?",
        args: [row.revision, schoolId, row.message_id],
      },
    ],
    writes: [
      {
        sql: "UPDATE roster_inbox SET revision=revision+1,state=?,assigned_to=? WHERE school_id=? AND message_id=?",
        args: [
          opName === "inboxSetHandled" ? "handled" : row.state,
          opName === "inboxAssign"
            ? ctx.body.assignedTo || ctx.principal.staffId
            : row.assigned_to,
          schoolId,
          row.message_id,
        ],
      },
    ],
    result: {
      id: row.message_id,
      revision: row.revision + 1,
      queued: outbox.length > 0,
    },
    outbox,
  });
}
export async function scheduled(ctx) {
  const now = ctx.now ?? Date.now(),
    env = ctx.env,
    schoolId = env.SCHOOL_ID,
    completion = await completePublishedDuties(env, schoolId, now),
    slot = Allocation.schedule(now),
    date = slot.date;
  const loaded = await loadState(env.DB, schoolId, { from: date, to: date });
  const enabled = loaded.state.systemConfig.scheduledAllocationEnabled === true;
  let allocationStatus = { state: enabled ? "outside-slot" : "disabled" };
  if (slot.optimise && enabled) {
    const key = slot.key || date + ":allocation:" + String(slot.minute),
      operationId = "schedule:" + key,
      previous = await one(
        env.DB,
        "SELECT outcome FROM roster_schedule WHERE school_id=? AND slot_key=?",
        [schoolId, key],
      );
    if (!previous) {
      try {
        const revision = await scopeRevision(env.DB, schoolId, "day:" + date),
          body = {
            operation: "autoAllocate",
            operationId,
            requestId: operationId,
            date,
            revision,
            mode: "auto",
            scheduleKey: key,
          },
          principal = await currentScheduledPrincipal(
            env,
            schoolId,
            "roster.allocate",
            operationId,
            body,
            date,
            now,
          ),
          result = await handle({
            env,
            principal,
            now,
            clock: ctx.clock,
            body,
            path: "/operations",
            method: "POST",
            request: new Request("https://gsis.internal/operations"),
          });
        allocationStatus = { state: "committed", result };
        await statement(
          env.DB,
          "INSERT OR IGNORE INTO roster_schedule(school_id,slot_key,service_date,outcome,operation_id,recorded_at) VALUES(?,?,?,?,?,?)",
          [schoolId, key, date, "committed", operationId, now],
        ).run();
      } catch (error) {
        allocationStatus = {
          state: "pending",
          code: error.code || "SCHEDULE_UNCONFIRMED",
        };
      }
    } else allocationStatus = { state: previous.outcome, replayed: true };
  }
  let communication;
  try {
    communication = await scheduledCommunication({ ...ctx, env, now }, slot);
  } catch (error) {
    communication = {
      state: "pending",
      code: error.code || "COMMUNICATION_UNCONFIRMED",
    };
  }
  // Named receipt RPCs are idempotent durable observations, so an interrupted
  // fanout can retry safely. This exception never retries an uncertain send.
  await statement(
    env.DB,
    "UPDATE product_outbox SET state='queued',last_code='RECEIPT_FANOUT_RECOVERY',updated_at=? WHERE school_id=? AND type='roster-provider-receipt' AND (state='uncertain' OR (state='leased' AND lease_until<=?))",
    [now, schoolId, now],
  ).run();
  const handlers = Object.fromEntries(
    [
      "roster-notification",
      "roster-inbox-reply",
      "roster-public-menu-reply",
      "roster-venues-complete",
      "roster-layout-timed",
      "roster-burrows",
      "roster-email",
      "roster-inbound",
      "roster-drive-export",
      "roster-provider-receipt",
    ].map((type) => [type, (job, e) => dispatchRosterJob(job, e, now)]),
  );
  const jobs = await drainOutbox(env.DB, env, {
    schoolId,
    now,
    limit: 8,
    handlers,
  });
  return {
    completion,
    allocation: allocationStatus,
    communication,
    jobs,
    scheduler: {
      date,
      minute: slot.minute,
      activeSender: env.SEND_OWNER === "v1",
    },
  };
}

function emailJobs(before, next, now) {
  const old = new Set((before.dailyEmploymentMail || []).map((x) => x.id));
  return (next.dailyEmploymentMail || [])
    .filter((j) => !old.has(j.id) && j.status === "queued")
    .map((j) => ({
      id: "email:" + j.id,
      type: "roster-email",
      payload: { date: j.date, mailId: j.id, to: j.to, subject: j.subject },
      dueAt: now,
    }));
}
async function scheduledCommunication(ctx, slot) {
  const { env, now } = ctx,
    schoolId = env.SCHOOL_ID,
    operationId = "communicate:" + slot.key,
    body = {
      operationId,
      date: slot.date,
      tick: Math.floor(now / 60000) * 60000,
    },
    existing = await operationStatus(env.DB, schoolId, operationId);
  if (existing?.receipt) return { state: "committed", replayed: true };
  const principal = await currentScheduledPrincipal(
      env,
      schoolId,
      "roster.notify",
      operationId,
      body,
      slot.date,
      now,
    ),
    range = requestedDates({ from: slot.today, to: Allocation.addDays(Allocation.monday(slot.date), 6) }, now),
    scope = "day:" + slot.date,
    revision = await scopeRevision(env.DB, schoolId, scope),
    loaded = await loadState(env.DB, schoolId, range),
    local = { ...ctx, principal, body },
    c = await context(local, loaded, range, { needsQuota: true }),
    next = structuredClone(loaded.state),
    result = Operations.scheduledTick(
      next,
      { ...c, service: true, skipScheduledAllocation: true },
      body.tick,
    );
  await updateCapacityWarnings(local, loaded, next, c, range.dates, revision);
  await refreshNewSummaries(local, loaded, next, c, revision);
  const op = await prepared(local, "roster.scheduled-notifications", body),
    quotaPlan = await captureObservedQuotas(local, loaded, next, c),
    change = await recordChanges(schoolId, loaded.before, next, loaded.records),
    sc = scopeChange(schoolId, scope, revision);
  accountingChanges(schoolId, loaded, change, {
    forceWeeks: quotaPlan.writes.length ? [weekOf(slot.date)] : [],
  });
  change.guards.push(sc.guard, ...quotaPlan.guards);
  change.writes.push(sc.write, ...quotaPlan.writes);
  const before = new Set(loaded.state.outbox.map((x) => x.id)),
    outbox = next.outbox
      .filter((j) => !before.has(j.id))
      .map((j) => ({
        id: j.id,
        type: "roster-notification",
        payload: j,
        dueAt: j.dueAt || now,
      }));
  outbox.push(...emailJobs(loaded.state, next, now));
  return {
    state: "committed",
    ...(await commit(local, op, {
      dependencies: c.dependencies,
      guards: [...change.guards, ...(c.localGuards || [])],
      writes: change.writes,
      result: { ...result, notificationsQueued: outbox.length },
      outbox,
      audit: { slot: slot.key, allocationSkipped: true },
    })),
  };
}
async function adoptPublication(ctx) {
  const { env, body, now } = ctx,
    schoolId = env.SCHOOL_ID,
    source = await serviceCall(env, "deploy", "/internal/publication/verify", {
      schoolId,
      publicationId: body.publicationId,
      sourceOperationId: body.sourceOperationId,
      sourceDigest: body.sourceDigest,
      sourceFence: body.sourceFence,
    });
  requireValue(
    source.schoolId === schoolId &&
      source.publicationId === body.publicationId &&
      Number.isSafeInteger(source.publicationSequence) &&
      Array.isArray(source.rows),
    "PUBLICATION_INVALID",
    503,
  );
  const op = await prepareOperation(env.DB, {
    schoolId,
    actorId: "deploy-delivery",
    operationId: identifier(body.operationId),
    action: "internal.roster.adopt",
    intent: {
      publicationId: body.publicationId,
      sourceOperationId: body.sourceOperationId,
      sourceDigest: body.sourceDigest,
      sourceFence: body.sourceFence,
    },
    now,
    validUntil: now + 60000,
  });
  if (op.receipt) return op.receipt;
  const existing = await one(
      env.DB,
      "SELECT * FROM roster_deploy_adoption WHERE school_id=?",
      [schoolId],
    ),
    sequence = existing?.sequence || 0,
    newer = source.publicationSequence > sequence;
  return commitOperation(env.DB, op, {
    now,
    admission: {
      kind: "system",
      reason:
        "Adopt verified immutable committed Deploy projection without granting user authority.",
    },
    guards: [
      {
        sql: "SELECT COALESCE((SELECT sequence FROM roster_deploy_adoption WHERE school_id=?),0)=? AS ok",
        args: [schoolId, sequence],
      },
    ],
    writes: newer
      ? [
          {
            sql: "INSERT INTO roster_deploy_adoption(school_id,publication_id,sequence,payload_json,adopted_at,operation_id) VALUES(?,?,?,?,?,?) ON CONFLICT(school_id) DO UPDATE SET publication_id=excluded.publication_id,sequence=excluded.sequence,payload_json=excluded.payload_json,adopted_at=excluded.adopted_at,operation_id=excluded.operation_id",
            args: [
              schoolId,
              source.publicationId,
              source.publicationSequence,
              canonical(source),
              now,
              op.operationId,
            ],
          },
        ]
      : [],
    result: {
      state: newer ? "adopted" : "ignored-stale",
      publicationId: source.publicationId,
      publicationSequence: source.publicationSequence,
      currentAdoptionSequence: Math.max(sequence, source.publicationSequence),
    },
    audit: {
      publicationId: source.publicationId,
      sourceOperationId: body.sourceOperationId,
    },
  });
}
async function currentScheduledPrincipal(
  env,
  schoolId,
  action,
  operationId,
  body,
  resourceId,
  now,
) {
  requireValue(
    env.CORE_SCHEDULER?.authorizeScheduled,
    "SCHEDULER_AUTHORITY_NOT_CONFIGURED",
    503,
  );
  return env.CORE_SCHEDULER.authorizeScheduled({
    schoolId,
    action,
    operationId,
    bodyDigest: await digest(body),
    resourceId,
    now,
  });
}
function templateFor(env, key, data) {
  const configured = JSON.parse(env.WHATSAPP_TEMPLATES_JSON || "{}"),
    t = Array.isArray(configured)
      ? configured.find((t) => t.key === key)
      : configured[key];
  requireValue(t && typeof t.name === "string", "TEMPLATE_NOT_CONFIGURED", 503);
  const parameters = (t.parameters || []).map((field) => ({
    type: "text",
    text: String(data[field] ?? "").slice(0, 1000),
  }));
  return {
    name: t.name,
    language: { code: t.language || "en" },
    ...(parameters.length
      ? { components: [{ type: "body", parameters }] }
      : {}),
  };
}
async function assertJobLease(env, job, now) {
  requireValue(
    await one(
      env.DB,
      "SELECT 1 AS ok FROM product_outbox WHERE school_id=? AND job_id=? AND state='leased' AND fence=? AND lease_until>?",
      [job.school_id, job.job_id, job.fence, Math.max(now, Date.now())],
    ),
    "JOB_LEASE_LOST",
    409,
  );
}
// Use current owner records after notificationCurrent resolves superseded source
// references. A correction can withdraw a duty without granting a new period.
function notificationReliefPeriods(state, payload) {
  const data = payload.data || {}, recipientId = payload.recipientId || payload.staffId;
  let periods;
  if (payload.kind === "mutual_request") {
    const request = state.requests.find(r => r.id === payload.requestId && r.date === payload.date && r.recipientId === recipientId);
    if (!request?.lessons?.length) return null;
    periods = request.lessons.map(lesson => lesson.period);
  } else if (payload.kind === "mutual_override") {
    if (!data.peerRequestIds?.length) return null;
    const requests = data.peerRequestIds.map(id => state.requests.find(r => r.id === id && r.date === payload.date && r.staffId === recipientId && !r.retiredAt));
    if (requests.some(r => !r?.lessons?.length || r.lessons.some(l => !Number.isInteger(l.period)))) return null;
    return []; // An override reports a changed prior arrangement, not new relief.
  } else if (payload.kind === "stapler_timetable") {
    const publication = data.communicationSnapshot || state.dailyPublicationStates[payload.date];
    const row = publication?.date === payload.date
      ? publication.teachers?.find(r => r.staffId === recipientId)
      : state.staplerPlans.find(r => r.date === payload.date)?.timetableNotices?.[recipientId];
    if (!row?.rows?.length || row.rows.some(r => !["relief", "stapler", "restored", "absence"].includes(r.kind) || !Number.isInteger(r.period))) return null;
    periods = row.rows.filter(r => r.kind === "relief" || r.kind === "stapler").map(r => r.period);
  } else {
    // Nonpersonal instructions keep their historical snapshotIds; the shared
    // validator identifies the publication supplying the actual message here.
    const snapshotIds = payload.kind === "conditional_relief_instruction" && !data.personal
      ? [data.checkedPublicationId] : data.snapshotIds;
    if (!snapshotIds?.length) return null;
    const snapshots = snapshotIds.map(id => state.publicationSnapshots.find(v => v.id === id && v.date === payload.date && v.staffId === recipientId));
    if (snapshots.some(v => !v || !Number.isInteger(v.period))) return null;
    periods = snapshots.filter(v => v.kind !== "cancelled").map(v => v.period);
  }
  return periods.every(p => Number.isInteger(p) && p >= 0 && p < 40) ? [...new Set(periods)] : null;
}
export async function dispatchRosterJob(job, env, now = Date.now()) {
  const payload = job.payload,
    schoolId = job.school_id;
  if (job.type === "roster-provider-receipt") {
    const event = await one(
      env.DB,
      "SELECT e.* FROM roster_provider_events e JOIN product_operations o ON o.school_id=e.school_id AND o.operation_id='event:'||e.event_id WHERE e.school_id=? AND e.event_id=? AND e.event_kind='status' AND o.state='committed'",
      [schoolId, payload.eventId],
    );
    requireValue(
      event &&
        event.provider === "whatsapp" &&
        ["lord", "venues"].includes(payload.owner),
      "RECEIPT_SOURCE_INVALID",
      409,
    );
    const value = JSON.parse(event.payload_json),
      binding = env[payload.owner.toUpperCase() + "_RECEIPTS"];
    requireValue(binding?.record, "RECEIPT_BINDING_UNAVAILABLE", 503);
    await assertJobLease(env, job, now);
    try {
      const receipt = await binding.record({
        schoolId,
        provider: "whatsapp",
        providerId: event.provider_message_id,
        status: value.status,
        occurredAt: event.event_at,
        eventId: event.event_id,
      });
      requireValue(
        receipt && typeof receipt === "object",
        "RECEIPT_UNCONFIRMED",
        503,
      );
      return { state: "delivered", providerId: event.event_id };
    } catch (error) {
      return {
        state: "failed",
        code: error.code || "RECEIPT_FANOUT_UNAVAILABLE",
        retryable: true,
      };
    }
  }
  if (job.type === "roster-inbound")
    return processInbound(job, env, now, { context, prepared, commit });
  if (job.type === "roster-venues-complete") {
    const result = await serviceCall(
      env,
      "venues",
      "/internal/source-complete",
      payload,
    );
    return {
      state: result.state === "pending" ? "failed" : "delivered",
      providerId: payload.sourceOperationId,
      code: result.state === "pending" ? "SOURCE_RECOVERY_PENDING" : undefined,
      retryable: result.state === "pending",
    };
  }
  if (job.type === "roster-layout-timed") {
    try {
      const result = await serviceCall(
        env,
        "layout",
        "/internal/adopt-timed-source",
        payload,
      );
      return {
        state: ["adopted", "ignored"].includes(result?.state)
          ? "delivered"
          : "failed",
        providerId: payload.sourceOperationId,
        retryable: !["adopted", "ignored"].includes(result?.state),
      };
    } catch (error) {
      return {
        state: "failed",
        code: error.code || "LAYOUT_TIMED_ADOPTION_FAILED",
        retryable: true,
      };
    }
  }
  if (job.type === "roster-burrows") return burrowsPackage(env, job, now);
  if (job.type === "roster-drive-export") {
    const body = { jobId: job.job_id, operationId: "export:" + job.job_id },
      principal = await currentScheduledPrincipal(
        env,
        schoolId,
        "roster.export",
        body.operationId,
        body,
        payload.publicationId,
        now,
      ),
      loaded = await loadState(env.DB, schoolId, {
        from: payload.date,
        to: payload.date,
      });
    requireValue(
      principal &&
        loaded.state.systemConfig.driveDaily?.enabled &&
        loaded.state.systemConfig.driveDaily.folderId === payload.folderId,
      "EXPORT_DESTINATION_CHANGED",
      409,
    );
    return exportDrive(env, job, now);
  }
  if (job.type === "roster-email") {
    const body = { jobId: job.job_id, operationId: "dispatch:" + job.job_id },
      principal = await currentScheduledPrincipal(
        env,
        schoolId,
        "roster.notify",
        body.operationId,
        body,
        payload.date,
        now,
      ),
      refs = await serviceCall(env, "contacts", "/internal/projection", {
        schoolId,
        decisionId: principal.decisionId,
        purpose: "roster",
      }),
      loaded = await loadState(env.DB, schoolId, {
        from: payload.date,
        to: payload.date,
      }),
      settings = loaded.state.dailyEmploymentSettings,
      allowed = (settings.recipients || []).some(
        (r) =>
          r.emails.includes(payload.to) &&
          (refs.people || refs.staff).some(
            (p) =>
              (p.staffId || p.id) === r.staffId &&
              p.active === true &&
              [p.email, ...(p.emails || [])]
                .filter(Boolean)
                .map((e) => String(e).toLowerCase())
                .includes(payload.to.toLowerCase()),
          ),
      );
    if (!allowed)
      return { state: "cancelled", code: "RECIPIENT_NO_LONGER_ELIGIBLE" };
    const snapshot = loaded.state.dailyEmploymentSnapshots.find(
      (x) => x.date === payload.date,
    );
    requireValue(snapshot, "REPORT_SNAPSHOT_MISSING", 503);
    const content = Operations.employmentEmail(snapshot, env.PUBLIC_ORIGIN);
    await assertJobLease(env, job, now);
    return dispatchProvider(env, {
      provider: "email",
      to: payload.to,
      subject: payload.subject,
      text: content.body,
      idempotencyKey: job.job_id,
    });
  }
  if (job.type === "roster-public-menu-reply") {
    const inbound = await one(
      env.DB,
      "SELECT * FROM roster_inbox WHERE school_id=? AND message_id=?",
      [schoolId, payload.inReplyTo],
    );
    if (
      !inbound ||
      Operations.mobile(JSON.parse(inbound.payload_json).sender) !==
        Operations.mobile(payload.to) ||
      now - inbound.received_at >= 86400000
    )
      return { state: "cancelled", code: "INBOX_REPLY_WINDOW_CLOSED" };
    await assertJobLease(env, job, now);
    return dispatchProvider(env, {
      provider: "whatsapp",
      to: payload.to,
      text: payload.text,
      recentInboundAt: inbound.received_at,
      idempotencyKey: job.job_id,
    });
  }
  const action = "roster.notify",
    body = { jobId: job.job_id, operationId: "dispatch:" + job.job_id },
    principal = await currentScheduledPrincipal(
      env,
      schoolId,
      action,
      body.operationId,
      body,
      payload.date || payload.staffId || "notifications",
      now,
    ),
    refs = await serviceCall(env, "contacts", "/internal/projection", {
      schoolId,
      decisionId: principal.decisionId,
      purpose: "roster",
      staffIds: [payload.recipientId || payload.staffId],
    });
  const person = (refs.people || refs.staff || []).find(
    (p) => (p.staffId || p.id) === (payload.recipientId || payload.staffId),
  );
  const reliefNotification = job.type === "roster-notification" &&
    isReliefNotificationKind(payload.kind) && !payload.data?.audience;
  if (
    !person ||
    (person.active !== true && !(reliefNotification && (person.eo === true || person.adhocEligible === true))) ||
    person.notificationConsent === false
  )
    return { state: "cancelled", code: "RECIPIENT_NO_LONGER_ELIGIBLE" };
  if (job.type === "roster-notification") {
    const capacityMessage = payload.kind==='capacity_warning'||payload.kind==='smc_report'&&!!payload.data?.scheduledSummary;
    const range = requestedDates({from:payload.date,to:capacityMessage?Allocation.addDays(Allocation.monday(payload.date),6):payload.date},now),
      loaded = await loadState(env.DB, schoolId, range),
      ctx = {
        env,
        principal,
        now,
        body: { date: payload.date, operation: "dailyNotify" },
      },
      c = await context(ctx, loaded, range, {needsQuota:capacityMessage});
    const currentRecipient = c.people.find(
      (p) => p.id === (payload.recipientId || payload.staffId),
    );
    if (!currentRecipient || currentRecipient.notificationConsent === false ||
      (currentRecipient.active !== true && !reliefNotification))
      return { state: "cancelled", code: "RECIPIENT_NO_LONGER_ELIGIBLE" };
    let currentActivations = [];
    if (currentRecipient.active !== true) {
      // Contacts/calendar IO can outlast an activation edit. Read the owning D1
      // again before sending; this is not atomic with Contacts or the provider.
      const rows = await query(env.DB,
        "SELECT payload_json FROM roster_records WHERE school_id=? AND collection='adhoc' AND staff_id=? AND service_date=? LIMIT 10001",
        [schoolId, currentRecipient.staffId || currentRecipient.id, payload.date]);
      requireValue(rows.length <= 10000 && rows.reduce((n, row) => n + row.payload_json.length, 0) <= 8388608, "ROSTER_RECORD_BOUNDS", 413);
      currentActivations = rows.map(row => JSON.parse(row.payload_json).data);
      if (!hasDatedReliefActivation(currentRecipient, currentActivations, payload.date))
        return { state: "cancelled", code: "RECIPIENT_NO_LONGER_ELIGIBLE" };
    }
    if (
      (payload.data?.audience === "duty-smc" ||
        ["smc_report", "smc_alert", "lse_notice", "urgent_forward"].includes(
          payload.kind,
        )) &&
      !Operations.dutySMC(loaded.state, c, payload.date).includes(
        currentRecipient.id,
      )
    )
      return { state: "cancelled", code: "RECIPIENT_NO_LONGER_DUTY_SMC" };
    if (
      payload.kind === "stapler_request" &&
      !c.projection.smcIds.includes(currentRecipient.id)
    )
      return { state: "cancelled", code: "SPONSOR_NO_LONGER_SMC" };
    if (Operations.notificationCurrent(loaded.state, payload, now))
      return { state: "cancelled", code: "NOTIFICATION_OBSOLETE" };
    if (currentRecipient.active !== true) {
      const periods = notificationReliefPeriods(loaded.state, payload);
      if (periods === null || !periods.every(period => hasDatedReliefActivation(currentRecipient, currentActivations, payload.date, period)))
        return { state: "cancelled", code: "RECIPIENT_NO_LONGER_ELIGIBLE" };
    }
    if(capacityMessage&&!await refreshCapacityMessage(ctx,loaded,c,payload,await scopeRevision(env.DB,schoolId,'day:'+payload.date)))
      return {state:'cancelled',code:'CAPACITY_WARNING_OBSOLETE_OR_UNVERIFIED'};
    let link = null;
    if (
      payload.kind === "mutual_request" ||
      payload.data?.snapshotIds?.length ||
      payload.data?.dailyPublication ||
      payload.data?.communicationSnapshot
    ) {
      const issued = await issueLink(env, job, loaded.state, now);
      requireValue(
        /^https:\/\/[^\s/?#]+$/.test(env.PUBLIC_ORIGIN || ""),
        "PUBLIC_ORIGIN_NOT_CONFIGURED",
        503,
      );
      link = env.PUBLIC_ORIGIN + "/roster/external/#token=" + issued.token;
    }
    await assertJobLease(env, job, now);
    return dispatchProvider(env, {
      provider: "whatsapp",
      to: currentRecipient.mobile,
      template: templateFor(env, payload.data?.templateKey || payload.kind, {
        ...(capacityMessage?capacityTemplateData(env,payload,currentRecipient):(payload.data || {})),
        link,
      }),
      idempotencyKey: job.job_id,
    });
  }
  if (job.type === "roster-inbox-reply") {
    const inbound = await one(
      env.DB,
      "SELECT * FROM roster_inbox WHERE school_id=? AND message_id=?",
      [schoolId, payload.inReplyTo],
    );
    if (
      !inbound ||
      Operations.mobile(JSON.parse(inbound.payload_json).sender) !==
        Operations.mobile(person.mobile) ||
      (inbound.staff_id !== person.staffId && inbound.staff_id !== person.id) ||
      now - inbound.received_at >= 86400000
    )
      return { state: "cancelled", code: "INBOX_REPLY_WINDOW_CLOSED" };
    await assertJobLease(env, job, now);
    return dispatchProvider(env, {
      provider: "whatsapp",
      to: person.mobile,
      text: payload.text,
      recentInboundAt: inbound.received_at,
      idempotencyKey: job.job_id,
    });
  }
  return { state: "failed", code: "ROSTER_JOB_UNSUPPORTED", retryable: false };
}
/** Called only after the shared Worker verifies the provider signature and active webhook owner. */
export async function webhook(ctx) {
  const { env, body } = ctx,
    now = ctx.now ?? Date.now(),
    schoolId = env.SCHOOL_ID,
    provider = ctx.provider || ctx.path.split("/").at(-1),
    events = [];
  requireValue(env.WEBHOOK_OWNER === "v1", "WEBHOOK_NOT_ACTIVE", 409);
  if (provider === "whatsapp") {
    requireValue(
      Array.isArray(body.entry) && body.entry.length <= 100,
      "WEBHOOK_BODY_INVALID",
    );
    for (const entry of body.entry)
      for (const change of entry.changes || []) {
        const value = change.value || {};
        requireValue(
          typeof env.WHATSAPP_PHONE_NUMBER_ID === "string" &&
            env.WHATSAPP_PHONE_NUMBER_ID.length > 0,
          "WHATSAPP_PHONE_NOT_CONFIGURED",
          503,
        );
        requireValue(
          value.metadata?.phone_number_id === env.WHATSAPP_PHONE_NUMBER_ID,
          "WEBHOOK_PHONE_MISMATCH",
          403,
        );
        for (const message of value.messages || [])
          events.push({
            kind: "message",
            id: message.id,
            at: Number(message.timestamp) * 1000,
            sender: message.from,
            text:
              message.text?.body ||
              message.button?.text ||
              message.interactive?.button_reply?.title ||
              "",
            buttonId:
              message.button?.payload ||
              message.interactive?.button_reply?.id ||
              null,
          });
        for (const status of value.statuses || [])
          events.push({
            kind: "status",
            id: status.id,
            at: Number(status.timestamp) * 1000,
            status: status.status,
            errors: (status.errors || []).map((e) => ({ code: e.code })),
          });
      }
  } else if (provider === "telegram") {
    requireValue(Number.isSafeInteger(body.update_id), "WEBHOOK_BODY_INVALID");
    const message = body.message || body.edited_message;
    if (message)
      events.push({
        kind: "message",
        id: String(body.update_id),
        at: Number(message.date) * 1000,
        sender: String(message.chat?.id || ""),
        text: message.text || "",
        buttonId: null,
      });
  } else throw new ApiError("PROVIDER_UNSUPPORTED", 404);
  requireValue(events.length <= 100, "WEBHOOK_BOUNDS", 413);
  let accepted = 0;
  for (const event of events) {
    requireValue(
      typeof event.id === "string" &&
        event.id.length > 0 &&
        event.id.length <= 512 &&
        Number.isSafeInteger(event.at) &&
        event.at >= 0 &&
        event.at <= now + 60000,
      "WEBHOOK_BODY_INVALID",
    );
    if (event.kind === "message")
      requireValue(
        typeof event.text === "string" &&
          event.text.length <= 4096 &&
          typeof event.sender === "string" &&
          event.sender.length <= 64,
        "WEBHOOK_BODY_INVALID",
      );
    const eventId = await digest({
      provider,
      id: event.id,
      kind: event.kind,
      status: event.status || null,
      at: event.at,
    });
    // Persist verified provider truth before the fanout-operation replay check.
    // An early receipt then survives a missing sender ID or interruption and
    // reconciles through the same monotonic recovery used by other owners.
    if (
      provider === "whatsapp" &&
      event.kind === "status" &&
      ["sent", "delivered", "read", "failed"].includes(event.status)
    )
      await recordProviderReceipt(
        env,
        {
          schoolId,
          provider,
          providerId: event.id,
          eventId,
          status: event.status,
          occurredAt: event.at,
        },
        now,
      );
    const op = await prepareOperation(env.DB, {
      schoolId,
      actorId: "provider:" + provider,
      operationId: "event:" + eventId,
      action: "internal.roster.webhook",
      intent: { eventId },
      now,
      validUntil: now + 60000,
    });
    if (op.receipt) continue;
    const writes = [
      {
        sql: "INSERT INTO roster_provider_events(school_id,event_id,provider,provider_message_id,event_kind,event_at,payload_json) VALUES(?,?,?,?,?,?,?)",
        args: [
          schoolId,
          eventId,
          provider,
          event.id,
          event.kind,
          event.at,
          canonical(event),
        ],
      },
    ];
    if (event.kind === "message")
      writes.push({
        sql: "INSERT OR IGNORE INTO roster_inbox(school_id,message_id,staff_id,received_at,state,assigned_to,revision,payload_json) VALUES(?,?,NULL,?,'unmatched',NULL,1,?)",
        args: [
          schoolId,
          provider + ":" + event.id,
          event.at,
          canonical({
            provider,
            sender: event.sender,
            text: event.text,
            buttonId: event.buttonId,
            identityVerified: false,
            actionState: "requires-verified-identity",
          }),
        ],
      });
    await commitOperation(env.DB, op, {
      now,
      admission: {
        kind: "system",
        reason:
          "Authenticated provider webhook delivers its defined deduplicated message/status event, never user authority.",
      },
      guards: [
        {
          sql: "SELECT NOT EXISTS(SELECT 1 FROM roster_provider_events WHERE school_id=? AND event_id=?) AS ok",
          args: [schoolId, eventId],
        },
      ],
      writes,
      outbox:
        event.kind === "message"
          ? [
              {
                id: "inbound:" + eventId,
                type: "roster-inbound",
                payload: { eventId },
                dueAt: now,
              },
            ]
          : provider === "whatsapp" &&
              ["sent", "delivered", "read", "failed"].includes(event.status)
            ? ["lord", "venues"].map((owner) => ({
                id: "receipt:" + owner + ":" + eventId,
                type: "roster-provider-receipt",
                payload: { eventId, owner },
                dueAt: now,
              }))
            : [],
      result: { accepted: true, eventId },
      audit: { provider, eventKind: event.kind, eventId },
    });
    accepted++;
  }
  return { accepted, received: events.length };
}
