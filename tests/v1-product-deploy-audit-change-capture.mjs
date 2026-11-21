import assert from "node:assert/strict";
import { productSuite } from "./v1-product/suite.mjs";

const suite = await productSuite("v1-deploy-audit-change-capture");

try {
  const admin = await suite.seedAccount("synthetic-audit-capture-admin");
  const { versionId } = await suite.call(admin, "deploy", "/versions/create", {
    operationId: "audit-capture-create-version-1",
    name: "Synthetic Audit Capture 2026",
  });
  assert.ok(versionId, "versionId should exist");

  const schoolId = "synthetic-school";
  const sessionInfo = await suite.request(admin, "core", "/session");
  assert.equal(sessionInfo.status, 200);
  const expectedEmail = sessionInfo.body?.data?.email || sessionInfo.body?.email || `${admin.id}@example.invalid`;

  // 1. Seed a historical audit row lacking before/after/labels to prove it is preserved untouched.
  const deployDb = await suite.db("deploy");
  const legacyEventId = "op:legacy-audit-row-1";
  const legacyAt = Date.now() - 60_000;
  const legacyDetails = JSON.stringify({
    versionId,
    scope: "loads",
    changedRows: 1,
    previewId: null,
  });
  await deployDb
    .prepare(
      "INSERT INTO product_audit(school_id,event_id,operation_id,actor_id,action,recorded_at,decision_id,details_json) VALUES(?,?,?,?,?,?,?,?)",
    )
    .bind(
      schoolId,
      legacyEventId,
      "legacy-audit-row-1",
      "legacy-actor-uuid",
      "deploy.edit",
      legacyAt,
      "legacy-decision-1",
      legacyDetails,
    )
    .run();

  const allAuditRows = async () =>
    (
      await deployDb
        .prepare(
          "SELECT event_id,operation_id,actor_id,action,recorded_at,details_json FROM product_audit WHERE school_id=? ORDER BY recorded_at ASC, event_id ASC",
        )
        .bind(schoolId)
        .all()
    ).results || [];

  // Acquire leases for loads/classes edits before sampling audit count.
  const lease = async (scope) => {
    const grant = await suite.call(admin, "deploy", "/lease", {
      operationId: `audit-capture-lease-${scope}`,
      versionId,
      scope,
      clientId: "audit-capture-client",
    });
    return { clientId: "audit-capture-client", leaseFence: grant.fence };
  };

  const loadsLease = await lease("loads");
  const classesLease = await lease("classes");
  const countBefore = (await allAuditRows()).length;

  const boot = await suite.call(admin, "deploy", "/bootstrap", { versionId });
  const doc = boot.current || boot.version || boot;
  const loadLevels = Object.keys(doc.loads || {});
  assert.ok(
    loadLevels.length > 0,
    "default loads should have at least one level",
  );
  const targetLevel = loadLevels.includes("P6")
    ? "P6"
    : loadLevels.includes("6")
      ? "6"
      : loadLevels[0];
  const loadSubjects = Object.keys(doc.loads[targetLevel] || {});
  const targetSubject =
    loadSubjects.find((s) => /^sci/i.test(s)) || loadSubjects[0];
  assert.ok(targetSubject, "targetSubject should exist in default loads");

  // 2. Single Subject Load change (setLoad).
  const singleOpId = "audit-capture-single-load-1";
  const singleBody = {
    ...loadsLease,
    versionId,
    operationId: singleOpId,
    scope: "loads",
    operation: "setLoad",
    level: targetLevel,
    subject: targetSubject,
    value: 7,
  };
  const singleRes = await suite.call(admin, "deploy", "/change", singleBody);
  assert.equal(singleRes.saved, true, "single setLoad should succeed");

  const afterSingle = await allAuditRows();
  assert.equal(
    afterSingle.length,
    countBefore + 1,
    "single save adds 1 audit row",
  );
  const singleRow = afterSingle.find((r) => r.operation_id === singleOpId);
  assert.ok(singleRow, "single save audit row exists");
  assert.equal(singleRow.event_id, `op:${singleOpId}`);
  assert.equal(singleRow.actor_id, admin.id);
  assert.equal(singleRow.action, "deploy.edit");
  const singleDetails = JSON.parse(singleRow.details_json);
  assert.equal(singleDetails.versionId, versionId);
  assert.equal(singleDetails.operationId, singleOpId);
  assert.equal(singleDetails.originalActor, admin.id);
  assert.equal(singleDetails.actorLabel, expectedEmail);
  assert.equal(singleDetails.capabilityAction, "deploy.edit");
  assert.equal(singleDetails.operationLabel, "Update subject load");
  assert.equal(singleDetails.editCount, 1);
  assert.deepEqual(singleDetails.operations, ["setLoad"]);
  assert.ok(
    singleDetails.capturedChanges >= 1,
    "single setLoad should capture at least one changed entry",
  );
  assert.match(
    JSON.stringify(singleDetails.after),
    /7/,
    "after payload records new period load 7",
  );

  // 3. Idempotent retry with the same operationId must emit zero extra audit rows.
  const retryRes = await suite.call(admin, "deploy", "/change", singleBody);
  assert.equal(retryRes.saved, true, "idempotent retry should return saved");
  assert.deepEqual(retryRes, singleRes, "retry returns identical receipt");
  assert.equal(
    (await allAuditRows()).length,
    countBefore + 1,
    "idempotent retry must keep exactly 1 audit event",
  );

  // 4. Multi-edit save (setLoad + addClass).
  const multiOpId = "audit-capture-multi-edit-1";
  const multiRes = await suite.call(admin, "deploy", "/change", {
    ...classesLease,
    versionId,
    operationId: multiOpId,
    scope: "classes",
    edits: [
      {
        operation: "setLoad",
        level: targetLevel,
        subject: targetSubject,
        value: 8,
      },
      { operation: "addClass", level: targetLevel, name: "6Z", shortName: "6Z" },
    ],
  });
  assert.equal(multiRes.saved, true, "multi-edit save should succeed");

  const afterMulti = await allAuditRows();
  assert.equal(
    afterMulti.length,
    countBefore + 2,
    "multi-edit save adds 1 audit row",
  );
  const multiRow = afterMulti.find((r) => r.operation_id === multiOpId);
  assert.ok(multiRow, "multi-edit audit row exists");
  const multiDetails = JSON.parse(multiRow.details_json);
  assert.equal(multiDetails.editCount, 2);
  assert.deepEqual(multiDetails.operations, ["setLoad", "addClass"]);
  assert.match(
    multiDetails.operationLabel,
    /^Batch edit \(2 edits\): Update subject load, Add class$/,
  );
  assert.ok(
    multiDetails.capturedChanges >= 2,
    "multi-edit save captures both load and class changes",
  );
  assert.match(JSON.stringify(multiDetails.before), /7/);
  assert.match(JSON.stringify(multiDetails.after), /8/);
  assert.match(JSON.stringify(multiDetails.after), /6Z/);

  // 5. Rejected writes (stale expectedRevision 409 + invalid edit 400) emit zero audit rows.
  const staleRes = await suite.request(admin, "deploy", "/change", {
    method: "POST",
    body: {
      ...loadsLease,
      versionId,
      operationId: "audit-capture-rejected-409",
      scope: "loads",
      expectedRevision: 0,
      operation: "setLoad",
      level: targetLevel,
      subject: targetSubject,
      value: 9,
    },
  });
  assert.equal(
    staleRes.status,
    409,
    "stale revision write should be rejected with 409",
  );

  const invalidRes = await suite.request(admin, "deploy", "/change", {
    method: "POST",
    body: {
      ...loadsLease,
      versionId,
      operationId: "audit-capture-rejected-400",
      scope: "loads",
      operation: "saveVersionNote",
      notes: 12345,
    },
  });
  assert.equal(
    invalidRes.status,
    400,
    "invalid write should be rejected with 400",
  );

  const afterRejected = await allAuditRows();
  assert.equal(
    afterRejected.length,
    countBefore + 2,
    "rejected writes must produce zero new audit events",
  );
  assert.equal(
    afterRejected.some((r) => r.operation_id === "audit-capture-rejected-409"),
    false,
  );
  assert.equal(
    afterRejected.some((r) => r.operation_id === "audit-capture-rejected-400"),
    false,
  );

  // 6. Historical audit row remains intact.
  const preservedLegacy = afterRejected.find((r) => r.event_id === legacyEventId);
  assert.ok(preservedLegacy, "historical audit row preserved");
  assert.equal(preservedLegacy.details_json, legacyDetails);
  assert.equal(preservedLegacy.actor_id, "legacy-actor-uuid");

  console.log(
    "PASS Deploy audit change capture: single Subject Load, multi-edit batch, idempotent retry (1 event), rejected writes (0 events), readable actor/operation labels, preserved originalActor/eventId/operationId and historical rows.",
  );
} finally {
  await suite.close();
}
