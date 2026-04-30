import fs from "node:fs/promises";
import path from "node:path";
import { SpreadsheetFile, Workbook } from "@oai/artifact-tool";

const repoRoot = "/Users/NPTSG0388/Workspace/jira-creation-stack-host/AF_Codebases";
const stepEnumPath = path.join(repoRoot, "dbp-antifraud-component-master/dbp-antifraud-component-master/dbp-antifraud-common/src/main/java/com/shopee/banking/af/component/common/constant/StepEnum.java");
const stepActionEnumPath = path.join(repoRoot, "risk-tool-deploy/risk-tool-deploy/dbp-antifraud-risktool-legacy/src/main/java/com/shopee/banking/af/risktool/enums/StepActionEnum.java");
const stepDir = path.join(repoRoot, "risk-decision-dbp-master/risk-decision-dbp-master/risk-decision-app/src/main/java/com/shopee/banking/af/rd/app/service/shared/step");
const configRoot = path.join(repoRoot, "risk-config-master/risk-config-master/anti-fraud-service");
const outputDir = "/Users/NPTSG0388/Documents/New project/outputs/af_step_documentation";
const outputFile = path.join(outputDir, "AF_Authentication_Step_Documentation.xlsx");

function splitEnumArgs(raw) {
  const parts = [];
  let cur = "";
  let inQuote = false;
  for (let i = 0; i < raw.length; i++) {
    const ch = raw[i];
    if (ch === '"' && raw[i - 1] !== "\\") inQuote = !inQuote;
    if (ch === "," && !inQuote) {
      parts.push(cur.trim());
      cur = "";
    } else {
      cur += ch;
    }
  }
  if (cur.trim()) parts.push(cur.trim());
  return parts.map((p) => p.replace(/^"|"$/g, ""));
}

function splitSqlValues(raw) {
  const out = [];
  let cur = "";
  let inQuote = false;
  for (let i = 0; i < raw.length; i++) {
    const ch = raw[i];
    const next = raw[i + 1];
    if (ch === "'" && next === "'") {
      cur += "''";
      i++;
      continue;
    }
    if (ch === "'") inQuote = !inQuote;
    if (ch === "," && !inQuote) {
      out.push(cleanSqlValue(cur));
      cur = "";
    } else {
      cur += ch;
    }
  }
  if (cur.length) out.push(cleanSqlValue(cur));
  return out;
}

function cleanSqlValue(v) {
  const t = v.trim();
  if (/^null$/i.test(t)) return "";
  if (t.startsWith("'") && t.endsWith("'")) return t.slice(1, -1).replace(/''/g, "'");
  return t;
}

function sqlQuotedAfter(line, key) {
  const m = line.match(new RegExp(`${key}\\s*=\\s*'([^']*)'`, "i"));
  return m ? m[1] : "";
}

function categoryFor(step, desc) {
  if (["BE", "BD", "BSTD"].includes(step)) return "Flow control";
  if (step.startsWith("BJ")) return "Navigation";
  if (["BUDL", "BULUL", "BSKP", "BRT"].includes(step)) return "Inner/system";
  if (step.includes("ST") || step === "BSV") return "SoftToken";
  if (step.includes("FV") || step.includes("BIO") || step.includes("SGP") || step.includes("NSFV") || desc.includes("人脸") || desc.includes("Face") || desc.includes("Touch")) return "Biometric / face";
  if (step.includes("SO") || step.includes("EO") || step === "BO") return "OTP";
  if (step.includes("DOB") || step.includes("ND") || step.includes("NRIC")) return "Identity";
  if (step.includes("PN") || step.includes("PAN") || step.includes("PAV") || desc.includes("notification")) return "Notification";
  if (step.includes("PW")) return "Password";
  if (step.includes("P")) return "PIN";
  return "Other";
}

function notesFor(step, desc) {
  if (step === "BSV") return "Single SoftToken verification. Can be executed on backend when firstStep=BSV and rdVerifyInfo.softToken is already present/config allows it.";
  if (step === "BST" || step === "BSTC") return "SoftToken activation/seed issuance flow; BSTC additionally returns CA cert.";
  if (["BPWFV", "BPFV", "BSOFV", "BPBIO", "BPNSFV", "BPWNSFV", "BPSGP", "BPWSGP", "BPDOB", "BPWDOB", "BPND"].includes(step)) return "Alternative-auth step: one of the supported actions can satisfy the step.";
  if (step.includes("ST")) return "Combined with SoftToken verification. In comma-separated config it may still be part of a larger sequence.";
  if (["BUDL", "BULUL", "BSKP", "BRT"].includes(step)) return "Inner step; normally not exposed as a user-facing authentication challenge.";
  if (step.startsWith("BJ")) return "Navigation/control step for app or SDK handoff.";
  return "";
}

function containsStep(text, step) {
  return new RegExp(`(^|[^A-Z0-9_])${step}([^A-Z0-9_]|$)`).test(text || "");
}

async function walk(dir) {
  const entries = await fs.readdir(dir, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) files.push(...await walk(full));
    else files.push(full);
  }
  return files;
}

function rel(p) {
  return p.replace(`${repoRoot}/`, "");
}

const stepEnumText = await fs.readFile(stepEnumPath, "utf8");
const stepRows = [];
for (const m of stepEnumText.matchAll(/^\s*([A-Z0-9]+)\("([^"]*)",\s*"([^"]*)"\)/gm)) {
  stepRows.push({
    step: m[1],
    description: m[2],
    type: m[3],
  });
}

const stepActionText = await fs.readFile(stepActionEnumPath, "utf8");
const actionMap = new Map();
for (const m of stepActionText.matchAll(/^\s*([A-Z0-9]+)\(([^;]*?)\),?/gm)) {
  const args = splitEnumArgs(m[2]);
  if (args.length >= 2) actionMap.set(m[1], args[1]);
}

const componentMap = new Map();
for (const file of await walk(stepDir)) {
  if (!file.endsWith(".java")) continue;
  const text = await fs.readFile(file, "utf8");
  const comp = text.match(/@Component\("([^"]+)"\)/);
  if (!comp) continue;
  const cls = text.match(/class\s+([A-Za-z0-9_]+)/)?.[1] || path.basename(file, ".java");
  const actions = [];
  for (const m of text.matchAll(/ActionManager\.([A-Z0-9_]+)/g)) actions.push(m[1]);
  for (const m of text.matchAll(/super\("([^"]+)"/g)) actions.push(m[1]);
  componentMap.set(comp[1], {
    component: cls,
    file: rel(file),
    supportedActions: [...new Set(actions)].join(", "),
  });
}

const usageRows = [];
const sqlFiles = (await walk(configRoot)).filter((f) =>
  f.endsWith(".sql") && !f.includes("/backup/") && !f.includes("/ROLLBACK/")
);
const stepSet = new Set(stepRows.map((r) => r.step));
for (const file of sqlFiles) {
  const lines = (await fs.readFile(file, "utf8")).split(/\r?\n/);
  lines.forEach((line, idx) => {
    if (!/biz_scenario_flow_config_tab/i.test(line) || !/(insert|update)/i.test(line)) return;
    const matches = [...stepSet].filter((step) => containsStep(line, step));
    if (!matches.length) return;

    let scene = sqlQuotedAfter(line, "scene");
    let subScene = sqlQuotedAfter(line, "sub_scene");
    let action = sqlQuotedAfter(line, "action");
    const fields = [];

    const insert = line.match(/INSERT\s+INTO\s+[^()]+\(([^)]*)\)\s*VALUES\s*\((.*)\)\s*;?/i);
    if (insert) {
      const cols = splitSqlValues(insert[1].replace(/`/g, ""));
      const vals = splitSqlValues(insert[2]);
      const row = Object.fromEntries(cols.map((c, i) => [c.trim(), vals[i] || ""]));
      scene = row.scene || scene;
      subScene = row.sub_scene || subScene;
      action = row.action || action;
      for (const f of ["default_step", "challenge1_step", "challenge2_step", "challenge3_step", "challenge4_step", "challenge5_step"]) {
        if (row[f] && matches.some((s) => containsStep(row[f], s))) fields.push(`${f}: ${row[f]}`);
      }
    } else {
      for (const f of ["default_step", "challenge1_step", "challenge2_step", "challenge3_step", "challenge4_step", "challenge5_step"]) {
        const v = sqlQuotedAfter(line, f);
        if (v && matches.some((s) => containsStep(v, s))) fields.push(`${f}: ${v}`);
      }
    }

    for (const step of matches) {
      usageRows.push({
        step,
        scene,
        subScene,
        action,
        configuredSteps: fields.join(" | ") || line.trim().slice(0, 500),
        sourceFile: rel(file),
        line: idx + 1,
      });
    }
  });
}

const usageCount = new Map();
for (const u of usageRows) usageCount.set(u.step, (usageCount.get(u.step) || 0) + 1);

const dictionary = stepRows.map((r) => {
  const comp = componentMap.get(r.step) || {};
  return {
    Step: r.step,
    Type: r.type,
    Category: categoryFor(r.step, r.description),
    "Description (CN)": r.description,
    "Risk-tool Auth Actions": actionMap.get(r.step) || "",
    "RD Component": comp.component || "",
    "RD Supported Actions": comp.supportedActions || "",
    "SQL Usage Count": usageCount.get(r.step) || 0,
    Notes: notesFor(r.step, r.description),
  };
});

const topUsage = [...usageCount.entries()].sort((a, b) => b[1] - a[1]).slice(0, 15);

const workbook = Workbook.create();
const summary = workbook.worksheets.add("Summary");
const dictSheet = workbook.worksheets.add("Step Dictionary");
const usageSheet = workbook.worksheets.add("Usage Examples");
const compSheet = workbook.worksheets.add("Component Mapping");
const guideSheet = workbook.worksheets.add("Reading Guide");

function writeTable(sheet, startCell, headers, rows) {
  const startCol = startCell.match(/[A-Z]+/)[0];
  const startRow = Number(startCell.match(/\d+/)[0]);
  const colNum = (letters) => letters.split("").reduce((n, c) => n * 26 + c.charCodeAt(0) - 64, 0);
  const colLetters = (n) => {
    let s = "";
    while (n > 0) {
      const m = (n - 1) % 26;
      s = String.fromCharCode(65 + m) + s;
      n = Math.floor((n - 1) / 26);
    }
    return s;
  };
  const start = colNum(startCol);
  const end = colLetters(start + headers.length - 1);
  const values = [headers, ...rows.map((row) => headers.map((h) => row[h] ?? ""))];
  sheet.getRange(`${startCol}${startRow}:${end}${startRow + values.length - 1}`).values = values;
}

writeTable(summary, "A1", ["Metric", "Value"], [
  { Metric: "Workbook purpose", Value: "AF authentication step reference: definitions, action mappings, RD components, and SQL usage examples." },
  { Metric: "StepEnum source", Value: rel(stepEnumPath) },
  { Metric: "StepActionEnum source", Value: rel(stepActionEnumPath) },
  { Metric: "Config source filter", Value: "risk-config-master/anti-fraud-service/**/*.sql excluding backup and ROLLBACK paths" },
  { Metric: "Total steps", Value: stepRows.length },
  { Metric: "Normal steps", Value: stepRows.filter((r) => r.type === "normal").length },
  { Metric: "Inner steps", Value: stepRows.filter((r) => r.type === "inner").length },
  { Metric: "Steps with current SQL usage examples", Value: [...usageCount.keys()].length },
  { Metric: "Usage rows captured", Value: usageRows.length },
]);

writeTable(summary, "D1", ["Step", "Usage Rows"], topUsage.map(([step, count]) => ({ Step: step, "Usage Rows": count })));
writeTable(dictSheet, "A1", Object.keys(dictionary[0]), dictionary);
writeTable(usageSheet, "A1", ["step", "scene", "subScene", "action", "configuredSteps", "sourceFile", "line"], usageRows);
writeTable(compSheet, "A1", ["Step", "Component", "Supported Actions", "Source File"], [...componentMap.entries()].sort().map(([step, v]) => ({
  Step: step,
  Component: v.component,
  "Supported Actions": v.supportedActions,
  "Source File": v.file,
})));
writeTable(guideSheet, "A1", ["Term", "Explanation"], [
  { Term: "Step", Explanation: "Configured authentication step token, e.g. BSV, BPST, BFV." },
  { Term: "Comma-separated steps", Explanation: "A sequence. Example: BSV,BFV means verify SoftToken first, then face verification." },
  { Term: "Alternative-auth step", Explanation: "One step supports multiple possible actions. Example: BPFV accepts PIN or face verification." },
  { Term: "default_step", Explanation: "Authentication flow selected when risk decision returns the default level." },
  { Term: "challenge*_step", Explanation: "Authentication flow selected for challenge/risk levels." },
  { Term: "EXP:", Explanation: "Expression-based config. The actual step depends on runtime context such as appVersion or loginInfo." },
  { Term: "BSV", Explanation: "Single SoftToken verification. It validates a softToken generated from the device seed." },
  { Term: "BST/BSTC", Explanation: "SoftToken activation and verification flow. BSTC also returns CA certificate data." },
  { Term: "Usage count", Explanation: "Count of matching non-backup SQL config lines captured in this workbook, not a production DB count." },
]);

const output = await SpreadsheetFile.exportXlsx(workbook);
await fs.mkdir(outputDir, { recursive: true });
await output.save(outputFile);
console.log(outputFile);
