import { FileBlob, SpreadsheetFile } from "@oai/artifact-tool";

const file = "/Users/NPTSG0388/Documents/New project/outputs/af_step_documentation/AF_Authentication_Step_Documentation.xlsx";
const input = await FileBlob.load(file);
const workbook = await SpreadsheetFile.importXlsx(input);

for (const range of [
  "Summary!A1:E20",
  "Step Dictionary!A1:I20",
  "Usage Examples!A1:G20",
  "Component Mapping!A1:D20",
  "Reading Guide!A1:B20",
]) {
  const inspected = await workbook.inspect({
    kind: "table",
    range,
    include: "values,formulas",
    tableMaxRows: 8,
    tableMaxCols: 10,
  });
  console.log(`INSPECT ${range}`);
  console.log(inspected.ndjson.split("\n").slice(0, 4).join("\n"));
}

const errors = await workbook.inspect({
  kind: "match",
  searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A",
  options: { useRegex: true, maxResults: 50 },
  summary: "formula error scan",
});
console.log("ERROR_SCAN");
console.log(errors.ndjson);

for (const sheetName of ["Summary", "Step Dictionary", "Usage Examples", "Component Mapping", "Reading Guide"]) {
  await workbook.render({ sheetName, range: "A1:H20", scale: 1 });
  console.log(`RENDER_OK ${sheetName}`);
}
