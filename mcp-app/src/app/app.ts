import {
  App,
  PostMessageTransport,
  applyHostStyleVariables,
  applyDocumentTheme
} from "@modelcontextprotocol/ext-apps";

interface QueryResult {
  schema: { fields: Array<{ name: string; type: string }> };
  rows: any[][];
  row_count: number;
  metrics: {
    parse_ms: number;
    plan_ms: number;
    optimize_ms: number;
    execute_ms: number;
    total_ms: number;
    peak_memory_bytes: number;
  };
}

const app = new App(
  {
    name: "Iceberg Query Results",
    version: "1.0.0"
  },
  {}
);

// Handle tool results
app.ontoolresult = (params) => {
  try {
    const data: QueryResult = JSON.parse(params.content[0].text);
    renderResults(data);
  } catch (e) {
    showError(`Failed to parse results: ${e}`);
  }
};

// Handle theme changes
app.onhostcontextchanged = (params) => {
  if (params.theme) applyDocumentTheme(params.theme);
  if (params.styles?.variables) applyHostStyleVariables(params.styles.variables);
};

function renderResults(data: QueryResult) {
  const container = document.getElementById("results")!;

  // Build table header
  const headerRow = data.schema.fields
    .map((f) => `<th>${f.name}<br><small>${f.type}</small></th>`)
    .join("");

  // Build table rows
  const dataRows = data.rows
    .map((row) =>
      `<tr>${row.map((cell) => `<td>${cell ?? "NULL"}</td>`).join("")}</tr>`
    )
    .join("");

  // Render table
  container.innerHTML = `
    <div class="metrics-bar">
      <span>Rows: ${data.row_count.toLocaleString()}</span>
      <span>Execution: ${data.metrics.total_ms}ms</span>
      <span>Memory: ${formatBytes(data.metrics.peak_memory_bytes)}</span>
    </div>
    <div class="metrics-breakdown">
      <div class="metric-item">
        <label>Parse</label>
        <span>${data.metrics.parse_ms}ms</span>
      </div>
      <div class="metric-item">
        <label>Plan</label>
        <span>${data.metrics.plan_ms}ms</span>
      </div>
      <div class="metric-item">
        <label>Optimize</label>
        <span>${data.metrics.optimize_ms}ms</span>
      </div>
      <div class="metric-item">
        <label>Execute</label>
        <span>${data.metrics.execute_ms}ms</span>
      </div>
    </div>
    <table class="results-table">
      <thead><tr>${headerRow}</tr></thead>
      <tbody>${dataRows}</tbody>
    </table>
  `;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function showError(message: string) {
  document.getElementById("results")!.innerHTML = `
    <div class="error">${message}</div>
  `;
}

// Connect to host
(async () => {
  await app.connect(new PostMessageTransport(window.parent));
})();
