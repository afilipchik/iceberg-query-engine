import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  registerAppTool,
  registerAppResource,
  RESOURCE_MIME_TYPE
} from "@modelcontextprotocol/ext-apps/server";
import { z } from "zod";
import { spawn } from "child_process";
import fs from "node:fs/promises";

const server = new McpServer({
  name: "iceberg-query-engine",
  version: "1.0.0"
});

// Tool: Execute SQL Query
registerAppTool(
  server,
  "execute-query",
  {
    title: "Execute SQL Query",
    description: "Execute a SQL query against the Iceberg Query Engine and display interactive results",
    inputSchema: {
      sql: z.string().describe("The SQL query to execute"),
      format: z.enum(["json"]).default("json").describe("Output format")
    },
    _meta: { "ui/resourceUri": "ui://iceberg-engine/results.html" }
  },
  async ({ sql }) => {
    // Call the Rust query engine
    const result = await executeQuery(sql);
    return {
      content: [{ type: "text", text: JSON.stringify(result) }]
    };
  }
);

// Tool: Explain Query Plan
registerAppTool(
  server,
  "explain-query",
  {
    title: "Explain Query Plan",
    description: "Show the execution plan for a SQL query",
    inputSchema: {
      sql: z.string().describe("The SQL query to explain")
    },
    _meta: { "ui/resourceUri": "ui://iceberg-engine/plan.html" }
  },
  async ({ sql }) => {
    const result = await executeQuery(`EXPLAIN ${sql}`);
    return {
      content: [{ type: "text", text: JSON.stringify(result) }]
    };
  }
);

// Register HTML resources
registerAppResource(
  server,
  "Query Results Viewer",
  "ui://iceberg-engine/results.html",
  { mimeType: RESOURCE_MIME_TYPE },
  async () => ({
    contents: [{
      uri: "ui://iceberg-engine/results.html",
      mimeType: RESOURCE_MIME_TYPE,
      text: await fs.readFile("dist/results.html", "utf-8")
    }]
  })
);

registerAppResource(
  server,
  "Query Plan Viewer",
  "ui://iceberg-engine/plan.html",
  { mimeType: RESOURCE_MIME_TYPE },
  async () => ({
    contents: [{
      uri: "ui://iceberg-engine/plan.html",
      mimeType: RESOURCE_MIME_TYPE,
      text: await fs.readFile("dist/plan.html", "utf-8")
    }]
  })
);

// Helper: Execute query via Rust CLI
async function executeQuery(sql: string): Promise<QueryResult> {
  return new Promise((resolve, reject) => {
    const child = spawn("cargo", [
      "run", "--release", "--", "sql", sql, "--format", "json"
    ], { cwd: ".." });

    let stdout = "";
    let stderr = "";

    child.stdout?.on("data", (data) => stdout += data);
    child.stderr?.on("data", (data) => stderr += data);

    child.on("close", (code) => {
      if (code === 0) {
        try {
          resolve(JSON.parse(stdout));
        } catch (e) {
          reject(new Error(`Failed to parse output: ${stdout}`));
        }
      } else {
        reject(new Error(stderr || `Process exited with code ${code}`));
      }
    });
  });
}

// Types
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

// Start server
(async () => {
  const transport = new StdioServerTransport();
  await server.connect(transport);
})();
