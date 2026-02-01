# Iceberg Query Engine MCP App

An MCP (Model Context Protocol) app that provides interactive SQL query execution and results visualization within Claude Desktop.

## Features

- **Execute SQL Queries**: Run queries against the Iceberg Query Engine
- **Explain Query Plans**: View execution plans for query optimization
- **Interactive Results UI**: Tabular data display with metrics
- **Dark/Light Theme Support**: Adapts to Claude Desktop theme

## Installation

1. Build the Rust query engine:
   ```bash
   cd ..
   cargo build --release
   ```

2. Build the MCP app:
   ```bash
   cd mcp-app
   npm install
   npm run build
   ```

3. Add to Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "iceberg-query-engine": {
      "command": "node",
      "args": ["/path/to/iceberg-query-engine/mcp-app/dist/server.js"]
    }
  }
}
```

## Usage

Once configured in Claude Desktop:

1. Ask Claude to execute a SQL query:
   > "Run SELECT * FROM lineitem LIMIT 10"

2. View the interactive results in the embedded UI

3. Ask for query plans:
   > "Explain the query SELECT COUNT(*) FROM orders"

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Claude Chat Window                    │
│  ┌─────────────────────────────────────────────────────┐│
│  │                MCP App (iframe)                      ││
│  │  ┌─────────────┐ ┌─────────────┐ ┌────────────────┐ ││
│  │  │ Results Tab │ │  Plan Tab   │ │  Metrics Tab   │ ││
│  │  └─────────────┘ └─────────────┘ └────────────────┘ ││
│  │  ┌─────────────────────────────────────────────────┐││
│  │  │              Interactive Data Grid              │││
│  │  │  id  │  name   │  amount  │  date              │││
│  │  │  1   │  Alice  │  100.00  │  2026-01-15        │││
│  │  │  2   │  Bob    │  250.50  │  2026-01-20        │││
│  │  └─────────────────────────────────────────────────┘││
│  │  ┌─────────────────────────────────────────────────┐││
│  │  │  Execution: 45ms │ Rows: 1,234 │ Memory: 4MB   │││
│  │  └─────────────────────────────────────────────────┘││
│  └─────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────┘
```

## File Structure

```
mcp-app/
├── package.json          # NPM dependencies
├── tsconfig.json         # TypeScript config
├── src/
│   ├── server.ts         # MCP server with tool registration
│   └── app/
│       ├── index.html    # Main app HTML
│       ├── app.ts        # App class initialization
│       └── components/   # UI components (future)
└── dist/                 # Built output
    ├── server.js         # Bundled MCP server
    ├── app.js            # Bundled app code
    ├── results.html      # Results viewer HTML
    └── plan.html         # Plan viewer HTML
```

## Development

```bash
# Install dependencies
npm install

# Build
npm run build

# Run (for testing)
npm run dev
```

## Dependencies

- `@modelcontextprotocol/sdk` - MCP SDK for server
- `@modelcontextprotocol/ext-apps` - Ext-apps SDK for UI
- `esbuild` - Fast bundler
- `typescript` - Type checking
- `zod` - Schema validation
