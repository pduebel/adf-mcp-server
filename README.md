# Azure Data Factory MCP Server

An MCP server that enables AI assistants like Claude to build, manage, and monitor Azure Data Factory pipelines through natural conversation.


## Why This Exists

AI assistants have the potential to hugely speed up the development, testing and maintenance of pipelines in Azure Data Factory. However, this potential can be difficult to materialise using current tooling. Most chat-based AI tools have no way of connecting to ADF directly, and current models don't seem to be able to produce ADF artifacts with the same level of accuracy as they can do with artifacts coded in common languages. As a result, managing pipelines in ADF with AI assistance can involve multiple iterations to reach a solution, which can involve copying and pasting vast amounts of error messages and pipeline definitions from ADF into the AI tool and deploying and running different versions of artifacts.

By enabling AI assistants to connect to ADF in a structured and autonomous fashion, they can have full context of the pipelines they are working with and can iterate quickly, leading to a huge reduction in the time to reach a final solution, as well as having potential applications with autonomous agents used for things like pipeline monitoring and failure handling.

MCP is emerging as the standard protocol for connecting AI assistants to external tools and processes. Enterprise data platform tooling is a relatively unexplored area for MCP adoption. This project provides a working local MCP server that gives Claude Desktop direct access to ADF operations, including creating pipelines, managing resources, and monitoring runs.


## What It Can Do

**Inspect the factory**
- List all pipelines, linked services, datasets, and data flows by name
- Fetch the full JSON definition of any individual resource

**Build and modify resources**
- Create or update pipelines, linked services, datasets, and data flows — accepting either an inline JSON string or a path to a local `.json` file
- Upsert semantics: if a resource already exists it is replaced atomically, so Claude can iterate on a definition without manual cleanup
- Parameterised pipelines are fully supported — parameters can be supplied at trigger time

**Run and monitor**
- Trigger a pipeline run (with optional runtime parameters) and get back the run ID
- Poll run status: Queued → InProgress → Succeeded / Failed / Cancelled, with start/end timestamps and duration
- Fetch per-activity logs for a run, including activity type, timing, output, and structured error details — giving Claude full visibility into exactly where and why a pipeline failed

**Delete**
- Permanently delete pipelines, linked services, datasets, or data flows by name


### Example Workflow

As an example, I used the following prompt in Claude Desktop using Opus 4.7.

"Build a medallion architecture (bronze/silver/gold) data pipeline in Azure Data Factory that ingests weather and air quality data from the Open-Meteo API for Regents Park, London (lat 51.531, lon -0.160).

You have ADF MCP tools available. The storage account is teststg1 (ADLS Gen2, system-assigned managed identity) with containers raw, silver, and gold.

Bronze: Ingest three Open-Meteo endpoints as raw JSON into date-partitioned folders in the raw container: weather forecast, yesterday's weather actuals, and air quality.

Silver: Use Mapping Data Flows to flatten the raw JSON into clean Delta tables. 

Gold: Two outputs as Delta — a daily conditions view (forecast joined with daily-aggregated air quality) and a forecast accuracy view (forecast joined with actuals, with error metrics derived).

Follow data factory and lakehouse best practices and test to ensure that any pipelines are working correctly."

### REMEMBER TO FIX THE ISSUE WHERE CLAUDE DESKTOP IS GETTING CONFUSED BY THE TIMEZONE WHEN CHECKING PIPELINE RUNS


## Architecture

The server is a single Python file (`adf_mcp_server.py`) that sits between Claude Desktop and Azure Data Factory.

**Transport — MCP over stdio**
Claude Desktop launches the server as a child process and communicates over stdin/stdout using the [Model Context Protocol](https://modelcontextprotocol.io/) JSON-RPC wire format. All diagnostic logging goes to stderr so it never interferes with the protocol stream.

**Azure API layer — Azure CLI**
Rather than calling the ADF REST API directly, the server shells out to the `az datafactory` CLI extension. This keeps authentication entirely ambient: if you are logged in with `az login`, the server inherits those credentials automatically — no secrets in code or config.

Each CLI call runs via `asyncio.create_subprocess_exec` so the MCP event loop is never blocked. Every call has a 90-second timeout; if it fires, the server returns a structured error rather than hanging the tool call. On Windows, `az` is a `.cmd` batch file that cannot be executed directly — the server resolves the full path to `az.cmd` (or falls back to `cmd /c az`) to handle this transparently.

**Upsert pattern**
The `az datafactory` CLI has no native upsert command for most resource types. When a create call fails because the resource already exists, the server deletes the old version and immediately recreates it with the new definition. For data flows, the CLI does provide a native `update` command, so the server uses create→update instead.

**Resource definitions**
Create/update tools accept a resource body as either a compact inline JSON string or an `@filepath` reference to a local `.json` file. Inline JSON is always written to a temporary file before being passed to `az` — this avoids Windows `cmd.exe` misinterpreting special characters that appear inside ADF expressions.


## Getting Started

### Prerequisites

- Python 3.10+
- [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli) installed and authenticated (`az login`)
- The `datafactory` Azure CLI extension: `az extension add --name datafactory`
- An Azure subscription with an existing ADF instance
- [Claude Desktop](https://claude.ai/download) (or another MCP-compatible client)

### Installation

```bash
git clone https://github.com/your-username/data-factory-agent.git
cd data-factory-agent
pip install -r requirements.txt
```

### Configuration

The server requires three environment variables identifying your ADF instance:

| Variable | Description |
|---|---|
| `ADF_SUBSCRIPTION_ID` | Azure subscription ID |
| `ADF_RESOURCE_GROUP` | Resource group containing the factory |
| `ADF_FACTORY_NAME` | Name of the Data Factory |

The recommended way to supply these is via the `env` block in your Claude Desktop config (see below), which avoids relying on system environment variables that may not be set when the desktop app launches.

### Connecting to Claude Desktop

Add the following to `claude_desktop_config.json` (typically `%APPDATA%\Claude\claude_desktop_config.json` on Windows or `~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "adf": {
      "command": "python",
      "args": ["C:\\path\\to\\data-factory-agent\\adf_mcp_server.py"],
      "env": {
        "ADF_SUBSCRIPTION_ID": "<your-subscription-id>",
        "ADF_RESOURCE_GROUP": "<your-resource-group>",
        "ADF_FACTORY_NAME": "<your-factory-name>"
      }
    }
  }
}
```

Fully quit and relaunch Claude Desktop after saving — check the system tray to make sure the old instance is gone. The ADF tools will appear in Claude's tool list once the server starts successfully.

### Verifying the server

To confirm the server responds correctly before connecting Claude Desktop, pipe a minimal MCP exchange to it directly:

```bash
printf '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}\n{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}\n{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"list_pipelines","arguments":{}}}\n' \
  | ADF_SUBSCRIPTION_ID=... ADF_RESOURCE_GROUP=... ADF_FACTORY_NAME=... python adf_mcp_server.py
```

You should see two JSON-RPC responses: an `initialize` result and then the `list_pipelines` result. If the tool response is missing, check stderr for errors.


## Limitations & Future Work

**Current scope**
- Designed as a local developer tool for a single authenticated user, not a production deployment
- Authentication is delegated entirely to `az login` — there is no support for service principals, managed identities, or multi-user access control
- The `az datafactory` CLI has no native upsert for pipelines, linked services, or datasets: updates are implemented as delete + recreate, which means there is a brief window where the resource does not exist. This is fine for development but not for live production pipelines
- Trigger management (creating, enabling, disabling schedules or event-based triggers) is not yet implemented
- No support for ADF integration runtimes, credentials objects, or private endpoint configuration

**Potential future directions**
- Authentication via service principal or managed identity for deployment in shared or CI/CD environments
- Support for ADF triggers to enable autonomous scheduling workflows
- Integration with Git-backed ADF to commit generated artifacts directly to a repository branch
- Enterprise hardening: audit logging of all tool calls, rate limiting, and scoped read-only vs. read-write access modes
- Broader Azure data platform coverage — Synapse Analytics, Azure Databricks, or Azure SQL as first-class targets


## Tech Stack

- **Python 3.10+** — asyncio for non-blocking subprocess execution
- **[MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)** (`mcp>=1.26.0`) — server framework and stdio transport
- **Azure CLI** (`az`) with the `datafactory` extension — all ADF operations are delegated to the CLI rather than calling the REST API directly
- **Azure Data Factory** — target platform for all resource management and pipeline execution


## About

Brief context on why you built this — one or two sentences. Something like: "Built as an exploration of how MCP can bridge AI assistants and enterprise data platforms. Part of a broader interest in the intersection of data engineering and AI tooling."

Link to your LinkedIn if you want.
