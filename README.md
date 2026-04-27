# Azure Data Factory MCP Server

An MCP server that enables AI assistants to build, manage, and monitor Azure Data Factory pipelines through natural conversation.


## Why This Exists

AI assistants have the potential to hugely speed up the development, testing, and maintenance of pipelines in Azure Data Factory - but only if they have direct access to the platform. Developing in ADF with AI assistance can require multiple iterations to reach a solution, and without direct access this involves copying pipeline definitions and error messages back and forth, manually deploying iterarions, and losing a lot of time.

This project aims to remedy this by providing an MCP-compatible AI assistant structured, autonomous access to ADF - full visibility into pipelines, the ability to create and modify resources, and direct feedback from pipeline runs. The assistant can iterate on a pipeline definition, trigger a run, inspect the failure, and fix the problem without any manual intervention.

MCP is emerging as the standard protocol for connecting AI assistants to external tools and services. Enterprise data platform tooling is a relatively unexplored area for MCP adoption, with this project being an early step in that direction.

## What It Can Do

**Inspect the factory**
- List all pipelines, linked services, datasets, and data flows by name
- Fetch the full JSON definition of any individual pipeline, linked service, or dataset

**Build and modify resources**
- Create or update pipelines, linked services, datasets, and data flows — accepting either an inline JSON string or an absolute path to a local `.json` file
- True upsert semantics via the Azure SDK: if a resource already exists it is updated in place, so Claude can iterate on a definition without any cleanup step
- Parameterised pipelines are fully supported — parameters can be supplied at trigger time

**Run and monitor**
- Trigger a pipeline run (with optional runtime parameters) and get back the run ID
- Poll run status: Queued → InProgress → Succeeded / Failed / Cancelled, with start/end timestamps and duration; optionally block until the run reaches a terminal state
- Fetch per-activity logs for a run, including activity type, timing, output, and structured error details — giving Claude full visibility into exactly where and why a pipeline failed

**Delete**
- Permanently delete pipelines, linked services, datasets, or data flows by name


### Example Workflow

The following prompt was given to Claude Desktop (Opus 4.7) with the ADF MCP server connected:

>Build a medallion architecture (bronze/silver/gold) data pipeline in Azure Data Factory that ingests weather and air quality data from the Open-Meteo API for Regents Park, London (lat 51.531, lon -0.160).
>
>You have ADF MCP tools available. The storage account is teststg1 (ADLS Gen2, system-assigned managed identity) with containers raw, silver, and gold.
>
>Bronze: Ingest three Open-Meteo endpoints as raw JSON into date-partitioned folders in the raw container: weather forecast, yesterday's weather actuals, and air quality.
>
>Silver: Use Mapping Data Flows to flatten the raw JSON into clean Delta tables. 
>
>Gold: Two outputs as Delta — a daily conditions view (forecast joined with daily-aggregated air quality) and a forecast accuracy view (forecast joined with actuals, with error metrics derived).
>
>Follow data factory and lakehouse best practices and test to ensure that any pipelines are working correctly.`
>
From that single prompt, with no further input beyond asking the model to continue, it autonomously created all linked services, datasets, data flows, and pipelines by iterating on failures until the full medallion pipeline ran successfully.


## Architecture

The server is a single Python file (`adf_mcp_server.py`) that sits between an MCP-compatible AI tool and Azure Data Factory. 

The AI tool launches it as a child process and communicates over stdin/stdout using [MCP](https://modelcontextprotocol.io/) JSON-RPC. All ADF operations go through the `azure-mgmt-datafactory` Python SDK. Authentication is handled by `DefaultAzureCredential`, which tries `az login` tokens, environment-variable service principals, and managed identity in order, so no credentials in code. Because the SDK is synchronous, every call runs via `asyncio.run_in_executor` so the MCP event loop is never blocked. All create/update tools use the SDK's native `create_or_update()` method. Resource definitions are accepted as either an inline JSON string or an absolute path to a local `.json` file.


## Getting Started
> **Note:** This project was developed and tested using Claude Desktop. The setup instructions below are specific to Claude Desktop. Other MCP-compatible tools should work but may require different configuration - refer to your tool's documentation for how to connect to an MCP server over stdio.

### Prerequisites

- Python 3.10+
- [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli) installed and authenticated (`az login`) - this is used by `DefaultAzureCredential` for local auth
- An Azure subscription with an existing ADF instance
- [Claude Desktop](https://claude.ai/download) or another MCP-compatible tool

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

Fully quit and relaunch Claude Desktop after saving and check the system tray to make sure the old instance is gone. The ADF tools will appear in Claude's tool list once the server starts successfully.

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
- Create/update tools accept an absolute file path and read it under the server process's own OS permissions, so only trusted MCP clients should connect to the server
- Trigger management (creating, enabling, disabling schedule or event-based triggers) is not yet implemented
- No support for ADF integration runtimes, credentials objects, or private endpoint configuration

**Potential future directions**
- Support for ADF triggers to enable autonomous scheduling workflows
- Integration with Git-backed ADF to commit generated artifacts directly to a repository branch
- Enterprise hardening: audit logging of all tool calls, rate limiting, and scoped read-only vs. read-write access modes
- Broader Azure data platform coverage. Synapse Analytics, Azure Databricks, or Azure SQL are potential candidates.


## Tech Stack

- **Python 3.10+** — asyncio for non-blocking SDK execution
- **[MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)** (`mcp>=1.26.0`) — server framework and stdio transport
- **`azure-mgmt-datafactory`** — Azure Data Factory management SDK
- **`azure-identity`** — `DefaultAzureCredential` for ambient authentication
- **Azure Data Factory** — target platform for all resource management and pipeline execution


## About

Built to demonstrate how MCP can bridge AI assistants and enterprise data platforms. Part of a broader interest in the intersection of data engineering and AI tooling.

[LinkedIn](https://www.linkedin.com/in/peter-duebel/)