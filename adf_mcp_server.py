"""
adf_mcp_server.py — MCP server exposing Azure Data Factory tools via az CLI.

Tools:
  create_or_update_pipeline  — create or upsert a pipeline definition
  trigger_pipeline_run       — start a pipeline run and return the run ID
  get_pipeline_run_status    — check the status of a run by run ID
  get_activity_run_logs      — fetch per-activity logs for a run

Authentication: ambient az login (no credentials in code).
ADF identity:   ADF_SUBSCRIPTION_ID, ADF_RESOURCE_GROUP, ADF_FACTORY_NAME env vars.
"""

import asyncio
import functools
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone

import mcp.types as types
from mcp.server import Server
from mcp.server.stdio import stdio_server

# ── Logging (must go to stderr; stdout is reserved for MCP JSON-RPC) ─────────
logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── ADF identity (read once at module load; checked per-call) ─────────────────
ADF_SUBSCRIPTION_ID = os.environ.get("ADF_SUBSCRIPTION_ID", "")
ADF_RESOURCE_GROUP = os.environ.get("ADF_RESOURCE_GROUP", "")
ADF_FACTORY_NAME = os.environ.get("ADF_FACTORY_NAME", "")

_ADF_ENV_KEYS = {
    "ADF_SUBSCRIPTION_ID": ADF_SUBSCRIPTION_ID,
    "ADF_RESOURCE_GROUP": ADF_RESOURCE_GROUP,
    "ADF_FACTORY_NAME": ADF_FACTORY_NAME,
}


def _check_config() -> dict | None:
    """Return an error dict if any required env var is missing, else None."""
    missing = [k for k, v in _ADF_ENV_KEYS.items() if not v]
    if missing:
        return {
            "success": False,
            "error": f"Missing required environment variables: {', '.join(missing)}",
            "stderr": "",
        }
    return None


def _base_flags() -> list[str]:
    """Common ADF resource flags shared by every az command."""
    return [
        "--subscription", ADF_SUBSCRIPTION_ID,
        "--resource-group", ADF_RESOURCE_GROUP,
        "--factory-name", ADF_FACTORY_NAME,
    ]


# ── subprocess helper ─────────────────────────────────────────────────────────

async def _run_az(cmd: list[str]) -> tuple[int, str, str]:
    """
    Run an az CLI command asynchronously.
    Returns (returncode, stdout, stderr). Never raises.
    """
    try:
        proc: subprocess.CompletedProcess = await asyncio.to_thread(
            functools.partial(
                subprocess.run,
                cmd,
                capture_output=True,
                text=True,
                env=os.environ.copy(),
            )
        )
        return proc.returncode, proc.stdout.strip(), proc.stderr.strip()
    except Exception as exc:
        return -1, "", str(exc)


# ── pipeline JSON resolver ────────────────────────────────────────────────────

def _resolve_pipeline_json(pipeline_definition: str) -> tuple[str | None, dict | None]:
    """
    Resolve pipeline_definition to the value for az --pipeline.

    Accepts either:
      - An inline JSON string starting with '{'
      - An absolute file path to a .json file (passed as @filepath to az)

    Returns (az_arg, None) on success, or (None, error_dict) on failure.
    """
    value = pipeline_definition.strip()

    if value.startswith("{"):
        # Inline JSON — validate and normalise to a compact single-line string.
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            return None, {
                "success": False,
                "error": f"pipeline_definition is not valid JSON: {exc}",
                "stderr": "",
            }
        return json.dumps(parsed, separators=(",", ":")), None

    # Treat as a file path.
    abs_path = os.path.abspath(value)
    if not os.path.isfile(abs_path):
        return None, {
            "success": False,
            "error": f"pipeline_definition file not found: {abs_path}",
            "stderr": "",
        }
    try:
        with open(abs_path, encoding="utf-8") as fh:
            json.load(fh)  # validate JSON
    except json.JSONDecodeError as exc:
        return None, {
            "success": False,
            "error": f"pipeline_definition file is not valid JSON: {exc}",
            "stderr": "",
        }
    # Use @filepath form — avoids shell-quoting issues with large JSON bodies.
    return f"@{abs_path}", None


# ── Tool 1: create_or_update_pipeline ────────────────────────────────────────

async def _create_or_update_pipeline(args: dict) -> dict:
    if err := _check_config():
        return {**err, "tool": "create_or_update_pipeline"}

    pipeline_name: str = args.get("pipeline_name", "").strip()
    pipeline_definition: str = args.get("pipeline_definition", "").strip()

    if not pipeline_name:
        return {"success": False, "error": "pipeline_name is required.", "stderr": "", "tool": "create_or_update_pipeline"}
    if not pipeline_definition:
        return {"success": False, "error": "pipeline_definition is required.", "stderr": "", "tool": "create_or_update_pipeline"}

    az_arg, resolve_err = _resolve_pipeline_json(pipeline_definition)
    if resolve_err:
        return {**resolve_err, "tool": "create_or_update_pipeline"}

    create_cmd = [
        "az", "datafactory", "pipeline", "create",
        *_base_flags(),
        "--name", pipeline_name,
        "--pipeline", az_arg,
        "--output", "json",
        "--only-show-errors",
    ]

    rc, stdout, stderr = await _run_az(create_cmd)

    if rc == 0:
        try:
            pipeline_obj = json.loads(stdout) if stdout else {}
        except json.JSONDecodeError:
            pipeline_obj = {"raw": stdout}
        return {
            "success": True,
            "action": "created",
            "pipeline_name": pipeline_name,
            "pipeline": pipeline_obj,
        }

    # If the pipeline already exists, delete then re-create (upsert).
    if "alreadyexists" in stderr.lower() or "already exists" in stderr.lower():
        delete_cmd = [
            "az", "datafactory", "pipeline", "delete",
            *_base_flags(),
            "--name", pipeline_name,
            "--yes",
            "--only-show-errors",
        ]
        del_rc, _, del_stderr = await _run_az(delete_cmd)
        if del_rc != 0:
            return {
                "success": False,
                "error": f"Failed to delete existing pipeline '{pipeline_name}' before update.",
                "stderr": del_stderr,
                "tool": "create_or_update_pipeline",
            }

        rc2, stdout2, stderr2 = await _run_az(create_cmd)
        if rc2 != 0:
            return {
                "success": False,
                "error": f"Failed to re-create pipeline '{pipeline_name}' after delete.",
                "stderr": stderr2,
                "tool": "create_or_update_pipeline",
            }
        try:
            pipeline_obj = json.loads(stdout2) if stdout2 else {}
        except json.JSONDecodeError:
            pipeline_obj = {"raw": stdout2}
        return {
            "success": True,
            "action": "updated",
            "pipeline_name": pipeline_name,
            "pipeline": pipeline_obj,
        }

    return {
        "success": False,
        "error": f"Failed to create pipeline '{pipeline_name}'.",
        "stderr": stderr,
        "tool": "create_or_update_pipeline",
    }


# ── Tool 2: trigger_pipeline_run ─────────────────────────────────────────────

async def _trigger_pipeline_run(args: dict) -> dict:
    if err := _check_config():
        return {**err, "tool": "trigger_pipeline_run"}

    pipeline_name: str = args.get("pipeline_name", "").strip()
    parameters: dict | None = args.get("parameters")

    if not pipeline_name:
        return {"success": False, "error": "pipeline_name is required.", "stderr": "", "tool": "trigger_pipeline_run"}

    cmd = [
        "az", "datafactory", "pipeline", "create-run",
        *_base_flags(),
        "--name", pipeline_name,
        "--output", "json",
        "--only-show-errors",
    ]

    if parameters:
        try:
            cmd += ["--parameters", json.dumps(parameters, separators=(",", ":"))]
        except (TypeError, ValueError) as exc:
            return {"success": False, "error": f"Invalid parameters: {exc}", "stderr": "", "tool": "trigger_pipeline_run"}

    rc, stdout, stderr = await _run_az(cmd)
    if rc != 0:
        return {
            "success": False,
            "error": f"Failed to trigger pipeline run for '{pipeline_name}'.",
            "stderr": stderr,
            "tool": "trigger_pipeline_run",
        }

    try:
        result = json.loads(stdout) if stdout else {}
    except json.JSONDecodeError:
        result = {"raw": stdout}

    run_id = result.get("runId", "")
    return {
        "success": True,
        "run_id": run_id,
        "pipeline_name": pipeline_name,
    }


# ── Tool 3: get_pipeline_run_status ──────────────────────────────────────────

async def _get_pipeline_run_status(args: dict) -> dict:
    if err := _check_config():
        return {**err, "tool": "get_pipeline_run_status"}

    run_id: str = args.get("run_id", "").strip()
    if not run_id:
        return {"success": False, "error": "run_id is required.", "stderr": "", "tool": "get_pipeline_run_status"}

    cmd = [
        "az", "datafactory", "pipeline-run", "show",
        *_base_flags(),
        "--run-id", run_id,
        "--output", "json",
        "--only-show-errors",
    ]

    rc, stdout, stderr = await _run_az(cmd)
    if rc != 0:
        return {
            "success": False,
            "error": f"Failed to retrieve pipeline run status for run ID '{run_id}'.",
            "stderr": stderr,
            "tool": "get_pipeline_run_status",
        }

    try:
        data = json.loads(stdout) if stdout else {}
    except json.JSONDecodeError:
        data = {"raw": stdout}

    return {
        "success": True,
        "run_id": run_id,
        "pipeline_name": data.get("pipelineName", ""),
        "status": data.get("status", ""),
        "run_start": data.get("runStart", None),
        "run_end": data.get("runEnd", None),
        "duration_ms": data.get("durationInMs", None),
        "message": data.get("message", ""),
        "run_group_id": data.get("runGroupId", ""),
    }


# ── Tool 4: get_activity_run_logs ────────────────────────────────────────────

async def _get_activity_run_logs(args: dict) -> dict:
    if err := _check_config():
        return {**err, "tool": "get_activity_run_logs"}

    run_id: str = args.get("run_id", "").strip()
    if not run_id:
        return {"success": False, "error": "run_id is required.", "stderr": "", "tool": "get_activity_run_logs"}

    last_updated_after: str | None = args.get("last_updated_after")
    last_updated_before: str | None = args.get("last_updated_before")

    # Derive time window from the pipeline run if not supplied by caller.
    if not last_updated_after or not last_updated_before:
        status_result = await _get_pipeline_run_status({"run_id": run_id})
        if not status_result.get("success"):
            return {**status_result, "tool": "get_activity_run_logs"}

        run_start = status_result.get("run_start")
        run_end = status_result.get("run_end")
        pipeline_status = status_result.get("status", "")

        if pipeline_status == "Queued" or not run_start:
            return {
                "success": True,
                "run_id": run_id,
                "activity_count": 0,
                "activities": [],
                "note": f"Pipeline run is '{pipeline_status}' and has not started yet. No activity logs available.",
            }

        if not last_updated_after:
            last_updated_after = run_start
        if not last_updated_before:
            if run_end:
                last_updated_before = run_end
            else:
                last_updated_before = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    cmd = [
        "az", "datafactory", "activity-run", "query-by-pipeline-run",
        *_base_flags(),
        "--run-id", run_id,
        "--last-updated-after", last_updated_after,
        "--last-updated-before", last_updated_before,
        "--output", "json",
        "--only-show-errors",
    ]

    rc, stdout, stderr = await _run_az(cmd)
    if rc != 0:
        return {
            "success": False,
            "error": f"Failed to retrieve activity logs for run ID '{run_id}'.",
            "stderr": stderr,
            "tool": "get_activity_run_logs",
        }

    try:
        data = json.loads(stdout) if stdout else {}
    except json.JSONDecodeError:
        data = {"raw": stdout}

    raw_activities = data.get("value", []) if isinstance(data, dict) else []

    activities = []
    for act in raw_activities:
        error_info = act.get("error")
        if isinstance(error_info, dict) and not any(error_info.values()):
            error_info = None
        activities.append({
            "activity_name": act.get("activityName", ""),
            "activity_type": act.get("activityType", ""),
            "status": act.get("status", ""),
            "run_start": act.get("activityRunStart", None),
            "run_end": act.get("activityRunEnd", None),
            "duration_ms": act.get("durationInMs", None),
            "error": error_info,
            "output": act.get("output", None),
        })

    return {
        "success": True,
        "run_id": run_id,
        "activity_count": len(activities),
        "activities": activities,
    }


# ── MCP server wiring ─────────────────────────────────────────────────────────

server = Server("adf-mcp-server")


@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="create_or_update_pipeline",
            description=(
                "Create or update an Azure Data Factory pipeline. "
                "Pass the pipeline definition as an inline JSON string or an absolute file path. "
                "If the pipeline already exists it will be replaced (delete + create)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline_name": {
                        "type": "string",
                        "description": "Name of the ADF pipeline to create or update.",
                    },
                    "pipeline_definition": {
                        "type": "string",
                        "description": (
                            "Pipeline JSON body as an inline string starting with '{', "
                            "or an absolute file path to a .json file."
                        ),
                    },
                },
                "required": ["pipeline_name", "pipeline_definition"],
            },
        ),
        types.Tool(
            name="trigger_pipeline_run",
            description=(
                "Trigger a named ADF pipeline and return the run ID. "
                "Optionally supply key-value parameters for the run."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline_name": {
                        "type": "string",
                        "description": "Name of the ADF pipeline to trigger.",
                    },
                    "parameters": {
                        "type": "object",
                        "description": "Optional key-value parameters passed to the pipeline run.",
                        "additionalProperties": True,
                    },
                },
                "required": ["pipeline_name"],
            },
        ),
        types.Tool(
            name="get_pipeline_run_status",
            description=(
                "Check the status of an ADF pipeline run by run ID. "
                "Returns status (Queued/InProgress/Succeeded/Failed/Cancelled), "
                "start/end timestamps, duration, and any error message."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "run_id": {
                        "type": "string",
                        "description": "The pipeline run ID returned by trigger_pipeline_run.",
                    },
                },
                "required": ["run_id"],
            },
        ),
        types.Tool(
            name="get_activity_run_logs",
            description=(
                "Fetch detailed per-activity logs for an ADF pipeline run. "
                "Returns each activity's name, type, status, timing, and error details. "
                "The time window defaults to the run's own start/end and can be overridden."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "run_id": {
                        "type": "string",
                        "description": "The pipeline run ID whose activity logs to fetch.",
                    },
                    "last_updated_after": {
                        "type": "string",
                        "description": "Optional ISO 8601 lower bound. Defaults to the pipeline run's start time.",
                    },
                    "last_updated_before": {
                        "type": "string",
                        "description": "Optional ISO 8601 upper bound. Defaults to the pipeline run's end time (or now if still running).",
                    },
                },
                "required": ["run_id"],
            },
        ),
    ]


@server.call_tool()
async def handle_call_tool(
    name: str,
    arguments: dict,
) -> list[types.TextContent]:
    dispatch = {
        "create_or_update_pipeline": _create_or_update_pipeline,
        "trigger_pipeline_run": _trigger_pipeline_run,
        "get_pipeline_run_status": _get_pipeline_run_status,
        "get_activity_run_logs": _get_activity_run_logs,
    }

    handler = dispatch.get(name)
    if handler is None:
        result = {
            "success": False,
            "error": f"Unknown tool: '{name}'. Available tools: {list(dispatch)}",
            "stderr": "",
            "tool": name,
        }
    else:
        result = await handler(arguments or {})

    return [types.TextContent(type="text", text=json.dumps(result, indent=2))]


# ── Entrypoint ────────────────────────────────────────────────────────────────

async def _check_datafactory_extension() -> None:
    """Warn to stderr if the az datafactory extension is not installed."""
    rc, _, _ = await _run_az(["az", "datafactory", "--help"])
    if rc != 0:
        print(
            "WARNING: The 'datafactory' Azure CLI extension does not appear to be installed. "
            "Run: az extension add --name datafactory",
            file=sys.stderr,
        )


async def main() -> None:
    await _check_datafactory_extension()
    logger.info("Starting ADF MCP server (stdio transport).")
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


if __name__ == "__main__":
    asyncio.run(main())
