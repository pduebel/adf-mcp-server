"""
adf_mcp_server.py — MCP server exposing Azure Data Factory tools via az CLI.

Tools:
  list_pipelines                 — list all pipelines in the factory
  list_linked_services           — list all linked services in the factory
  list_datasets                  — list all datasets in the factory
  get_pipeline                   — get the full definition of a single pipeline
  get_linked_service             — get the full definition of a single linked service
  get_dataset                    — get the full definition of a single dataset
  create_or_update_pipeline      — create or upsert a pipeline definition
  create_or_update_linked_service — create or upsert a linked service definition
  create_or_update_dataset       — create or upsert a dataset definition
  delete_pipeline                — delete a pipeline by name
  delete_linked_service          — delete a linked service by name
  delete_dataset                 — delete a dataset by name
  trigger_pipeline_run           — start a pipeline run and return the run ID
  get_pipeline_run_status        — check the status of a run by run ID
  get_activity_run_logs          — fetch per-activity logs for a run

Authentication: ambient az login (no credentials in code).
ADF identity:   ADF_SUBSCRIPTION_ID, ADF_RESOURCE_GROUP, ADF_FACTORY_NAME env vars.
"""

import asyncio
import json
import logging
import os
import platform
import shutil
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


def _az_exe() -> list[str]:
    """
    Return the prefix needed to invoke the Azure CLI on this platform.

    On Windows, 'az' is a .cmd batch file and cannot be executed directly
    by subprocess without the shell. We use 'cmd /c az' so that Windows can
    locate and run az.cmd through the normal PATH lookup.
    On other platforms, 'az' is a regular executable.
    """
    if platform.system() == "Windows":
        # Prefer the full path if we can find it (survives a minimal PATH).
        az_path = shutil.which("az.cmd") or shutil.which("az")
        if az_path:
            return [az_path]
        # Fall back to letting cmd.exe resolve it.
        return ["cmd", "/c", "az"]
    return ["az"]


def _base_flags() -> list[str]:
    """Common ADF resource flags shared by every az command."""
    return [
        "--subscription", ADF_SUBSCRIPTION_ID,
        "--resource-group", ADF_RESOURCE_GROUP,
        "--factory-name", ADF_FACTORY_NAME,
    ]


# ── subprocess helper ─────────────────────────────────────────────────────────

# How long (seconds) to wait for any single az CLI call before giving up.
# Claude Desktop's tool-call timeout is typically 60–120 s; keeping this below
# that ensures we return a clean error rather than a silent timeout on the
# client side.  Upsert operations can involve two sequential az calls
# (delete + create), so each individual call gets this budget.
_AZ_TIMEOUT = 90


async def _run_az(az_args: list[str], timeout: int = _AZ_TIMEOUT) -> tuple[int, str, str]:
    """
    Run an Azure CLI command asynchronously.

    Pass only the arguments after 'az' (e.g. ["datafactory", "pipeline", "list", ...]).
    The correct executable prefix for the current platform is prepended automatically.
    Returns (returncode, stdout, stderr). Never raises.

    Uses asyncio.create_subprocess_exec so the event loop is not blocked and
    the subprocess is properly killed if the timeout fires.
    """
    cmd = [*_az_exe(), *az_args]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=os.environ.copy(),
        )
        try:
            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                proc.communicate(),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            try:
                proc.kill()
                await proc.wait()
            except Exception:
                pass
            return -1, "", (
                f"az command timed out after {timeout}s. "
                "The operation may have still completed in Azure — verify in the portal."
            )
        return proc.returncode, stdout_bytes.decode(errors="replace").strip(), stderr_bytes.decode(errors="replace").strip()
    except Exception as exc:
        return -1, "", str(exc)


# ── list / get helpers ────────────────────────────────────────────────────────

async def _list_adf_resources(az_subcommand: list[str], resource_label: str, tool_name: str) -> dict:
    """Generic list for pipelines, linked-services, or datasets."""
    if err := _check_config():
        return {**err, "tool": tool_name}

    cmd = [*az_subcommand, "list", *_base_flags(), "--output", "json", "--only-show-errors"]
    rc, stdout, stderr = await _run_az(cmd)
    if rc != 0:
        return {"success": False, "error": f"Failed to list {resource_label}.", "stderr": stderr, "tool": tool_name}

    try:
        items = json.loads(stdout) if stdout else []
    except json.JSONDecodeError:
        items = []

    names = [item.get("name", "") for item in items if isinstance(item, dict)]
    return {"success": True, "count": len(names), "names": names, "tool": tool_name}


async def _get_adf_resource(az_subcommand: list[str], name: str, resource_label: str, tool_name: str) -> dict:
    """Generic get (show) for a single pipeline, linked-service, or dataset."""
    if err := _check_config():
        return {**err, "tool": tool_name}
    if not name:
        return {"success": False, "error": f"{resource_label}_name is required.", "stderr": "", "tool": tool_name}

    cmd = [*az_subcommand, "show", *_base_flags(), "--name", name, "--output", "json", "--only-show-errors"]
    rc, stdout, stderr = await _run_az(cmd)
    if rc != 0:
        return {"success": False, "error": f"Failed to get {resource_label} '{name}'.", "stderr": stderr, "tool": tool_name}

    try:
        resource = json.loads(stdout) if stdout else {}
    except json.JSONDecodeError:
        resource = {"raw": stdout}

    return {"success": True, "name": name, "resource": resource, "tool": tool_name}


# ── Tool 1–3: list_* ──────────────────────────────────────────────────────────

async def _list_pipelines(_args: dict) -> dict:
    return await _list_adf_resources(["datafactory", "pipeline"], "pipelines", "list_pipelines")


async def _list_linked_services(_args: dict) -> dict:
    return await _list_adf_resources(["datafactory", "linked-service"], "linked services", "list_linked_services")


async def _list_datasets(_args: dict) -> dict:
    return await _list_adf_resources(["datafactory", "dataset"], "datasets", "list_datasets")


# ── Tool 4–6: get_* ───────────────────────────────────────────────────────────

async def _get_pipeline(args: dict) -> dict:
    return await _get_adf_resource(
        ["datafactory", "pipeline"], args.get("pipeline_name", "").strip(), "pipeline", "get_pipeline"
    )


async def _get_linked_service(args: dict) -> dict:
    return await _get_adf_resource(
        ["datafactory", "linked-service"], args.get("linked_service_name", "").strip(), "linked_service", "get_linked_service"
    )


async def _get_dataset(args: dict) -> dict:
    return await _get_adf_resource(
        ["datafactory", "dataset"], args.get("dataset_name", "").strip(), "dataset", "get_dataset"
    )


# ── shared resource helpers ───────────────────────────────────────────────────

def _resolve_resource_json(definition: str, field_name: str) -> tuple[str | None, dict | None]:
    """
    Resolve a resource definition argument to the value for an az --<resource> flag.

    Accepts either:
      - An inline JSON string starting with '{'
      - An absolute file path to a .json file (passed as @filepath to az)

    Returns (az_arg, None) on success, or (None, error_dict) on failure.
    """
    value = definition.strip()

    if value.startswith("{"):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            return None, {
                "success": False,
                "error": f"{field_name} is not valid JSON: {exc}",
                "stderr": "",
            }
        return json.dumps(parsed, separators=(",", ":")), None

    abs_path = os.path.abspath(value)
    if not os.path.isfile(abs_path):
        return None, {
            "success": False,
            "error": f"{field_name} file not found: {abs_path}",
            "stderr": "",
        }
    try:
        with open(abs_path, encoding="utf-8") as fh:
            json.load(fh)
    except json.JSONDecodeError as exc:
        return None, {
            "success": False,
            "error": f"{field_name} file is not valid JSON: {exc}",
            "stderr": "",
        }
    return f"@{abs_path}", None


async def _upsert_adf_resource(
    az_subcommand: list[str],
    resource_name: str,
    json_flag: str,
    az_arg: str,
    tool_name: str,
) -> dict:
    """
    Generic create-or-upsert for any ADF resource (pipeline, linked-service, dataset).

    az_subcommand : e.g. ["datafactory", "pipeline"]
    resource_name : display name for error messages
    json_flag     : CLI flag for the resource body, e.g. "--pipeline"
    az_arg        : the value for json_flag (compact JSON string or @filepath)
    tool_name     : included in error dicts for caller context
    """
    create_cmd = [
        *az_subcommand, "create",
        *_base_flags(),
        "--name", resource_name,
        json_flag, az_arg,
        "--output", "json",
        "--only-show-errors",
    ]

    rc, stdout, stderr = await _run_az(create_cmd)

    if rc == 0:
        try:
            obj = json.loads(stdout) if stdout else {}
        except json.JSONDecodeError:
            obj = {"raw": stdout}
        return {"success": True, "action": "created", "name": resource_name, "resource": obj, "tool": tool_name}

    if "alreadyexists" in stderr.lower() or "already exists" in stderr.lower():
        delete_cmd = [
            *az_subcommand, "delete",
            *_base_flags(),
            "--name", resource_name,
            "--yes",
            "--only-show-errors",
        ]
        del_rc, _, del_stderr = await _run_az(delete_cmd)
        if del_rc != 0:
            return {
                "success": False,
                "error": f"Failed to delete existing '{resource_name}' before update.",
                "stderr": del_stderr,
                "tool": tool_name,
            }

        rc2, stdout2, stderr2 = await _run_az(create_cmd)
        if rc2 != 0:
            return {
                "success": False,
                "error": f"Failed to re-create '{resource_name}' after delete.",
                "stderr": stderr2,
                "tool": tool_name,
            }
        try:
            obj = json.loads(stdout2) if stdout2 else {}
        except json.JSONDecodeError:
            obj = {"raw": stdout2}
        return {"success": True, "action": "updated", "name": resource_name, "resource": obj, "tool": tool_name}

    return {
        "success": False,
        "error": f"Failed to create '{resource_name}'.",
        "stderr": stderr,
        "tool": tool_name,
    }


# ── delete helper ────────────────────────────────────────────────────────────

async def _delete_adf_resource(
    az_subcommand: list[str],
    resource_name: str,
    resource_label: str,
    tool_name: str,
) -> dict:
    """Generic delete for any ADF resource (pipeline, linked-service, dataset)."""
    if err := _check_config():
        return {**err, "tool": tool_name}
    if not resource_name:
        return {"success": False, "error": f"{resource_label}_name is required.", "stderr": "", "tool": tool_name}

    cmd = [
        *az_subcommand, "delete",
        *_base_flags(),
        "--name", resource_name,
        "--yes",
        "--only-show-errors",
    ]
    rc, _, stderr = await _run_az(cmd)
    if rc != 0:
        return {
            "success": False,
            "error": f"Failed to delete {resource_label} '{resource_name}'.",
            "stderr": stderr,
            "tool": tool_name,
        }
    return {"success": True, "action": "deleted", "name": resource_name, "tool": tool_name}


async def _delete_pipeline(args: dict) -> dict:
    return await _delete_adf_resource(
        ["datafactory", "pipeline"], args.get("pipeline_name", "").strip(), "pipeline", "delete_pipeline"
    )


async def _delete_linked_service(args: dict) -> dict:
    return await _delete_adf_resource(
        ["datafactory", "linked-service"], args.get("linked_service_name", "").strip(), "linked_service", "delete_linked_service"
    )


async def _delete_dataset(args: dict) -> dict:
    return await _delete_adf_resource(
        ["datafactory", "dataset"], args.get("dataset_name", "").strip(), "dataset", "delete_dataset"
    )


# ── Tool 1: create_or_update_pipeline ────────────────────────────────────────

async def _create_or_update_pipeline(args: dict) -> dict:
    if err := _check_config():
        return {**err, "tool": "create_or_update_pipeline"}

    name: str = args.get("pipeline_name", "").strip()
    definition: str = args.get("pipeline_definition", "").strip()

    if not name:
        return {"success": False, "error": "pipeline_name is required.", "stderr": "", "tool": "create_or_update_pipeline"}
    if not definition:
        return {"success": False, "error": "pipeline_definition is required.", "stderr": "", "tool": "create_or_update_pipeline"}

    az_arg, err = _resolve_resource_json(definition, "pipeline_definition")
    if err:
        return {**err, "tool": "create_or_update_pipeline"}

    return await _upsert_adf_resource(["datafactory", "pipeline"], name, "--pipeline", az_arg, "create_or_update_pipeline")


# ── Tool 2: create_or_update_linked_service ───────────────────────────────────

async def _create_or_update_linked_service(args: dict) -> dict:
    if err := _check_config():
        return {**err, "tool": "create_or_update_linked_service"}

    name: str = args.get("linked_service_name", "").strip()
    definition: str = args.get("linked_service_definition", "").strip()

    if not name:
        return {"success": False, "error": "linked_service_name is required.", "stderr": "", "tool": "create_or_update_linked_service"}
    if not definition:
        return {"success": False, "error": "linked_service_definition is required.", "stderr": "", "tool": "create_or_update_linked_service"}

    az_arg, err = _resolve_resource_json(definition, "linked_service_definition")
    if err:
        return {**err, "tool": "create_or_update_linked_service"}

    return await _upsert_adf_resource(["datafactory", "linked-service"], name, "--linked-service", az_arg, "create_or_update_linked_service")


# ── Tool 3: create_or_update_dataset ─────────────────────────────────────────

async def _create_or_update_dataset(args: dict) -> dict:
    if err := _check_config():
        return {**err, "tool": "create_or_update_dataset"}

    name: str = args.get("dataset_name", "").strip()
    definition: str = args.get("dataset_definition", "").strip()

    if not name:
        return {"success": False, "error": "dataset_name is required.", "stderr": "", "tool": "create_or_update_dataset"}
    if not definition:
        return {"success": False, "error": "dataset_definition is required.", "stderr": "", "tool": "create_or_update_dataset"}

    az_arg, err = _resolve_resource_json(definition, "dataset_definition")
    if err:
        return {**err, "tool": "create_or_update_dataset"}

    return await _upsert_adf_resource(["datafactory", "dataset"], name, "--dataset", az_arg, "create_or_update_dataset")


# ── Tool 4: trigger_pipeline_run ─────────────────────────────────────────────

async def _trigger_pipeline_run(args: dict) -> dict:
    if err := _check_config():
        return {**err, "tool": "trigger_pipeline_run"}

    pipeline_name: str = args.get("pipeline_name", "").strip()
    parameters: dict | None = args.get("parameters")

    if not pipeline_name:
        return {"success": False, "error": "pipeline_name is required.", "stderr": "", "tool": "trigger_pipeline_run"}

    cmd = [
        "datafactory", "pipeline", "create-run",
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


# ── Tool 5: get_pipeline_run_status ──────────────────────────────────────────

async def _get_pipeline_run_status(args: dict) -> dict:
    if err := _check_config():
        return {**err, "tool": "get_pipeline_run_status"}

    run_id: str = args.get("run_id", "").strip()
    if not run_id:
        return {"success": False, "error": "run_id is required.", "stderr": "", "tool": "get_pipeline_run_status"}

    cmd = [
        "datafactory", "pipeline-run", "show",
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


# ── Tool 6: get_activity_run_logs ────────────────────────────────────────────

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
        "datafactory", "activity-run", "query-by-pipeline-run",
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
            name="list_pipelines",
            description="List the names of all pipelines that exist in the Azure Data Factory.",
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="list_linked_services",
            description="List the names of all linked services that exist in the Azure Data Factory.",
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="list_datasets",
            description="List the names of all datasets that exist in the Azure Data Factory.",
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="get_pipeline",
            description="Get the full JSON definition of a single ADF pipeline by name.",
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline_name": {"type": "string", "description": "Name of the pipeline to retrieve."},
                },
                "required": ["pipeline_name"],
            },
        ),
        types.Tool(
            name="get_linked_service",
            description="Get the full JSON definition of a single ADF linked service by name.",
            inputSchema={
                "type": "object",
                "properties": {
                    "linked_service_name": {"type": "string", "description": "Name of the linked service to retrieve."},
                },
                "required": ["linked_service_name"],
            },
        ),
        types.Tool(
            name="get_dataset",
            description="Get the full JSON definition of a single ADF dataset by name.",
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset_name": {"type": "string", "description": "Name of the dataset to retrieve."},
                },
                "required": ["dataset_name"],
            },
        ),
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
            name="create_or_update_linked_service",
            description=(
                "Create or update an Azure Data Factory linked service. "
                "Pass the linked service definition as an inline JSON string or an absolute file path. "
                "If the linked service already exists it will be replaced (delete + create)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "linked_service_name": {
                        "type": "string",
                        "description": "Name of the ADF linked service to create or update.",
                    },
                    "linked_service_definition": {
                        "type": "string",
                        "description": (
                            "Linked service JSON body as an inline string starting with '{', "
                            "or an absolute file path to a .json file."
                        ),
                    },
                },
                "required": ["linked_service_name", "linked_service_definition"],
            },
        ),
        types.Tool(
            name="create_or_update_dataset",
            description=(
                "Create or update an Azure Data Factory dataset. "
                "Pass the dataset definition as an inline JSON string or an absolute file path. "
                "If the dataset already exists it will be replaced (delete + create)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset_name": {
                        "type": "string",
                        "description": "Name of the ADF dataset to create or update.",
                    },
                    "dataset_definition": {
                        "type": "string",
                        "description": (
                            "Dataset JSON body as an inline string starting with '{', "
                            "or an absolute file path to a .json file."
                        ),
                    },
                },
                "required": ["dataset_name", "dataset_definition"],
            },
        ),
        types.Tool(
            name="delete_pipeline",
            description="Permanently delete an ADF pipeline by name. This action cannot be undone.",
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline_name": {"type": "string", "description": "Name of the pipeline to delete."},
                },
                "required": ["pipeline_name"],
            },
        ),
        types.Tool(
            name="delete_linked_service",
            description="Permanently delete an ADF linked service by name. This action cannot be undone.",
            inputSchema={
                "type": "object",
                "properties": {
                    "linked_service_name": {"type": "string", "description": "Name of the linked service to delete."},
                },
                "required": ["linked_service_name"],
            },
        ),
        types.Tool(
            name="delete_dataset",
            description="Permanently delete an ADF dataset by name. This action cannot be undone.",
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset_name": {"type": "string", "description": "Name of the dataset to delete."},
                },
                "required": ["dataset_name"],
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
        "list_pipelines": _list_pipelines,
        "list_linked_services": _list_linked_services,
        "list_datasets": _list_datasets,
        "get_pipeline": _get_pipeline,
        "get_linked_service": _get_linked_service,
        "get_dataset": _get_dataset,
        "create_or_update_pipeline": _create_or_update_pipeline,
        "create_or_update_linked_service": _create_or_update_linked_service,
        "create_or_update_dataset": _create_or_update_dataset,
        "delete_pipeline": _delete_pipeline,
        "delete_linked_service": _delete_linked_service,
        "delete_dataset": _delete_dataset,
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
    rc, _, _ = await _run_az(["datafactory", "--help"])
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
