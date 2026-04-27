"""
adf_mcp_server.py — MCP server exposing Azure Data Factory tools via the Azure Python SDK.

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
  list_dataflows                 — list all data flows in the factory
  create_or_update_dataflow      — create or update a data flow definition
  delete_dataflow                — delete a data flow by name
  trigger_pipeline_run           — start a pipeline run and return the run ID
  get_pipeline_run_status        — check the status of a run by run ID
  get_activity_run_logs          — fetch per-activity logs for a run

Authentication: DefaultAzureCredential (includes az login, env vars, managed identity).
ADF identity:   ADF_SUBSCRIPTION_ID, ADF_RESOURCE_GROUP, ADF_FACTORY_NAME env vars.
"""

import asyncio
import functools
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import mcp.types as types
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import RunFilterParameters
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
        }
    return None


# ── SDK client (lazy singleton) ───────────────────────────────────────────────

_adf_client: DataFactoryManagementClient | None = None


def _get_client() -> DataFactoryManagementClient:
    """Return the shared ADF management client, creating it on first call."""
    global _adf_client
    if _adf_client is None:
        _adf_client = DataFactoryManagementClient(
            DefaultAzureCredential(), ADF_SUBSCRIPTION_ID
        )
    return _adf_client


# ── Async bridge ──────────────────────────────────────────────────────────────

async def _run_sdk(fn, *args, **kwargs):
    """Run a synchronous SDK call in a thread pool so the event loop is never blocked."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, functools.partial(fn, *args, **kwargs))


# ── Resource definition helper ────────────────────────────────────────────────

def _resolve_resource_json(definition: str, field_name: str) -> tuple[dict | None, dict | None]:
    """
    Parse a resource definition argument into a dict.

    Accepts either:
      - An inline JSON string starting with '{'
      - A file path to a .json file

    Returns (body_dict, None) on success, or (None, error_dict) on failure.
    """
    value = definition.strip()
    if value.startswith("{"):
        try:
            return json.loads(value), None
        except json.JSONDecodeError as exc:
            return None, {"success": False, "error": f"{field_name} is not valid JSON: {exc}"}

    abs_path = os.path.abspath(value)
    if not os.path.isfile(abs_path):
        return None, {"success": False, "error": f"{field_name} file not found: {abs_path}"}
    try:
        with open(abs_path, encoding="utf-8") as fh:
            return json.load(fh), None
    except json.JSONDecodeError as exc:
        return None, {"success": False, "error": f"{field_name} file is not valid JSON: {exc}"}


def _sdk_error(exc: HttpResponseError) -> str:
    """Extract a readable message from an HttpResponseError."""
    return str(exc.message) if exc.message else str(exc)


def _fmt_utc(dt: datetime | None) -> str | None:
    """Format a datetime as an ISO 8601 string with an explicit UTC offset (+00:00)."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


_TERMINAL_STATUSES = {"Succeeded", "Failed", "Cancelled", "TimedOut"}


# ── list / get helpers ────────────────────────────────────────────────────────

async def _list_adf_resources(list_fn, resource_label: str, tool_name: str) -> dict:
    """Generic list for pipelines, linked-services, datasets, or data flows."""
    if err := _check_config():
        return {**err, "tool": tool_name}
    try:
        names = await _run_sdk(
            lambda: [item.name for item in list_fn(ADF_RESOURCE_GROUP, ADF_FACTORY_NAME) if item.name]
        )
        return {"success": True, "count": len(names), "names": names, "tool": tool_name}
    except HttpResponseError as exc:
        return {"success": False, "error": f"Failed to list {resource_label}: {_sdk_error(exc)}", "tool": tool_name}


async def _get_adf_resource(get_fn, name: str, name_field: str, resource_label: str, tool_name: str) -> dict:
    """Generic get for a single pipeline, linked-service, or dataset."""
    if err := _check_config():
        return {**err, "tool": tool_name}
    if not name:
        return {"success": False, "error": f"{name_field} is required.", "tool": tool_name}
    try:
        resource = await _run_sdk(get_fn, ADF_RESOURCE_GROUP, ADF_FACTORY_NAME, name)
        return {"success": True, "name": name, "resource": resource.as_dict(), "tool": tool_name}
    except ResourceNotFoundError:
        return {"success": False, "error": f"{resource_label} '{name}' not found.", "tool": tool_name}
    except HttpResponseError as exc:
        return {"success": False, "error": f"Failed to get {resource_label} '{name}': {_sdk_error(exc)}", "tool": tool_name}


async def _upsert_adf_resource(upsert_fn, name: str, body: dict, resource_label: str, tool_name: str) -> dict:
    """Generic create-or-update for any ADF resource."""
    try:
        result = await _run_sdk(upsert_fn, ADF_RESOURCE_GROUP, ADF_FACTORY_NAME, name, body)
        return {"success": True, "action": "upserted", "name": name, "resource": result.as_dict(), "tool": tool_name}
    except HttpResponseError as exc:
        return {"success": False, "error": f"Failed to create or update {resource_label} '{name}': {_sdk_error(exc)}", "tool": tool_name}


async def _delete_adf_resource(delete_fn, name: str, name_field: str, resource_label: str, tool_name: str) -> dict:
    """Generic delete for any ADF resource."""
    if err := _check_config():
        return {**err, "tool": tool_name}
    if not name:
        return {"success": False, "error": f"{name_field} is required.", "tool": tool_name}
    try:
        await _run_sdk(delete_fn, ADF_RESOURCE_GROUP, ADF_FACTORY_NAME, name)
        return {"success": True, "action": "deleted", "name": name, "tool": tool_name}
    except ResourceNotFoundError:
        return {"success": False, "error": f"{resource_label} '{name}' not found.", "tool": tool_name}
    except HttpResponseError as exc:
        return {"success": False, "error": f"Failed to delete {resource_label} '{name}': {_sdk_error(exc)}", "tool": tool_name}


# ── Tool 1–3: list_* ──────────────────────────────────────────────────────────

async def _list_pipelines(_args: dict) -> dict:
    return await _list_adf_resources(_get_client().pipelines.list_by_factory, "pipelines", "list_pipelines")


async def _list_linked_services(_args: dict) -> dict:
    return await _list_adf_resources(_get_client().linked_services.list_by_factory, "linked services", "list_linked_services")


async def _list_datasets(_args: dict) -> dict:
    return await _list_adf_resources(_get_client().datasets.list_by_factory, "datasets", "list_datasets")


# ── Tool 4–6: get_* ───────────────────────────────────────────────────────────

async def _get_pipeline(args: dict) -> dict:
    return await _get_adf_resource(
        _get_client().pipelines.get,
        args.get("pipeline_name", "").strip(),
        "pipeline_name", "pipeline", "get_pipeline",
    )


async def _get_linked_service(args: dict) -> dict:
    return await _get_adf_resource(
        _get_client().linked_services.get,
        args.get("linked_service_name", "").strip(),
        "linked_service_name", "linked service", "get_linked_service",
    )


async def _get_dataset(args: dict) -> dict:
    return await _get_adf_resource(
        _get_client().datasets.get,
        args.get("dataset_name", "").strip(),
        "dataset_name", "dataset", "get_dataset",
    )


# ── Tool 7–10: create_or_update_* ────────────────────────────────────────────

async def _create_or_update_pipeline(args: dict) -> dict:
    if err := _check_config():
        return {**err, "tool": "create_or_update_pipeline"}
    name = args.get("pipeline_name", "").strip()
    definition = args.get("pipeline_definition", "").strip()
    if not name:
        return {"success": False, "error": "pipeline_name is required.", "tool": "create_or_update_pipeline"}
    if not definition:
        return {"success": False, "error": "pipeline_definition is required.", "tool": "create_or_update_pipeline"}
    body, err = _resolve_resource_json(definition, "pipeline_definition")
    if err:
        return {**err, "tool": "create_or_update_pipeline"}
    return await _upsert_adf_resource(_get_client().pipelines.create_or_update, name, body, "pipeline", "create_or_update_pipeline")


async def _create_or_update_linked_service(args: dict) -> dict:
    if err := _check_config():
        return {**err, "tool": "create_or_update_linked_service"}
    name = args.get("linked_service_name", "").strip()
    definition = args.get("linked_service_definition", "").strip()
    if not name:
        return {"success": False, "error": "linked_service_name is required.", "tool": "create_or_update_linked_service"}
    if not definition:
        return {"success": False, "error": "linked_service_definition is required.", "tool": "create_or_update_linked_service"}
    body, err = _resolve_resource_json(definition, "linked_service_definition")
    if err:
        return {**err, "tool": "create_or_update_linked_service"}
    return await _upsert_adf_resource(_get_client().linked_services.create_or_update, name, body, "linked service", "create_or_update_linked_service")


async def _create_or_update_dataset(args: dict) -> dict:
    if err := _check_config():
        return {**err, "tool": "create_or_update_dataset"}
    name = args.get("dataset_name", "").strip()
    definition = args.get("dataset_definition", "").strip()
    if not name:
        return {"success": False, "error": "dataset_name is required.", "tool": "create_or_update_dataset"}
    if not definition:
        return {"success": False, "error": "dataset_definition is required.", "tool": "create_or_update_dataset"}
    body, err = _resolve_resource_json(definition, "dataset_definition")
    if err:
        return {**err, "tool": "create_or_update_dataset"}
    return await _upsert_adf_resource(_get_client().datasets.create_or_update, name, body, "dataset", "create_or_update_dataset")


# ── Tool 11: list_dataflows ───────────────────────────────────────────────────

async def _list_dataflows(_args: dict) -> dict:
    return await _list_adf_resources(_get_client().data_flows.list_by_factory, "data flows", "list_dataflows")


# ── Tool 12: get_dataflow ────────────────────────────────────────────────────

async def _get_dataflow(args: dict) -> dict:
    return await _get_adf_resource(
        _get_client().data_flows.get,
        args.get("dataflow_name", "").strip(),
        "dataflow_name", "data flow", "get_dataflow",
    )


# ── Tool 13: create_or_update_dataflow ───────────────────────────────────────

async def _create_or_update_dataflow(args: dict) -> dict:
    if err := _check_config():
        return {**err, "tool": "create_or_update_dataflow"}

    name = args.get("dataflow_name", "").strip()
    definition = args.get("dataflow_definition", "").strip()
    flow_type = args.get("flow_type", "MappingDataFlow").strip()

    if not name:
        return {"success": False, "error": "dataflow_name is required.", "tool": "create_or_update_dataflow"}
    if not definition:
        return {"success": False, "error": "dataflow_definition is required.", "tool": "create_or_update_dataflow"}
    if flow_type not in ("MappingDataFlow", "Flowlet"):
        return {"success": False, "error": "flow_type must be 'MappingDataFlow' or 'Flowlet'.", "tool": "create_or_update_dataflow"}

    body, err = _resolve_resource_json(definition, "dataflow_definition")
    if err:
        return {**err, "tool": "create_or_update_dataflow"}

    # Inject flow_type into the properties body if the caller omitted it.
    props = body.get("properties", body)
    if "type" not in props:
        props["type"] = flow_type

    return await _upsert_adf_resource(_get_client().data_flows.create_or_update, name, body, "data flow", "create_or_update_dataflow")


# ── Tool 13–16: delete_* ─────────────────────────────────────────────────────

async def _delete_pipeline(args: dict) -> dict:
    return await _delete_adf_resource(
        _get_client().pipelines.delete,
        args.get("pipeline_name", "").strip(),
        "pipeline_name", "pipeline", "delete_pipeline",
    )


async def _delete_linked_service(args: dict) -> dict:
    return await _delete_adf_resource(
        _get_client().linked_services.delete,
        args.get("linked_service_name", "").strip(),
        "linked_service_name", "linked service", "delete_linked_service",
    )


async def _delete_dataset(args: dict) -> dict:
    return await _delete_adf_resource(
        _get_client().datasets.delete,
        args.get("dataset_name", "").strip(),
        "dataset_name", "dataset", "delete_dataset",
    )


async def _delete_dataflow(args: dict) -> dict:
    return await _delete_adf_resource(
        _get_client().data_flows.delete,
        args.get("dataflow_name", "").strip(),
        "dataflow_name", "data flow", "delete_dataflow",
    )


# ── Tool 17: trigger_pipeline_run ────────────────────────────────────────────

async def _trigger_pipeline_run(args: dict) -> dict:
    if err := _check_config():
        return {**err, "tool": "trigger_pipeline_run"}

    pipeline_name = args.get("pipeline_name", "").strip()
    parameters: dict | None = args.get("parameters")

    if not pipeline_name:
        return {"success": False, "error": "pipeline_name is required.", "tool": "trigger_pipeline_run"}

    try:
        response = await _run_sdk(
            _get_client().pipelines.create_run,
            ADF_RESOURCE_GROUP,
            ADF_FACTORY_NAME,
            pipeline_name,
            parameters=parameters,
        )
        return {"success": True, "run_id": response.run_id, "pipeline_name": pipeline_name, "tool": "trigger_pipeline_run"}
    except ResourceNotFoundError:
        return {"success": False, "error": f"Pipeline '{pipeline_name}' not found.", "tool": "trigger_pipeline_run"}
    except HttpResponseError as exc:
        return {"success": False, "error": f"Failed to trigger pipeline run for '{pipeline_name}': {_sdk_error(exc)}", "tool": "trigger_pipeline_run"}


# ── Tool 18: get_pipeline_run_status ─────────────────────────────────────────

async def _fetch_run_status(run_id: str) -> dict:
    """Single SDK call returning the current pipeline run status."""
    try:
        run = await _run_sdk(
            _get_client().pipeline_runs.get,
            ADF_RESOURCE_GROUP,
            ADF_FACTORY_NAME,
            run_id,
        )
        return {
            "success": True,
            "run_id": run_id,
            "pipeline_name": run.pipeline_name or "",
            "status": run.status or "",
            "run_start": _fmt_utc(run.run_start),
            "run_end": _fmt_utc(run.run_end),
            "duration_ms": run.duration_in_ms,
            "message": run.message or "",
            "run_group_id": run.run_group_id or "",
            "tool": "get_pipeline_run_status",
        }
    except ResourceNotFoundError:
        return {"success": False, "error": f"Pipeline run '{run_id}' not found.", "tool": "get_pipeline_run_status"}
    except HttpResponseError as exc:
        return {"success": False, "error": f"Failed to retrieve pipeline run status for '{run_id}': {_sdk_error(exc)}", "tool": "get_pipeline_run_status"}


async def _get_pipeline_run_status(args: dict) -> dict:
    if err := _check_config():
        return {**err, "tool": "get_pipeline_run_status"}

    run_id = args.get("run_id", "").strip()
    if not run_id:
        return {"success": False, "error": "run_id is required.", "tool": "get_pipeline_run_status"}

    wait = bool(args.get("wait_for_completion", False))
    poll_interval = max(5, min(int(args.get("poll_interval_seconds", 15)), 60))
    timeout = max(30, min(int(args.get("timeout_seconds", 300)), 3600))

    result = await _fetch_run_status(run_id)
    if not wait or not result.get("success"):
        return result

    deadline = time.monotonic() + timeout
    while result["status"] not in _TERMINAL_STATUSES:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            result["timed_out"] = True
            result["note"] = f"Polling stopped after {timeout}s; run is still '{result['status']}'."
            break
        await asyncio.sleep(min(poll_interval, remaining))
        new_result = await _fetch_run_status(run_id)
        if not new_result.get("success"):
            return new_result
        result = new_result

    return result


# ── Tool 19: get_activity_run_logs ───────────────────────────────────────────

def _parse_dt(value: str | None, fallback: datetime) -> datetime:
    """Parse an ISO 8601 string to a datetime, falling back to a default."""
    if not value:
        return fallback
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return fallback


async def _get_activity_run_logs(args: dict) -> dict:
    if err := _check_config():
        return {**err, "tool": "get_activity_run_logs"}

    run_id = args.get("run_id", "").strip()
    if not run_id:
        return {"success": False, "error": "run_id is required.", "tool": "get_activity_run_logs"}

    last_updated_after: str | None = args.get("last_updated_after")
    last_updated_before: str | None = args.get("last_updated_before")

    # Derive time window from the pipeline run if not supplied by caller.
    if not last_updated_after or not last_updated_before:
        status_result = await _get_pipeline_run_status({"run_id": run_id})
        if not status_result.get("success"):
            return {**status_result, "tool": "get_activity_run_logs"}

        pipeline_status = status_result.get("status", "")
        run_start = status_result.get("run_start")
        run_end = status_result.get("run_end")

        if pipeline_status == "Queued" or not run_start:
            return {
                "success": True,
                "run_id": run_id,
                "activity_count": 0,
                "activities": [],
                "note": f"Pipeline run is '{pipeline_status}' and has not started yet. No activity logs available.",
                "tool": "get_activity_run_logs",
            }

        if not last_updated_after:
            last_updated_after = run_start
        if not last_updated_before:
            last_updated_before = run_end or datetime.now(timezone.utc).isoformat()

    now = datetime.now(timezone.utc)
    filter_params = RunFilterParameters(
        last_updated_after=_parse_dt(last_updated_after, now),
        last_updated_before=_parse_dt(last_updated_before, now),
    )

    try:
        response = await _run_sdk(
            _get_client().activity_runs.query_by_pipeline_run,
            ADF_RESOURCE_GROUP,
            ADF_FACTORY_NAME,
            run_id,
            filter_params,
        )
    except HttpResponseError as exc:
        return {"success": False, "error": f"Failed to retrieve activity logs for run '{run_id}': {_sdk_error(exc)}", "tool": "get_activity_run_logs"}

    activities = []
    for act in (response.value or []):
        error_info = act.error
        # Suppress empty error objects.
        if isinstance(error_info, dict) and not any(error_info.values()):
            error_info = None
        elif hasattr(error_info, "as_dict"):
            error_dict = error_info.as_dict()
            error_info = error_dict if any(error_dict.values()) else None

        activities.append({
            "activity_name": act.activity_name or "",
            "activity_type": act.activity_type or "",
            "status": act.status or "",
            "run_start": _fmt_utc(act.activity_run_start),
            "run_end": _fmt_utc(act.activity_run_end),
            "duration_ms": act.duration_in_ms,
            "error": error_info,
            "output": act.output,
        })

    return {
        "success": True,
        "run_id": run_id,
        "activity_count": len(activities),
        "activities": activities,
        "tool": "get_activity_run_logs",
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
                "Pass the pipeline definition as an inline JSON string or an absolute file path."
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
                "Pass the linked service definition as an inline JSON string or an absolute file path."
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
                "Pass the dataset definition as an inline JSON string or an absolute file path."
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
            name="list_dataflows",
            description="List the names of all data flows that exist in the Azure Data Factory.",
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="get_dataflow",
            description="Get the full JSON definition of a single ADF data flow by name.",
            inputSchema={
                "type": "object",
                "properties": {
                    "dataflow_name": {"type": "string", "description": "Name of the data flow to retrieve."},
                },
                "required": ["dataflow_name"],
            },
        ),
        types.Tool(
            name="create_or_update_dataflow",
            description=(
                "Create or update an Azure Data Factory data flow. "
                "Pass the data flow properties as an inline JSON string or an absolute file path."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "dataflow_name": {
                        "type": "string",
                        "description": "Name of the ADF data flow to create or update.",
                    },
                    "dataflow_definition": {
                        "type": "string",
                        "description": (
                            "Data flow properties JSON as an inline string starting with '{', "
                            "or an absolute file path to a .json file."
                        ),
                    },
                    "flow_type": {
                        "type": "string",
                        "enum": ["MappingDataFlow", "Flowlet"],
                        "description": "Type of data flow. Used as a default if not already present in the definition JSON. Defaults to 'MappingDataFlow'.",
                    },
                },
                "required": ["dataflow_name", "dataflow_definition"],
            },
        ),
        types.Tool(
            name="delete_dataflow",
            description="Permanently delete an ADF data flow by name. This action cannot be undone.",
            inputSchema={
                "type": "object",
                "properties": {
                    "dataflow_name": {"type": "string", "description": "Name of the data flow to delete."},
                },
                "required": ["dataflow_name"],
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
                "start/end timestamps (UTC, ISO 8601 with +00:00 offset), duration, and any error message. "
                "Set wait_for_completion=true to poll internally until the run finishes — "
                "this avoids the need to call this tool repeatedly."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "run_id": {
                        "type": "string",
                        "description": "The pipeline run ID returned by trigger_pipeline_run.",
                    },
                    "wait_for_completion": {
                        "type": "boolean",
                        "description": (
                            "If true, poll until the run reaches a terminal state "
                            "(Succeeded/Failed/Cancelled/TimedOut) before returning. "
                            "Defaults to false."
                        ),
                    },
                    "poll_interval_seconds": {
                        "type": "integer",
                        "description": "Seconds between status checks when wait_for_completion is true. Clamped to [5, 60]. Defaults to 15.",
                    },
                    "timeout_seconds": {
                        "type": "integer",
                        "description": "Maximum seconds to wait when wait_for_completion is true. Clamped to [30, 3600]. Defaults to 300.",
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
        "list_dataflows": _list_dataflows,
        "get_dataflow": _get_dataflow,
        "create_or_update_dataflow": _create_or_update_dataflow,
        "delete_dataflow": _delete_dataflow,
        "trigger_pipeline_run": _trigger_pipeline_run,
        "get_pipeline_run_status": _get_pipeline_run_status,
        "get_activity_run_logs": _get_activity_run_logs,
    }

    handler = dispatch.get(name)
    if handler is None:
        result = {
            "success": False,
            "error": f"Unknown tool: '{name}'. Available tools: {list(dispatch)}",
            "tool": name,
        }
    else:
        result = await handler(arguments or {})

    return [types.TextContent(type="text", text=json.dumps(result, indent=2))]


# ── Entrypoint ────────────────────────────────────────────────────────────────

async def main() -> None:
    logger.info("Starting ADF MCP server (stdio transport).")
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


if __name__ == "__main__":
    asyncio.run(main())
