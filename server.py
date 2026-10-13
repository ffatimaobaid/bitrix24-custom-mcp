"""
Bitrix24 Product Catalog & CRM MCP Server
===========================================
Remote, read-only FastMCP server that exposes Bitrix24 product catalog and CRM data
via streamable HTTP transport for use as a Claude custom connector.

Endpoint: POST/GET /mcp/<MCP_SECRET_PATH>
Health:   GET     /health
All other paths -> 404.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Union

from dotenv import load_dotenv

# Load .env file if present
load_dotenv(Path(__file__).parent / ".env")
load_dotenv()

import httpx
from fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

# ---------------------------------------------------------------------------
# Configuration & Security Flags
# ---------------------------------------------------------------------------
BITRIX_WEBHOOK_URL: str = os.environ.get("BITRIX_WEBHOOK_URL", "")
if not BITRIX_WEBHOOK_URL:
    raise RuntimeError("BITRIX_WEBHOOK_URL is required. Set it in .env or environment variables.")

MCP_SECRET_PATH: str = os.environ.get("MCP_SECRET_PATH", "")
if not MCP_SECRET_PATH:
    raise RuntimeError("MCP_SECRET_PATH is required. Set it in .env or environment variables.")

PORT: int = int(os.environ.get("PORT", "8000"))
ENABLE_WRITES: bool = os.environ.get("ENABLE_WRITES", "false").lower() in ("1", "true", "yes")

BITRIX_WRITE_WEBHOOK_URL: str = os.environ.get("BITRIX_WRITE_WEBHOOK_URL", "")
if ENABLE_WRITES and not BITRIX_WRITE_WEBHOOK_URL:
    raise RuntimeError(
        "BITRIX_WRITE_WEBHOOK_URL is required when ENABLE_WRITES=true. Set it in .env or environment variables."
    )

BITRIX_BASE = BITRIX_WEBHOOK_URL.rstrip("/")
BITRIX_WRITE_BASE = BITRIX_WRITE_WEBHOOK_URL.rstrip("/") if BITRIX_WRITE_WEBHOOK_URL else ""

# ---------------------------------------------------------------------------
# Logging - never emit webhook code or secret
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bitrix24-mcp")

# Stop httpx and httpcore loggers from printing request URLs (prevents logging webhook tokens)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

_REDACT_RE = re.compile(r"(rest/\d+/)[^/]+/", re.IGNORECASE)


def _safe_url(url: str) -> str:
    return _REDACT_RE.sub(r"\1***/", url)


# ---------------------------------------------------------------------------
# Method Whitelist & Read-Only Enforcer
# ---------------------------------------------------------------------------
_DISALLOWED_PATTERNS = (
    ".add", ".update", ".delete", ".set", ".import", ".batch",
    ".install", ".uninstall", ".bind", ".unbind", ".attach",
    ".detach", ".modify", ".deletebyfilter", ".quickorder",
)

_ALLOWED_READ_METHODS = {
    # Catalog
    "catalog.catalog.list", "catalog.catalog.get",
    "catalog.section.list", "catalog.section.get",
    "catalog.product.list", "catalog.product.get",
    "catalog.product.sku.list", "catalog.price.list",
    "catalog.storeproduct.list", "crm.productsection.list",
    # CRM Core Items
    "crm.item.fields", "crm.item.list", "crm.item.get",
    "crm.item.productrow.list",
    # CRM Entity-specific Read Methods
    "crm.deal.productrows.get", "crm.deal.contact.items.get",
    "crm.contact.company.items.get", "crm.company.contact.items.get",
    "crm.activity.list", "crm.activity.get", "crm.activity.communication.list",
    "crm.timeline.comment.list", "crm.timeline.comment.get",
    "crm.status.list", "crm.category.list", "crm.dealcategory.list", "crm.enum.ownertype",
    "crm.type.list", "crm.type.get", "crm.type.fields",
    "crm.duplicate.findbycomm", "crm.stagehistory.list",
    "crm.deal.list", "crm.deal.get", "crm.deal.fields",
    "crm.lead.list", "crm.lead.get", "crm.lead.fields",
    "crm.contact.list", "crm.contact.get", "crm.contact.fields",
    "crm.company.list", "crm.company.get", "crm.company.fields",
    "crm.quote.list", "crm.quote.get", "crm.quote.fields",
    "user.get", "user.current", "profile",
}


def _validate_read_method(method: str) -> None:
    method_lower = method.lower().strip()
    
    # Always block write signatures on read client
    for bad in _DISALLOWED_PATTERNS:
        if bad in method_lower:
            raise PermissionError(
                f"Method '{method}' is not permitted on the read client. Write operations are blocked."
            )

    if method_lower in _ALLOWED_READ_METHODS:
        return

    # Check safe read suffixes
    if any(method_lower.endswith(sfx) for sfx in (".list", ".get", ".fields", ".findbycomm", ".ownertype")):
        return

    raise PermissionError(
        f"Method '{method}' is not permitted on this read-only client."
    )


# ---------------------------------------------------------------------------
# Write Method Allowlist & Enforcer
# ---------------------------------------------------------------------------
_ALLOWED_WRITE_METHODS = {
    "crm.timeline.comment.add",
    "crm.activity.todo.add",
    "crm.item.update",
    "crm.item.add",
}


def _validate_write_method(method: str) -> None:
    if not ENABLE_WRITES:
        raise PermissionError(
            "Write operations are disabled. Set ENABLE_WRITES=true and configure BITRIX_WRITE_WEBHOOK_URL."
        )
    if not BITRIX_WRITE_BASE:
        raise RuntimeError("BITRIX_WRITE_WEBHOOK_URL is required for write operations.")

    m = method.lower().strip()
    if m not in _ALLOWED_WRITE_METHODS:
        raise PermissionError(
            f"Write method '{method}' is not permitted. Only {sorted(_ALLOWED_WRITE_METHODS)} are allowed. "
            "Deletions, imports, bulk operations, and portal changes are strictly prohibited."
        )


async def _bx_write_call(
    client: httpx.AsyncClient,
    method: str,
    params: dict[str, Any],
    *,
    retry: int = 0,
) -> dict[str, Any]:
    _validate_write_method(method)
    url = f"{BITRIX_WRITE_BASE}/{method}.json"
    log.info("Bitrix24 WRITE -> %s", method)

    try:
        r = await client.post(url, json=params, timeout=30)
    except httpx.RequestError as exc:
        raise RuntimeError(f"Network error calling write method {method}: {exc}") from exc

    if r.status_code in _RETRY_STATUSES and retry < _MAX_RETRIES:
        wait = 2 ** retry
        log.warning("HTTP %s on write %s, retrying in %ss", r.status_code, method, wait)
        await asyncio.sleep(wait)
        return await _bx_write_call(client, method, params, retry=retry + 1)

    if r.status_code == 429 and retry >= _MAX_RETRIES:
        raise RuntimeError("Bitrix24 write rate limit exceeded after retries.")

    try:
        body = r.json()
    except Exception:
        raise RuntimeError(f"Bitrix24 write non-JSON response (HTTP {r.status_code})")

    if "error" in body:
        raise RuntimeError(f"Bitrix24 [{body.get('error')}]: {body.get('error_description', '')}")

    return body


def _audit_write(tool_name: str, entity_type: Any, record_id: Any, field_names: list[str]) -> None:
    """Log every write action to the server log without logging sensitive field values."""
    now_iso = datetime.now().isoformat()
    log.info(
        "AUDIT WRITE: tool=%s | entity_type=%s | record_id=%s | fields_modified=%s | timestamp=%s",
        tool_name,
        entity_type,
        record_id,
        sorted(list(field_names)),
        now_iso,
    )


# ---------------------------------------------------------------------------
# Schema Cache & System Field Protection
# ---------------------------------------------------------------------------
_FIELDS_CACHE: dict[int, dict[str, Any]] = {}

_SYSTEM_PROTECTED_FIELDS = {
    # System IDs & Timestamps
    "id", "ID",
    "createdTime", "created_time", "DATE_CREATE", "dateCreate", "dateCreateShort",
    "updatedTime", "updated_time", "DATE_MODIFY", "dateModify", "dateModifyShort",
    "createdBy", "created_by", "CREATED_BY_ID",
    "updatedBy", "updated_by", "MODIFY_BY_ID",
    "movedTime", "moved_time",
    "movedBy", "moved_by",
    "lastActivityTime", "last_activity_time",
    "lastActivityBy", "last_activity_by",
    "lastCommunicationTime", "lastCommunicationCallTime", "lastCommunicationEmailTime",
    "lastCommunicationImolTime", "lastCommunicationWebformTime",
    "webformId", "webform_id",
    "stageSemanticId", "isReturnCustomer", "dateClosed",
    "searchContent",
}


async def _get_entity_fields(client: httpx.AsyncClient, entity_type_id: int) -> dict[str, Any]:
    if entity_type_id in _FIELDS_CACHE:
        return _FIELDS_CACHE[entity_type_id]
    body = await _bx_call(client, "crm.item.fields", {"entityTypeId": entity_type_id})
    fields = body.get("result", {}).get("fields", {})
    if fields:
        _FIELDS_CACHE[entity_type_id] = fields
    return fields


_COMMUNICATION_FIELDS = {
    "phone", "email", "web", "im",
    "phonemobile", "phonework", "phonehome", "phonemailing",
    "emailhome", "emailwork", "emailmailing",
    "fm",
}


def _validate_fields_for_write(
    proposed_fields: dict[str, Any],
    schema_fields: dict[str, Any],
    entity_type_id: int,
) -> None:
    if not isinstance(proposed_fields, dict):
        raise ValueError("Fields must be a dictionary of field names and values.")
    if not proposed_fields:
        raise ValueError("Fields dictionary cannot be empty.")

    lower_schema = {k.lower(): (k, v) for k, v in schema_fields.items()} if schema_fields else {}

    for k in proposed_fields.keys():
        kl = k.lower()
        if k in _SYSTEM_PROTECTED_FIELDS or kl in {f.lower() for f in _SYSTEM_PROTECTED_FIELDS}:
            raise ValueError(f"Modification of protected system field '{k}' is prohibited.")

        # Universal CRM communication fields (phone, email, fm) are valid across entities
        if kl in _COMMUNICATION_FIELDS:
            continue

        if schema_fields:
            if k not in schema_fields and kl not in lower_schema:
                raise ValueError(
                    f"Unknown field '{k}' for entity type {entity_type_id}. "
                    "Use crm_get_fields to inspect supported schema."
                )
            schema_info = schema_fields.get(k) or (lower_schema.get(kl)[1] if kl in lower_schema else None)
            if schema_info and schema_info.get("isReadOnly") is True:
                raise ValueError(f"Field '{k}' is marked read-only in Bitrix24 schema and cannot be modified.")


def _extract_communication_values(fields: dict[str, Any]) -> dict[str, list[str]]:
    phones: list[str] = []
    emails: list[str] = []

    for k, v in fields.items():
        kl = k.lower()
        if "phone" in kl:
            if isinstance(v, str) and v.strip():
                phones.append(v.strip())
            elif isinstance(v, list):
                for item in v:
                    if isinstance(item, dict) and "value" in item:
                        phones.append(str(item["value"]).strip())
                    elif isinstance(item, str) and item.strip():
                        phones.append(item.strip())
        elif "email" in kl:
            if isinstance(v, str) and v.strip():
                emails.append(v.strip())
            elif isinstance(v, list):
                for item in v:
                    if isinstance(item, dict) and "value" in item:
                        emails.append(str(item["value"]).strip())
                    elif isinstance(item, str) and item.strip():
                        emails.append(item.strip())

    return {"PHONE": phones, "EMAIL": emails}


def _resolve_entity_type_name(entity_type: Union[int, str]) -> str:
    if isinstance(entity_type, str) and not entity_type.strip().isdigit():
        return entity_type.strip().lower()
    eid = _resolve_entity_type_id(entity_type)
    for k, v in _ENTITY_TYPE_NAME_MAP.items():
        if v == eid:
            return k
    return str(eid)


# ---------------------------------------------------------------------------
# Bitrix24 HTTP Helpers
# ---------------------------------------------------------------------------
_RETRY_STATUSES = {429, 500, 502, 503, 504}
_MAX_RETRIES = 3


async def _bx_call(
    client: httpx.AsyncClient,
    method: str,
    params: dict[str, Any],
    *,
    retry: int = 0,
) -> dict[str, Any]:
    _validate_read_method(method)
    url = f"{BITRIX_BASE}/{method}.json"
    log.info("Bitrix24 -> %s", method)

    try:
        r = await client.post(url, json=params, timeout=30)
    except httpx.RequestError as exc:
        raise RuntimeError(f"Network error calling {method}: {exc}") from exc

    if r.status_code in _RETRY_STATUSES and retry < _MAX_RETRIES:
        wait = 2 ** retry
        log.warning("HTTP %s on %s, retrying in %ss", r.status_code, method, wait)
        await asyncio.sleep(wait)
        return await _bx_call(client, method, params, retry=retry + 1)

    if r.status_code == 429 and retry >= _MAX_RETRIES:
        raise RuntimeError("Bitrix24 rate limit exceeded after retries.")

    try:
        body = r.json()
    except Exception:
        raise RuntimeError(f"Bitrix24 non-JSON response (HTTP {r.status_code})")

    if "error" in body:
        raise RuntimeError(f"Bitrix24 [{body.get('error')}]: {body.get('error_description', '')}")

    return body


async def _bx_list_all(
    client: httpx.AsyncClient,
    method: str,
    params: dict[str, Any],
    max_pages: int = 50,
) -> tuple[list[dict], int]:
    items: list[dict] = []
    start = params.get("start", 0)
    total = None
    page_count = 0

    while page_count < max_pages:
        body = await _bx_call(client, method, {**params, "start": start})
        result = body.get("result", {})

        if isinstance(result, list):
            page = result
        elif isinstance(result, dict):
            # Handles {"items": [...]}, {"products": [...]}, {"categories": [...]}, {"storeProducts": [...]}
            page = []
            for key, val in result.items():
                if isinstance(val, list):
                    page = val
                    break
        else:
            page = []

        if total is None:
            total = body.get("total", len(page))

        items.extend(page)
        page_count += 1

        next_offset = body.get("next")
        if next_offset is None or not page:
            break
        start = next_offset

    return items, total if total is not None else len(items)


def _trim(obj: Any, keys: Optional[list[str]] = None) -> Any:
    """Recursively strip null, empty strings, and empty collections for concise LLM context."""
    if isinstance(obj, list):
        return [_trim(i, keys) for i in obj]
    if isinstance(obj, dict):
        if keys:
            return {k: obj[k] for k in keys if k in obj}
        return {k: v for k, v in obj.items() if v is not None and v != ""}
    return obj


# ---------------------------------------------------------------------------
# Entity Type ID Resolver
# ---------------------------------------------------------------------------
_ENTITY_TYPE_NAME_MAP = {
    "lead": 1,
    "deal": 2,
    "contact": 3,
    "company": 4,
    "invoice": 5,
    "quote": 7,
    "requisite": 8,
    "smart_invoice": 31,
    "smartinvoice": 31,
}

_ENTITY_TYPE_ID_MAP = {v: k.title() for k, v in _ENTITY_TYPE_NAME_MAP.items() if "_" not in k}


def _resolve_entity_type_id(entity_type: Union[int, str]) -> int:
    if isinstance(entity_type, int):
        return entity_type
    s = str(entity_type).strip().lower()
    if s.isdigit():
        return int(s)
    if s in _ENTITY_TYPE_NAME_MAP:
        return _ENTITY_TYPE_NAME_MAP[s]
    raise ValueError(
        f"Unknown entity type '{entity_type}'. Use an integer ID (e.g. 1=Lead, 2=Deal, 3=Contact, 4=Company, 7=Quote, 31=Smart Invoice) or common name."
    )


# ---------------------------------------------------------------------------
# FastMCP Server Definition
# ---------------------------------------------------------------------------
mcp = FastMCP(
    "Bitrix24 Catalog & CRM Server",
    instructions=(
        "Read-only access to Bitrix24 Product Catalog and CRM data. "
        "Use list_catalogs to discover catalog iblockId values. "
        "Use universal crm_* tools with entityTypeId (1=Lead, 2=Deal, 3=Contact, 4=Company, 7=Quote, 31=Smart Invoice, or custom SPA ID) to query CRM data."
    ),
)


# ===========================================================================
# 1. CATALOG TOOLS (9 Tools)
# ===========================================================================

@mcp.tool(
    description=(
        "List all trade catalogs. Returns id, name, iblockId, skuPropertyId. "
        "Call this first to discover iblockId values needed by other catalog tools."
    )
)
async def list_catalogs() -> str:
    async with httpx.AsyncClient() as client:
        body = await _bx_call(client, "catalog.catalog.list", {})
    result = body.get("result", {})
    catalogs = result.get("catalogs", result) if isinstance(result, dict) else result
    return json.dumps({"catalogs": _trim(catalogs)}, ensure_ascii=False)


@mcp.tool(
    description=(
        "List product sections/categories in a catalog (iblock_id required). "
        "Optional: parent_id to list sub-sections, limit (default 50)."
    )
)
async def list_sections(
    iblock_id: int,
    parent_id: Optional[int] = None,
    limit: int = 50,
) -> str:
    flt: dict[str, Any] = {"iblockId": iblock_id}
    if parent_id is not None:
        flt["iblockSectionId"] = parent_id

    params: dict[str, Any] = {
        "filter": flt,
        "select": ["id", "name", "code", "iblockId", "iblockSectionId", "depthLevel", "active"],
        "order": {"name": "asc"},
    }

    async with httpx.AsyncClient() as client:
        items, total = await _bx_list_all(client, "catalog.section.list", params)

    return json.dumps({
        "sections": _trim(items[:limit]),
        "returned": min(limit, len(items)),
        "total": total,
    }, ensure_ascii=False)


@mcp.tool(
    description=(
        "List products filtered by catalog (iblock_id required). "
        "Optional: section_id, name_search (partial match), select (field list), "
        "limit (default 20, max 200), offset for pagination."
    )
)
async def list_products(
    iblock_id: int,
    section_id: Optional[int] = None,
    name_search: Optional[str] = None,
    select: Optional[list[str]] = None,
    limit: int = 20,
    offset: int = 0,
) -> str:
    limit = min(limit, 200)
    default_select = [
        "id", "iblockId", "name", "active", "code", "xmlId",
        "iblockSectionId", "purchasingPrice", "purchasingCurrency",
        "quantity", "measure", "weight", "dateCreate",
    ]
    fields = list(select) if select else list(default_select)
    if "iblockId" not in fields:
        fields.append("iblockId")

    flt: dict[str, Any] = {"iblockId": iblock_id}
    if section_id is not None:
        flt["iblockSectionId"] = section_id
    if name_search:
        flt["%name"] = name_search

    params: dict[str, Any] = {
        "filter": flt,
        "select": fields,
        "order": {"id": "asc"},
        "start": offset,
    }

    async with httpx.AsyncClient() as client:
        body = await _bx_call(client, "catalog.product.list", params)

    result = body.get("result", {})
    products = result.get("products", result) if isinstance(result, dict) else result
    total = body.get("total", len(products))

    return json.dumps({
        "products": _trim(products[:limit]),
        "returned": min(limit, len(products)),
        "total": total,
        "offset": offset,
        "has_more": (offset + len(products)) < total,
    }, ensure_ascii=False)


@mcp.tool(description="Get full details for a single product by its numeric id.")
async def get_product(product_id: int) -> str:
    async with httpx.AsyncClient() as client:
        body = await _bx_call(client, "catalog.product.get", {"id": product_id})
    result = body.get("result", {})
    product = result.get("product", result) if isinstance(result, dict) else result
    return json.dumps({"product": _trim(product)}, ensure_ascii=False)


@mcp.tool(
    description=(
        "List SKU / variation offers for a product. "
        "Returns variation records (id, name, purchasingPrice, etc.)."
    )
)
async def list_product_variations(product_id: int) -> str:
    async with httpx.AsyncClient() as client:
        items = []
        total = 0
        try:
            cat_body = await _bx_call(client, "catalog.catalog.list", {})
            cats = cat_body.get("result", {}).get("catalogs", [])
            offers_iblocks = [
                c.get("iblockId") for c in cats
                if c.get("productIblockId") is not None or c.get("skuPropertyId") is not None
            ]
            for o_ib in offers_iblocks:
                o_items, _ = await _bx_list_all(
                    client,
                    "catalog.product.list",
                    {
                        "filter": {"iblockId": o_ib, "parentId": product_id},
                        "select": ["id", "iblockId", "name", "purchasingPrice", "purchasingCurrency", "quantity", "parentId"],
                    },
                )
                items.extend(o_items)
            total = len(items)
        except Exception:
            pass

        if not items:
            try:
                items, total = await _bx_list_all(
                    client,
                    "catalog.product.sku.list",
                    {"filter": {"parentId": product_id}},
                )
            except Exception:
                pass

    return json.dumps({
        "parent_product_id": product_id,
        "variations": _trim(items),
        "total": total,
    }, ensure_ascii=False)


@mcp.tool(
    description=(
        "Get price records for a product or variation. "
        "Returns currency, price, catalogGroupId (price type), and price scale."
    )
)
async def get_prices(product_id: int) -> str:
    async with httpx.AsyncClient() as client:
        items, total = await _bx_list_all(
            client,
            "catalog.price.list",
            {"filter": {"productId": product_id}},
        )
    return json.dumps({
        "product_id": product_id,
        "prices": _trim(items),
        "total": total,
    }, ensure_ascii=False)


@mcp.tool(
    description=(
        "Get per-warehouse inventory stock levels for a product. "
        "Returns storeId, amount (on hand), and amountReserved per warehouse."
    )
)
async def get_stock(product_id: int) -> str:
    async with httpx.AsyncClient() as client:
        items, total = await _bx_list_all(
            client,
            "catalog.storeproduct.list",
            {
                "filter": {"productId": product_id},
                "select": ["id", "storeId", "productId", "amount", "amountReserved"],
            },
        )
    return json.dumps({
        "product_id": product_id,
        "stores": _trim(items),
        "total": total,
    }, ensure_ascii=False)


@mcp.tool(
    description=(
        "Count products in a catalog (iblock_id required). "
        "Optional section_id to narrow scope. "
        "Uses the total field from the API response, not a local count. Very fast."
    )
)
async def count_products(
    iblock_id: int,
    section_id: Optional[int] = None,
) -> str:
    flt: dict[str, Any] = {"iblockId": iblock_id}
    if section_id is not None:
        flt["iblockSectionId"] = section_id

    async with httpx.AsyncClient() as client:
        body = await _bx_call(client, "catalog.product.list", {
            "filter": flt,
            "select": ["id", "iblockId"],
            "start": 0,
        })

    return json.dumps({
        "count": body.get("total", 0),
        "iblock_id": iblock_id,
        "section_id": section_id,
    }, ensure_ascii=False)


@mcp.tool(
    description=(
        "Sum inventory quantities across all warehouses. "
        "product_id: sum for one product. "
        "iblock_id: sum the entire catalog (slow for large catalogs). "
        "Returns total_quantity and total_reserved."
    )
)
async def get_total_stock(
    product_id: Optional[int] = None,
    iblock_id: Optional[int] = None,
) -> str:
    if product_id is None and iblock_id is None:
        return json.dumps({"error": "Provide either product_id or iblock_id"})

    async with httpx.AsyncClient() as client:
        if product_id is not None:
            items, _ = await _bx_list_all(client, "catalog.storeproduct.list", {
                "filter": {"productId": product_id},
                "select": ["amount", "amountReserved"],
            })
            product_ids = [product_id]
        else:
            all_products, _ = await _bx_list_all(client, "catalog.product.list", {
                "filter": {"iblockId": iblock_id},
                "select": ["id", "iblockId"],
            })
            product_ids = [p["id"] for p in all_products]

            if not product_ids:
                return json.dumps({
                    "total_quantity": 0, "total_reserved": 0,
                    "product_count": 0, "iblock_id": iblock_id,
                }, ensure_ascii=False)

            items = []
            for i in range(0, len(product_ids), 50):
                batch = product_ids[i : i + 50]
                batch_items, _ = await _bx_list_all(client, "catalog.storeproduct.list", {
                    "filter": {"productId": batch},
                    "select": ["amount", "amountReserved"],
                })
                items.extend(batch_items)

    total_qty = sum(float(r.get("amount") or 0) for r in items)
    total_res = sum(float(r.get("amountReserved") or 0) for r in items)

    out: dict[str, Any] = {
        "total_quantity": total_qty,
        "total_reserved": total_res,
        "product_count": len(product_ids),
    }
    if product_id is not None:
        out["product_id"] = product_id
    else:
        out["iblock_id"] = iblock_id

    return json.dumps(out, ensure_ascii=False)


# ===========================================================================
# 2. CRM TOOLS (11 Tools)
# ===========================================================================

@mcp.tool(
    description=(
        "Get field definitions and labels (including custom UF_CRM_* fields) for a CRM entity. "
        "entity_type: 1 or 'lead', 2 or 'deal', 3 or 'contact', 4 or 'company', 7 or 'quote', "
        "31 or 'smart_invoice', or a custom Smart Process (SPA) entityTypeId."
    )
)
async def crm_get_fields(entity_type: Union[int, str]) -> str:
    entity_type_id = _resolve_entity_type_id(entity_type)
    async with httpx.AsyncClient() as client:
        body = await _bx_call(client, "crm.item.fields", {"entityTypeId": entity_type_id})
    
    fields_raw = body.get("result", {}).get("fields", {})
    # Simplify field metadata for compact response
    fields_compact = {}
    for code, meta in fields_raw.items():
        fields_compact[code] = {
            "title": meta.get("title", meta.get("formLabel", code)),
            "type": meta.get("type"),
            "isRequired": meta.get("isRequired", False),
            "isReadOnly": meta.get("isReadOnly", False),
            "isMultiple": meta.get("isMultiple", False),
        }

    return json.dumps({
        "entity_type_id": entity_type_id,
        "entity_name": _ENTITY_TYPE_ID_MAP.get(entity_type_id, f"SPA_{entity_type_id}"),
        "total_fields": len(fields_compact),
        "fields": fields_compact,
    }, ensure_ascii=False)


@mcp.tool(
    description=(
        "List CRM items (leads, deals, contacts, companies, quotes, smart invoices, SPAs) "
        "with filtering, field selection, ordering, and pagination. "
        "entity_type: 1 or 'lead', 2 or 'deal', 3 or 'contact', 4 or 'company', 7 or 'quote', 31 or 'smart_invoice'. "
        "filter: dict like {'stageId': 'NEW', '>opportunity': 1000, 'assignedById': 1}. "
        "select: list of field codes like ['id', 'title', 'stageId', 'opportunity', 'companyId', 'contactId']. "
        "order: dict like {'id': 'desc'} or {'opportunity': 'asc'}. "
        "limit: default 20 (max 200). offset: start position."
    )
)
async def crm_list_items(
    entity_type: Union[int, str],
    filter: Optional[dict[str, Any]] = None,
    select: Optional[list[str]] = None,
    order: Optional[dict[str, str]] = None,
    limit: int = 20,
    offset: int = 0,
) -> str:
    entity_type_id = _resolve_entity_type_id(entity_type)
    limit = min(limit, 200)

    params: dict[str, Any] = {
        "entityTypeId": entity_type_id,
        "start": offset,
    }
    if filter:
        params["filter"] = filter
    if select:
        params["select"] = select
    if order:
        params["order"] = order

    async with httpx.AsyncClient() as client:
        body = await _bx_call(client, "crm.item.list", params)

    result = body.get("result", {})
    items = result.get("items", result) if isinstance(result, dict) else result
    total = body.get("total", len(items))

    return json.dumps({
        "entity_type_id": entity_type_id,
        "items": _trim(items[:limit]),
        "returned": min(limit, len(items)),
        "total": total,
        "offset": offset,
        "has_more": (offset + len(items)) < total,
    }, ensure_ascii=False)


@mcp.tool(
    description=(
        "Get full details for a single CRM item by ID. "
        "entity_type: 1 or 'lead', 2 or 'deal', 3 or 'contact', 4 or 'company', 7 or 'quote', 31 or 'smart_invoice'. "
        "id: numeric item ID."
    )
)
async def crm_get_item(entity_type: Union[int, str], id: int) -> str:
    entity_type_id = _resolve_entity_type_id(entity_type)
    async with httpx.AsyncClient() as client:
        body = await _bx_call(client, "crm.item.get", {"entityTypeId": entity_type_id, "id": id})

    result = body.get("result", {})
    item = result.get("item", result) if isinstance(result, dict) else result
    return json.dumps({
        "entity_type_id": entity_type_id,
        "item": _trim(item),
    }, ensure_ascii=False)


@mcp.tool(
    description=(
        "Fast count of CRM items matching an optional filter without fetching all data. "
        "entity_type: 1 or 'lead', 2 or 'deal', 3 or 'contact', 4 or 'company', 7 or 'quote', 31 or 'smart_invoice'. "
        "filter: optional criteria dict (e.g. {'stageId': 'WON', 'assignedById': 1})."
    )
)
async def crm_count_items(
    entity_type: Union[int, str],
    filter: Optional[dict[str, Any]] = None,
) -> str:
    entity_type_id = _resolve_entity_type_id(entity_type)
    params: dict[str, Any] = {
        "entityTypeId": entity_type_id,
        "select": ["id"],
        "start": 0,
    }
    if filter:
        params["filter"] = filter

    async with httpx.AsyncClient() as client:
        body = await _bx_call(client, "crm.item.list", params)

    return json.dumps({
        "entity_type_id": entity_type_id,
        "entity_name": _ENTITY_TYPE_ID_MAP.get(entity_type_id, f"SPA_{entity_type_id}"),
        "count": body.get("total", 0),
        "filter": filter or {},
    }, ensure_ascii=False)


@mcp.tool(
    description=(
        "Get product line items / rows attached to a deal. "
        "deal_id: numeric deal ID. Returns product ID, price, quantity, discount, etc."
    )
)
async def crm_deal_products(deal_id: int) -> str:
    async with httpx.AsyncClient() as client:
        # Try both deal productrows and universal item productrows
        try:
            body = await _bx_call(client, "crm.deal.productrows.get", {"id": deal_id})
            rows = body.get("result", [])
        except Exception:
            body = await _bx_call(client, "crm.item.productrow.list", {
                "filter": {"=ownerType": "D", "=ownerId": deal_id}
            })
            result = body.get("result", {})
            rows = result.get("productRows", result) if isinstance(result, dict) else result

    return json.dumps({
        "deal_id": deal_id,
        "product_rows": _trim(rows),
        "total_rows": len(rows),
    }, ensure_ascii=False)


@mcp.tool(
    description=(
        "Get linked relations for a CRM entity (deal-contact, contact-company, company-contact, deal-company). "
        "entity_type: 2 or 'deal', 3 or 'contact', 4 or 'company'. "
        "id: numeric ID of the source entity."
    )
)
async def crm_related(entity_type: Union[int, str], id: int) -> str:
    entity_type_id = _resolve_entity_type_id(entity_type)
    out: dict[str, Any] = {
        "source_entity_type_id": entity_type_id,
        "source_id": id,
        "contacts": [],
        "companies": [],
        "deals": [],
    }

    async with httpx.AsyncClient() as client:
        if entity_type_id == 2:  # Deal
            # Fetch deal item to get companyId and contactIds
            try:
                deal_body = await _bx_call(client, "crm.item.get", {
                    "entityTypeId": 2, "id": id
                })
                item = deal_body.get("result", {}).get("item", {})
                if item.get("companyId"):
                    out["companies"].append({"id": item["companyId"]})
                if item.get("contactId"):
                    out["contacts"].append({"id": item["contactId"], "is_primary": True})
                for cid in item.get("contactIds", []):
                    if not any(c["id"] == cid for c in out["contacts"]):
                        out["contacts"].append({"id": cid, "is_primary": False})
            except Exception:
                pass

            # Also check deal.contact.items.get
            try:
                c_body = await _bx_call(client, "crm.deal.contact.items.get", {"id": id})
                for c in c_body.get("result", []):
                    cid = c.get("CONTACT_ID")
                    if cid and not any(x["id"] == cid for x in out["contacts"]):
                        out["contacts"].append({"id": cid, "sort": c.get("SORT")})
            except Exception:
                pass

        elif entity_type_id == 3:  # Contact
            # Contact -> Companies
            try:
                comp_body = await _bx_call(client, "crm.contact.company.items.get", {"id": id})
                for comp in comp_body.get("result", []):
                    out["companies"].append({
                        "id": comp.get("COMPANY_ID"),
                        "is_primary": comp.get("IS_PRIMARY") == "Y",
                    })
            except Exception:
                pass

            # Contact -> Deals
            try:
                deals_body = await _bx_call(client, "crm.item.list", {
                    "entityTypeId": 2,
                    "filter": {"=contactId": id},
                    "select": ["id", "title", "stageId", "opportunity", "currencyId"],
                    "limit": 20,
                })
                out["deals"] = deals_body.get("result", {}).get("items", [])
            except Exception:
                pass

        elif entity_type_id == 4:  # Company
            # Company -> Contacts
            try:
                c_body = await _bx_call(client, "crm.company.contact.items.get", {"id": id})
                for c in c_body.get("result", []):
                    out["contacts"].append({
                        "id": c.get("CONTACT_ID"),
                        "is_primary": c.get("IS_PRIMARY") == "Y",
                    })
            except Exception:
                pass

            # Company -> Deals
            try:
                deals_body = await _bx_call(client, "crm.item.list", {
                    "entityTypeId": 2,
                    "filter": {"=companyId": id},
                    "select": ["id", "title", "stageId", "opportunity", "currencyId"],
                    "limit": 20,
                })
                out["deals"] = deals_body.get("result", {}).get("items", [])
            except Exception:
                pass

    return json.dumps(_trim(out), ensure_ascii=False)


@mcp.tool(
    description=(
        "List CRM activities (tasks, calls, meetings, emails) and optionally timeline comments. "
        "owner_type: optional entity type (1=Lead, 2=Deal, 3=Contact, 4=Company). "
        "owner_id: optional numeric ID of the specific lead/deal/contact/company. "
        "type_id: optional activity type (1=Meeting, 2=Call, 3=Task, 4=Email). "
        "completed: 'Y' or 'N'. "
        "include_timeline_comments: if True, also retrieves timeline comments for the entity."
    )
)
async def crm_activities(
    owner_type: Optional[Union[int, str]] = None,
    owner_id: Optional[int] = None,
    type_id: Optional[int] = None,
    completed: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
    include_timeline_comments: bool = False,
) -> str:
    limit = min(limit, 200)
    flt: dict[str, Any] = {}

    owner_type_id = _resolve_entity_type_id(owner_type) if owner_type is not None else None
    if owner_type_id is not None:
        flt["OWNER_TYPE_ID"] = owner_type_id
    if owner_id is not None:
        flt["OWNER_ID"] = owner_id
    if type_id is not None:
        flt["TYPE_ID"] = type_id
    if completed is not None:
        flt["COMPLETED"] = completed.upper()

    params: dict[str, Any] = {
        "filter": flt,
        "select": ["ID", "OWNER_TYPE_ID", "OWNER_ID", "TYPE_ID", "SUBJECT", "START_TIME", "END_TIME", "DEADLINE", "COMPLETED", "RESPONSIBLE_ID", "PRIORITY"],
        "order": {"ID": "DESC"},
        "start": offset,
    }

    async with httpx.AsyncClient() as client:
        body = await _bx_call(client, "crm.activity.list", params)
        activities = body.get("result", [])
        total = body.get("total", len(activities))

        comments = []
        if include_timeline_comments and owner_type_id is not None and owner_id is not None:
            entity_name = _ENTITY_TYPE_ID_MAP.get(owner_type_id, "deal").lower()
            try:
                com_body = await _bx_call(client, "crm.timeline.comment.list", {
                    "filter": {"ENTITY_TYPE": entity_name, "ENTITY_ID": owner_id},
                    "order": {"ID": "DESC"},
                })
                comments = com_body.get("result", [])
            except Exception:
                pass

    return json.dumps({
        "activities": _trim(activities[:limit]),
        "comments": _trim(comments),
        "returned": min(limit, len(activities)),
        "total": total,
        "offset": offset,
        "has_more": (offset + len(activities)) < total,
    }, ensure_ascii=False)


@mcp.tool(
    description=(
        "List CRM status stages and pipeline categories (funnels). "
        "entity_id: optional status category (e.g. 'DEAL_STAGE', 'STATUS' for leads, 'QUOTE_STATUS'). "
        "entity_type: optional (2 or 'deal' to include deal pipeline categories)."
    )
)
async def crm_list_statuses(
    entity_id: Optional[str] = None,
    entity_type: Optional[Union[int, str]] = 2,
) -> str:
    async with httpx.AsyncClient() as client:
        flt = {"ENTITY_ID": entity_id} if entity_id else {}
        status_body = await _bx_call(client, "crm.status.list", {"filter": flt, "order": {"SORT": "ASC"}})
        statuses = status_body.get("result", [])

        categories = []
        if entity_type is not None:
            try:
                et_id = _resolve_entity_type_id(entity_type)
                # Bitrix24 only supports categories for Deals (2) and SPAs (>= 128)
                if et_id == 2 or et_id >= 128:
                    try:
                        cat_body = await _bx_call(client, "crm.category.list", {"entityTypeId": int(et_id)})
                        categories = cat_body.get("result", {}).get("categories", [])
                    except Exception:
                        if et_id == 2:
                            cat_body = await _bx_call(client, "crm.dealcategory.list", {})
                            categories = cat_body.get("result", [])
            except Exception:
                pass

    return json.dumps({
        "statuses": _trim(statuses),
        "categories": _trim(categories),
        "total_statuses": len(statuses),
        "total_categories": len(categories),
    }, ensure_ascii=False)


@mcp.tool(
    description=(
        "List CRM pipeline funnels / categories for deals or Smart Process Automations (SPAs). "
        "entity_type: CRM entity type (defaults to 2 for deals). "
        "Requires entityTypeId (always passed automatically). Note: only Deals (2) and SPAs support categories."
    )
)
async def crm_list_categories(
    entity_type: Union[int, str] = 2,
) -> str:
    et_id = _resolve_entity_type_id(entity_type)
    if et_id not in (2,) and et_id < 128:
        return json.dumps({
            "categories": [],
            "total": 0,
            "entity_type_id": et_id,
            "message": f"Entity type '{entity_type}' (ID {et_id}) does not support sales funnels/categories. Only Deals (2) and Smart Process Automations (SPAs) support categories.",
        }, ensure_ascii=False)

    async with httpx.AsyncClient() as client:
        try:
            cat_body = await _bx_call(client, "crm.category.list", {"entityTypeId": int(et_id)})
            categories = cat_body.get("result", {}).get("categories", [])
        except Exception as e:
            if et_id == 2:
                try:
                    fallback_body = await _bx_call(client, "crm.dealcategory.list", {})
                    categories = fallback_body.get("result", [])
                except Exception:
                    raise e
            else:
                raise e

    return json.dumps({
        "categories": _trim(categories),
        "total": len(categories),
        "entity_type_id": et_id,
    }, ensure_ascii=False)


@mcp.tool(
    description=(
        "Find duplicate CRM records (leads, contacts, companies) by email or phone number. "
        "type: 'EMAIL' or 'PHONE'. "
        "values: list of phone numbers or email strings to check. "
        "entity_type: optional ('LEAD', 'CONTACT', 'COMPANY')."
    )
)
async def crm_find_duplicates(
    type: str,
    values: list[str],
    entity_type: Optional[str] = None,
) -> str:
    params: dict[str, Any] = {
        "type": type.upper(),
        "values": values,
    }
    if entity_type:
        params["entity_type"] = entity_type.upper()

    async with httpx.AsyncClient() as client:
        body = await _bx_call(client, "crm.duplicate.findbycomm", params)

    return json.dumps({
        "type": type.upper(),
        "searched_values": values,
        "duplicates": body.get("result", {}),
    }, ensure_ascii=False)


@mcp.tool(
    description=(
        "Compute pipeline summary for deals: count of deals and total monetary value per stage. "
        "category_id: deal pipeline/category ID (e.g. 0 for main pipeline). "
        "assigned_by_id: optional filter by responsible user ID. "
        "date_create_from: optional start date (e.g. '2026-01-01'). "
        "date_create_to: optional end date (e.g. '2026-12-31'). "
        "Paginates server-side and returns ONLY aggregate metrics."
    )
)
async def pipeline_summary(
    category_id: Optional[int] = 0,
    assigned_by_id: Optional[int] = None,
    date_create_from: Optional[str] = None,
    date_create_to: Optional[str] = None,
) -> str:
    flt: dict[str, Any] = {}
    if category_id is not None:
        flt["categoryId"] = category_id
    if assigned_by_id is not None:
        flt["assignedById"] = assigned_by_id
    if date_create_from:
        flt[">=begindate"] = date_create_from
    if date_create_to:
        flt["<=begindate"] = date_create_to

    params = {
        "entityTypeId": 2,  # Deal
        "filter": flt,
        "select": ["id", "stageId", "opportunity", "currencyId", "categoryId", "assignedById"],
    }

    async with httpx.AsyncClient() as client:
        # Fetch all deals in scope
        deals, total = await _bx_list_all(client, "crm.item.list", params)

        # Fetch stage names
        stage_names: dict[str, str] = {}
        try:
            entity_stage_key = f"DEAL_STAGE_{category_id}" if category_id and category_id > 0 else "DEAL_STAGE"
            st_body = await _bx_call(client, "crm.status.list", {"filter": {"ENTITY_ID": entity_stage_key}})
            for s in st_body.get("result", []):
                stage_names[s.get("STATUS_ID", "")] = s.get("NAME", "")
        except Exception:
            pass

    # Aggregations
    stage_data: dict[str, dict[str, Any]] = {}
    total_val = 0.0
    primary_currency = "USD"

    for d in deals:
        stage_id = d.get("stageId") or "UNKNOWN"
        opp = float(d.get("opportunity") or 0.0)
        curr = d.get("currencyId") or primary_currency
        primary_currency = curr
        total_val += opp

        if stage_id not in stage_data:
            stage_data[stage_id] = {
                "stage_id": stage_id,
                "stage_name": stage_names.get(stage_id, stage_id),
                "count": 0,
                "total_value": 0.0,
                "currency": curr,
            }
        stage_data[stage_id]["count"] += 1
        stage_data[stage_id]["total_value"] += opp

    stages_list = list(stage_data.values())
    for s in stages_list:
        s["total_value"] = round(s["total_value"], 2)
        s["pct_count"] = round((s["count"] / len(deals) * 100), 1) if deals else 0.0
        s["pct_value"] = round((s["total_value"] / total_val * 100), 1) if total_val > 0 else 0.0

    return json.dumps({
        "total_deals": len(deals),
        "total_pipeline_value": round(total_val, 2),
        "currency": primary_currency,
        "category_id": category_id,
        "assigned_by_id": assigned_by_id,
        "stages": stages_list,
    }, ensure_ascii=False)


@mcp.tool(
    description=(
        "Get stage transition history and calculate time spent in each stage. "
        "entity_type: 1 or 'lead', 2 or 'deal', 7 or 'quote', 31 or 'smart_invoice'. "
        "owner_id: optional numeric ID of the deal/lead/item to inspect history for."
    )
)
async def stage_history(
    entity_type: Union[int, str] = 2,
    owner_id: Optional[int] = None,
    limit: int = 50,
) -> str:
    entity_type_id = _resolve_entity_type_id(entity_type)
    flt: dict[str, Any] = {}
    if owner_id is not None:
        flt["OWNER_ID"] = owner_id

    params: dict[str, Any] = {
        "entityTypeId": entity_type_id,
        "filter": flt,
        "order": {"id": "asc"},
    }

    async with httpx.AsyncClient() as client:
        body = await _bx_call(client, "crm.stagehistory.list", params)

    result = body.get("result", {})
    items = result.get("items", result) if isinstance(result, dict) else result
    
    # Calculate time-in-stage if items belong to a single owner
    durations: dict[str, float] = {}  # stage_id -> total seconds
    if owner_id is not None and len(items) > 1:
        for i in range(len(items) - 1):
            curr = items[i]
            nxt = items[i + 1]
            st = curr.get("STAGE_ID") or curr.get("stageId")
            try:
                t1 = datetime.fromisoformat(curr.get("CREATED_TIME", "").replace("Z", "+00:00"))
                t2 = datetime.fromisoformat(nxt.get("CREATED_TIME", "").replace("Z", "+00:00"))
                diff_sec = (t2 - t1).total_seconds()
                durations[st] = durations.get(st, 0.0) + diff_sec
            except Exception:
                pass

    return json.dumps({
        "entity_type_id": entity_type_id,
        "owner_id": owner_id,
        "transitions_count": len(items),
        "history": _trim(items[:limit]),
        "stage_durations_seconds": {k: round(v, 1) for k, v in durations.items()},
    }, ensure_ascii=False)


# ===========================================================================
# 3. CRM WRITE TOOLS (4 Tools - Tightly Scoped & Confirmed)
# ===========================================================================

async def crm_add_comment(
    entity_type: Union[int, str],
    entity_id: int,
    comment: str,
) -> str:
    """Add a timeline comment to a CRM record (lead, deal, contact, company, etc.).
    
    Parameters:
    - entity_type: Entity type ID (1=lead, 2=deal, 3=contact, 4=company) or name ('lead', 'deal', etc.)
    - entity_id: ID of the CRM record
    - comment: Text content of the timeline comment
    """
    if not comment or not str(comment).strip():
        raise ValueError("Comment text cannot be empty.")
    entity_type_name = _resolve_entity_type_name(entity_type)
    
    params = {
        "fields": {
            "ENTITY_ID": int(entity_id),
            "ENTITY_TYPE": entity_type_name,
            "COMMENT": str(comment).strip(),
        }
    }
    
    _audit_write("crm_add_comment", entity_type_name, entity_id, ["COMMENT"])
    async with httpx.AsyncClient() as client:
        body = await _bx_write_call(client, "crm.timeline.comment.add", params)
    
    comment_id = body.get("result")
    return json.dumps({
        "status": "created",
        "comment_id": comment_id,
        "entity_type": entity_type_name,
        "entity_id": int(entity_id),
    }, ensure_ascii=False)


async def crm_add_todo(
    entity_type: Union[int, str],
    entity_id: int,
    deadline: str,
    title: Optional[str] = None,
    description: Optional[str] = None,
    responsible_id: Optional[int] = None,
) -> str:
    """Create a follow-up/to-do activity on a CRM record.
    
    Parameters:
    - entity_type: Entity type ID (1=lead, 2=deal, 3=contact, 4=company) or name
    - entity_id: ID of the CRM record
    - deadline: Due date and time (ISO 8601 or YYYY-MM-DD HH:MM:SS)
    - title: Optional title of the to-do
    - description: Optional detailed description of the to-do
    - responsible_id: Optional user ID responsible for this activity
    """
    if not title and not description:
        raise ValueError("Either title or description must be provided for the to-do activity.")
    if not deadline or not str(deadline).strip():
        raise ValueError("Deadline is required for the to-do activity.")
        
    entity_type_id = _resolve_entity_type_id(entity_type)
    
    params: dict[str, Any] = {
        "ownerTypeId": entity_type_id,
        "ownerId": int(entity_id),
        "deadline": str(deadline).strip(),
    }
    field_names = ["deadline"]
    if title:
        params["title"] = str(title).strip()
        field_names.append("title")
    if description:
        params["description"] = str(description).strip()
        field_names.append("description")
    if responsible_id is not None:
        params["responsibleId"] = int(responsible_id)
        field_names.append("responsibleId")

    _audit_write("crm_add_todo", entity_type_id, entity_id, field_names)
    async with httpx.AsyncClient() as client:
        body = await _bx_write_call(client, "crm.activity.todo.add", params)
        
    result = body.get("result", {})
    todo_id = result.get("id") if isinstance(result, dict) else result
    return json.dumps({
        "status": "created",
        "activity_id": todo_id,
        "entity_type_id": entity_type_id,
        "entity_id": int(entity_id),
        "deadline": str(deadline).strip(),
    }, ensure_ascii=False)


async def crm_update_item(
    entity_type: Union[int, str],
    entity_id: int,
    fields: dict[str, Any],
    confirm: bool = False,
) -> str:
    """Update fields on a single lead, deal, contact, or company.
    
    Two-step confirmation:
    - confirm=False (default): Fetches current record and returns a before/after preview diff WITHOUT writing.
    - confirm=True: Validates fields, checks for system fields, and applies the update.
    """
    if not isinstance(fields, dict):
        raise ValueError("Parameter 'fields' must be a dictionary.")
    if not fields:
        raise ValueError("Parameter 'fields' cannot be empty.")
    if isinstance(entity_id, (list, tuple, dict)):
        raise ValueError("Bulk updates are prohibited. Provide a single integer entity_id.")

    entity_type_id = _resolve_entity_type_id(entity_type)
    
    async with httpx.AsyncClient() as client:
        # Validate schema using read client
        schema = await _get_entity_fields(client, entity_type_id)
        _validate_fields_for_write(fields, schema, entity_type_id)
        
        # Fetch current record using read client
        get_body = await _bx_call(client, "crm.item.get", {"entityTypeId": entity_type_id, "id": int(entity_id)})
        item = get_body.get("result", {}).get("item")
        if not item:
            raise RuntimeError(f"Record {entity_id} of entity type {entity_type_id} not found.")

        # Preview mode (confirm=False)
        if not confirm:
            diff = {}
            for k, new_v in fields.items():
                old_v = item.get(k)
                diff[k] = {
                    "before": old_v,
                    "after": new_v,
                }
            return json.dumps({
                "status": "preview",
                "requires_confirmation": True,
                "entity_type_id": entity_type_id,
                "entity_id": int(entity_id),
                "diff": diff,
                "message": "Preview only. No changes were made to Bitrix24. Re-call with confirm=True to apply.",
            }, ensure_ascii=False)

        # Write execution (confirm=True)
        _audit_write("crm_update_item", entity_type_id, entity_id, list(fields.keys()))
        res = await _bx_write_call(
            client,
            "crm.item.update",
            {
                "entityTypeId": entity_type_id,
                "id": int(entity_id),
                "fields": fields,
            },
        )
        
    return json.dumps({
        "status": "updated",
        "entity_type_id": entity_type_id,
        "entity_id": int(entity_id),
        "fields_updated": sorted(list(fields.keys())),
        "result": _trim(res.get("result", {})),
    }, ensure_ascii=False)


async def crm_create_item(
    entity_type: Union[int, str],
    fields: dict[str, Any],
    confirm: bool = False,
) -> str:
    """Create a new lead, contact, or company.
    
    Creation is restricted to lead (1), contact (3), or company (4).
    Two-step confirmation:
    - confirm=False (default): Returns preview of proposed fields (and checks for duplicates if contact/company).
    - confirm=True: Creates the record in Bitrix24.
    """
    if not isinstance(fields, dict):
        raise ValueError("Parameter 'fields' must be a dictionary.")
    if not fields:
        raise ValueError("Parameter 'fields' cannot be empty.")
        
    entity_type_id = _resolve_entity_type_id(entity_type)
    if entity_type_id not in (1, 3, 4):
        raise ValueError(
            f"Creation is currently restricted to lead (1), contact (3), or company (4). "
            f"Entity type {entity_type_id} is out of scope (deals, quotes, invoices are not supported for creation)."
        )
    
    entity_type_name = _resolve_entity_type_name(entity_type)
    
    async with httpx.AsyncClient() as client:
        # Validate schema using read client
        schema = await _get_entity_fields(client, entity_type_id)
        _validate_fields_for_write(fields, schema, entity_type_id)

        # Check for duplicates if contact or company
        duplicates: list[dict[str, Any]] = []
        if entity_type_id in (3, 4):
            comm_values = _extract_communication_values(fields)
            for comm_type, vals in comm_values.items():
                if vals:
                    try:
                        dup_res = await _bx_call(
                            client,
                            "crm.duplicate.findbycomm",
                            {
                                "type": comm_type,
                                "values": vals,
                                "entity_type": "CONTACT" if entity_type_id == 3 else "COMPANY",
                            },
                        )
                        d_items = dup_res.get("result", [])
                        if d_items:
                            duplicates.append({"type": comm_type, "values": vals, "matches": d_items})
                    except Exception as exc:
                        log.warning("Duplicate check failed: %s", exc)

        if not confirm:
            return json.dumps({
                "status": "preview",
                "requires_confirmation": True,
                "entity_type": entity_type_name,
                "entity_type_id": entity_type_id,
                "proposed_fields": fields,
                "potential_duplicates": duplicates,
                "message": "Preview only. Record was NOT created. Re-call with confirm=True to create the record.",
            }, ensure_ascii=False)

        # Write execution (confirm=True)
        _audit_write("crm_create_item", entity_type_id, "NEW", list(fields.keys()))
        res = await _bx_write_call(
            client,
            "crm.item.add",
            {
                "entityTypeId": entity_type_id,
                "fields": fields,
            },
        )

    item = res.get("result", {}).get("item", {})
    new_id = item.get("id") if isinstance(item, dict) else res.get("result")
    return json.dumps({
        "status": "created",
        "entity_type": entity_type_name,
        "entity_type_id": entity_type_id,
        "id": new_id,
        "fields_created": sorted(list(fields.keys())),
    }, ensure_ascii=False)


# ===========================================================================
# Conditional MCP Tool Registration for Writes
# ===========================================================================
if ENABLE_WRITES:
    mcp.tool(
        description=(
            "Add a timeline comment to a CRM record (lead, deal, contact, company). "
            "Requires entity_type (ID or name), entity_id, and comment text."
        )
    )(crm_add_comment)

    mcp.tool(
        description=(
            "Create a follow-up/to-do activity on a CRM record. "
            "Requires entity_type, entity_id, deadline (ISO 8601), and title or description."
        )
    )(crm_add_todo)

    mcp.tool(
        description=(
            "Update fields on a single lead, deal, contact, or company. "
            "Two-step confirmation: with confirm=false (default), previews diff without writing. "
            "With confirm=true, validates fields against schema and executes the update."
        )
    )(crm_update_item)

    mcp.tool(
        description=(
            "Create a new lead, contact, or company (deals not supported yet). "
            "Two-step confirmation: with confirm=false (default), previews and checks for duplicates. "
            "With confirm=true, validates fields against schema and creates the item."
        )
    )(crm_create_item)


# ===========================================================================
# Starlette routing & MCP Lifespan
# ===========================================================================
_mcp_asgi = mcp.http_app(path=f"/mcp/{MCP_SECRET_PATH}")


async def _health(request: Request) -> JSONResponse:
    return JSONResponse({
        "status": "ok",
        "service": "bitrix24-catalog-crm-mcp",
        "writes_enabled": ENABLE_WRITES,
    })


def build_app() -> Starlette:
    return Starlette(
        routes=[
            Route("/health", endpoint=_health, methods=["GET"]),
            Mount("", app=_mcp_asgi),
        ],
        lifespan=_mcp_asgi.lifespan,
    )


app = build_app()

if __name__ == "__main__":
    import uvicorn
    log.info("Starting Bitrix24 Catalog & CRM MCP on port %s", PORT)
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
