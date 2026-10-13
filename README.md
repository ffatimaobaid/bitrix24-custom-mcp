# Bitrix24 Product Catalog & CRM MCP Server

A **remote MCP server** in Python that exposes your Bitrix24 product catalog and CRM data to Claude via streamable HTTP transport.

By default, the server is **strictly read-only**. When explicitly configured, a tightly scoped set of safe CRM write tools can be activated via a dedicated write webhook with multi-layer safety guards.

## Architecture

```
Claude (cloud) ──HTTPS──► your-server.com/mcp/<SECRET>  (FastMCP / Starlette)
                                    │
                                    ├──► Bitrix24 REST API (Read webhook: BITRIX_WEBHOOK_URL)
                                    │
                                    └──► Bitrix24 REST API (Write webhook: BITRIX_WRITE_WEBHOOK_URL)
```

- **Secret Path**: Acts as a bearer token (`/mcp/<MCP_SECRET_PATH>`). Only callers with the full secret path can communicate with the MCP server.
- **Uptime Monitoring**: `/health` is public and returns service status and write enablement flags.

---

## Safety & Security Design

1. **Writes Off By Default**: Write tools are never registered on the MCP server unless `ENABLE_WRITES=true` is set.
2. **Dedicated Write Webhook**: Write operations strictly route through `BITRIX_WRITE_WEBHOOK_URL`. The read webhook `BITRIX_WEBHOOK_URL` is never used for writes and remains strictly read-only.
3. **Startup Fail-Safe**: If `ENABLE_WRITES=true` and `BITRIX_WRITE_WEBHOOK_URL` is missing, the server halts immediately at startup.
4. **Strict Write Allowlist**: A hardcoded allowlist permits only:
   - `crm.timeline.comment.add`
   - `crm.activity.todo.add`
   - `crm.item.update`
   - `crm.item.add`
   All destructive methods (`*.delete`), bulk updates, `*.import`, product row replacement, and funnel/stage modifications are rejected with `PermissionError`.
5. **Two-Step Confirmation**:
   - `crm_update_item` and `crm_create_item` default to `confirm=false` (preview mode). They return a structured diff or preview without altering Bitrix24 data.
   - Changes are executed only when explicitly called with `confirm=true`.
6. **Schema Validation & System Field Protection**:
   - Validates all input keys against `crm.item.fields` and rejects unknown fields.
   - Strictly blocks modification of protected system fields (`id`, `createdTime`, `createdBy`, `updatedTime`, `updatedBy`, `webformId`, etc.) and read-only schema fields.
7. **Duplicate Detection**: Before creating contacts or companies, phone and email values are pre-checked via `crm.duplicate.findbycomm` and included in the preview.
8. **Sensitive Data Privacy & Audit Logging**:
   - Webhook tokens and secrets are automatically redacted in server logs (`_safe_url`).
   - Every write operation is logged to the server audit log with tool name, entity type, record ID, and modified field names—**never logging sensitive field values**.

---

## Tool Inventory (24 Tools Total)

### 🛍️ Product Catalog Tools (9 Read-Only)
| Tool | Bitrix24 Method | Purpose |
|------|----------------|---------|
| `list_catalogs` | `catalog.catalog.list` | All trade catalogs + iblockId values |
| `list_sections` | `catalog.section.list` | Product categories / sections |
| `list_products` | `catalog.product.list` | Products with search, filtering & pagination |
| `get_product` | `catalog.product.get` | Single product full detail |
| `list_product_variations` | `catalog.product.list` | SKU/offer variations per product |
| `get_prices` | `catalog.price.list` | All price records per product |
| `get_stock` | `catalog.storeproduct.list` | Per-warehouse inventory stock levels |
| `count_products` | `catalog.product.list` | Fast product count (reads `total` header) |
| `get_total_stock` | `catalog.storeproduct.list` | Total inventory on hand and reserved |

### 🏢 CRM Read Tools (12 Read-Only)
| Tool | Bitrix24 Method | Purpose |
|------|----------------|---------|
| `crm_get_fields` | `crm.item.fields` | Field definitions & labels (includes custom `UF_CRM_*` fields) |
| `crm_list_items` | `crm.item.list` | Universal item query (leads, deals, contacts, companies, SPAs) |
| `crm_get_item` | `crm.item.get` | Complete details for an individual CRM item |
| `crm_count_items` | `crm.item.list` | Fast count of CRM items matching filter criteria |
| `crm_deal_products` | `crm.deal.productrows.get` | Product line items / rows attached to a deal |
| `crm_related` | Graph relationships | Traverses relationships between deals, contacts, and companies |
| `crm_activities` | `crm.activity.list`, `crm.timeline.comment.list` | Tasks, calls, meetings, emails & timeline notes |
| `crm_list_statuses` | `crm.status.list`, `crm.category.list` | Pipeline stages and funnels/categories |
| `crm_list_categories` | `crm.category.list` / `crm.dealcategory.list` | Pipeline sales funnels / categories with entityTypeId |
| `crm_find_duplicates` | `crm.duplicate.findbycomm` | Search duplicates by phone number or email |
| `pipeline_summary` | `crm.item.list` + `crm.status.list` | Aggregate deal count & total monetary value per stage |
| `stage_history` | `crm.stagehistory.list` | Stage transitions and time spent in each stage |

### ✍️ CRM Write Tools (4 Tools — Active when `ENABLE_WRITES=true`)
| Tool | Bitrix24 Method | Purpose & Guardrails |
|------|----------------|----------------------|
| `crm_add_comment` | `crm.timeline.comment.add` | Adds a timeline note to a lead, deal, contact, or company. |
| `crm_add_todo` | `crm.activity.todo.add` | Creates a scheduled follow-up/to-do activity with deadline. |
| `crm_update_item` | `crm.item.update` | Updates fields on a single record. Defaults to `confirm=false` preview diff. Validates field names and rejects system fields. |
| `crm_create_item` | `crm.item.add` | Creates a lead, contact, or company (deals not allowed). Defaults to `confirm=false` preview. Runs duplicate checks for contacts/companies. |

---

## Universal `entity_type` Values
All `crm_*` tools accept either integer IDs or friendly names:
- `1` or `"lead"` — Leads
- `2` or `"deal"` — Deals
- `3` or `"contact"` — Contacts
- `4` or `"company"` — Companies
- `7` or `"quote"` — Quotes
- `31` or `"smart_invoice"` — Smart Invoices
- Custom Smart Process Automation (SPA) integer `entityTypeId` (e.g. 128, 130)

---

## Environment Configuration

Configure in `.env`:

```env
# ── Required ──────────────────────────────────────────────────────────────────
# Read webhook URL (must end with /)
BITRIX_WEBHOOK_URL=https://yourdomain.bitrix24.com/rest/1/readsecret/

# Secret path component for your MCP endpoint:
MCP_SECRET_PATH=your-long-random-secret-string

# ── Optional ──────────────────────────────────────────────────────────────────
PORT=8000

# ── CRM Write Capabilities ────────────────────────────────────────────────────
# Set to 'true' to register the 4 CRM write tools (defaults to false)
ENABLE_WRITES=true

# Dedicated write webhook (required if ENABLE_WRITES=true)
BITRIX_WRITE_WEBHOOK_URL=https://yourdomain.bitrix24.com/rest/1/writesecret/
```

---

## Running the Server

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start local server
python server.py
```

### Health Check Endpoint
```bash
curl http://localhost:8000/health
```
Output:
```json
{"status": "ok", "service": "bitrix24-catalog-crm-mcp", "writes_enabled": true}
```
