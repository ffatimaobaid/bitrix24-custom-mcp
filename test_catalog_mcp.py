#!/usr/bin/env python3
"""
test_catalog_mcp.py
====================
Read-only and safety guardrail verification test script for Bitrix24 MCP Server.
Tests read queries against Bitrix24 and local guardrail logic.

NO records are created, modified, or commented on.
All live write tests have been completely removed.
"""

import asyncio
import json
import logging
import os
import sys
from pathlib import Path

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

# Silence httpx and httpcore loggers to prevent leaking webhook tokens in output
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# Import read tools and validation functions directly from server
from server import (
    _validate_read_method,
    _validate_write_method,
    # Catalog tools
    list_catalogs,
    list_sections,
    list_products,
    get_product,
    list_product_variations,
    get_prices,
    get_stock,
    count_products,
    get_total_stock,
    # CRM read tools
    crm_get_fields,
    crm_list_items,
    crm_get_item,
    crm_count_items,
    crm_deal_products,
    crm_related,
    crm_activities,
    crm_list_statuses,
    crm_list_categories,
    crm_find_duplicates,
    pipeline_summary,
    stage_history,
)

WEBHOOK = os.environ.get("BITRIX_WEBHOOK_URL", "").rstrip("/")
if not WEBHOOK:
    print("ERROR: BITRIX_WEBHOOK_URL is not set in environment or .env")
    sys.exit(1)

_PASS = "\033[92m[PASS]\033[0m"
_FAIL = "\033[91m[FAIL]\033[0m"
_SKIP = "\033[93m[SKIP]\033[0m"

results: list[tuple[str, bool, str]] = []


def ok(name: str, detail: str = "") -> None:
    results.append((name, True, detail))
    print(f"  {_PASS} {name}" + (f": {detail}" if detail else ""))


def fail(name: str, detail: str) -> None:
    results.append((name, False, detail))
    print(f"  {_FAIL} {name}: {detail}")


def skip(name: str, reason: str) -> None:
    results.append((name, None, reason))
    print(f"  {_SKIP} {name}: {reason}")


async def main() -> None:
    print("\nBitrix24 Catalog & CRM MCP - Read & Guardrail Verification Suite")
    print("NOTE: All live write operations (create, update, comment, to-do) are omitted.")
    print("No records will be created or modified in Bitrix24.\n")

    first_product_id: int | None = None
    first_iblock_id: int | None = None
    first_deal_id: int | None = None

    # =======================================================================
    # PART 1: CATALOG TOOLS (READ ONLY)
    # =======================================================================
    print("--- Catalog Tools ---")

    # 1. list_catalogs
    print("1. list_catalogs")
    try:
        raw = await list_catalogs()
        data = json.loads(raw)
        catalogs = data.get("catalogs", [])
        if catalogs:
            first_iblock_id = catalogs[0].get("iblockId") or catalogs[0].get("id")
            ok("list_catalogs", f"{len(catalogs)} catalog(s) found. First iblockId={first_iblock_id}")
        else:
            ok("list_catalogs", "0 catalogs found")
    except Exception as e:
        fail("list_catalogs", str(e))

    # 2. list_sections
    print("2. list_sections")
    if first_iblock_id is None:
        skip("list_sections", "No iblockId available")
    else:
        try:
            raw = await list_sections(iblock_id=first_iblock_id, limit=10)
            data = json.loads(raw)
            sections = data.get("sections", [])
            ok("list_sections", f"{len(sections)} section(s) returned for catalog {first_iblock_id}")
        except Exception as e:
            fail("list_sections", str(e))

    # 3. list_products
    print("3. list_products")
    if first_iblock_id is None:
        skip("list_products", "No iblockId available")
    else:
        try:
            raw = await list_products(iblock_id=first_iblock_id, limit=10)
            data = json.loads(raw)
            products = data.get("products", [])
            total = data.get("total", len(products))
            if products:
                first_product_id = products[0].get("id")
            ok("list_products", f"total={total}, returned={len(products)}, first product_id={first_product_id}")
        except Exception as e:
            fail("list_products", str(e))

    # 4. get_product
    print("4. get_product")
    if first_product_id is None:
        skip("get_product", "No product_id available")
    else:
        try:
            raw = await get_product(product_id=first_product_id)
            data = json.loads(raw)
            p = data.get("product", {})
            ok("get_product", f"id={p.get('id')}, name={p.get('name')}")
        except Exception as e:
            fail("get_product", str(e))

    # 5. list_product_variations
    print("5. list_product_variations")
    if first_product_id is None:
        skip("list_product_variations", "No product_id available")
    else:
        try:
            raw = await list_product_variations(product_id=first_product_id)
            data = json.loads(raw)
            variations = data.get("variations", [])
            ok("list_product_variations", f"{len(variations)} variation offer(s) found")
        except Exception as e:
            fail("list_product_variations", str(e))

    # 6. get_prices
    print("6. get_prices")
    if first_product_id is None:
        skip("get_prices", "No product_id available")
    else:
        try:
            raw = await get_prices(product_id=first_product_id)
            data = json.loads(raw)
            prices = data.get("prices", [])
            ok("get_prices", f"{len(prices)} price record(s) for product {first_product_id}")
        except Exception as e:
            fail("get_prices", str(e))

    # 7. get_stock
    print("7. get_stock")
    if first_product_id is None:
        skip("get_stock", "No product_id available")
    else:
        try:
            raw = await get_stock(product_id=first_product_id)
            data = json.loads(raw)
            stores = data.get("stores", [])
            ok("get_stock", f"{len(stores)} store record(s) for product {first_product_id}")
        except Exception as e:
            fail("get_stock", str(e))

    # 8. count_products
    print("8. count_products")
    if first_iblock_id is None:
        skip("count_products", "No iblockId available")
    else:
        try:
            raw = await count_products(iblock_id=first_iblock_id)
            data = json.loads(raw)
            ok("count_products", f"count={data.get('count')} products in catalog {first_iblock_id}")
        except Exception as e:
            fail("count_products", str(e))

    # 9. get_total_stock
    print("9. get_total_stock")
    if first_product_id is None:
        skip("get_total_stock", "No product_id available")
    else:
        try:
            raw = await get_total_stock(product_id=first_product_id)
            data = json.loads(raw)
            ok("get_total_stock", f"total_qty={data.get('total_quantity')}, total_reserved={data.get('total_reserved')}")
        except Exception as e:
            fail("get_total_stock", str(e))

    # =======================================================================
    # PART 2: CRM READ TOOLS
    # =======================================================================
    print("\n--- CRM Read Tools ---")

    # 10. crm_get_fields
    print("10. crm_get_fields")
    try:
        raw = await crm_get_fields(entity_type="deal")
        data = json.loads(raw)
        ok("crm_get_fields", f"entity={data.get('entity')}, total_fields={data.get('total_fields')}")
    except Exception as e:
        fail("crm_get_fields", str(e))

    # 11. crm_list_items
    print("11. crm_list_items")
    try:
        raw = await crm_list_items(entity_type="deal", limit=10)
        data = json.loads(raw)
        items = data.get("items", [])
        total = data.get("total", len(items))
        if items:
            first_deal_id = items[0].get("id")
        ok("crm_list_items", f"entity=deal, total={total}, returned={len(items)}, first_deal_id={first_deal_id}")
    except Exception as e:
        fail("crm_list_items", str(e))

    # 12. crm_get_item
    print("12. crm_get_item")
    if first_deal_id is None:
        skip("crm_get_item", "No deal_id available")
    else:
        try:
            raw = await crm_get_item(entity_type="deal", id=first_deal_id)
            data = json.loads(raw)
            item = data.get("item", {})
            ok("crm_get_item", f"id={item.get('id')}, title={item.get('title')}")
        except Exception as e:
            fail("crm_get_item", str(e))

    # 13. crm_count_items
    print("13. crm_count_items")
    try:
        raw = await crm_count_items(entity_type="deal")
        data = json.loads(raw)
        ok("crm_count_items", f"entity={data.get('entity')}, count={data.get('count')}")
    except Exception as e:
        fail("crm_count_items", str(e))

    # 14. crm_deal_products
    print("14. crm_deal_products")
    if first_deal_id is None:
        skip("crm_deal_products", "No deal_id available")
    else:
        try:
            raw = await crm_deal_products(deal_id=first_deal_id)
            data = json.loads(raw)
            ok("crm_deal_products", f"deal_id={first_deal_id}, total_product_rows={data.get('total_rows')}")
        except Exception as e:
            fail("crm_deal_products", str(e))

    # 15. crm_related
    print("15. crm_related")
    if first_deal_id is None:
        skip("crm_related", "No deal_id available")
    else:
        try:
            raw = await crm_related(entity_type="deal", id=first_deal_id)
            data = json.loads(raw)
            ok("crm_related", f"deal_id={first_deal_id}, linked_contacts={len(data.get('contacts', []))}, linked_companies={len(data.get('companies', []))}")
        except Exception as e:
            fail("crm_related", str(e))

    # 16. crm_activities
    print("16. crm_activities")
    try:
        raw = await crm_activities(limit=10)
        data = json.loads(raw)
        ok("crm_activities", f"total={data.get('total')}, returned={len(data.get('activities', []))}")
    except Exception as e:
        fail("crm_activities", str(e))

    # 17. crm_list_statuses
    print("17. crm_list_statuses")
    try:
        raw = await crm_list_statuses(entity_type="deal")
        data = json.loads(raw)
        ok("crm_list_statuses", f"statuses={len(data.get('statuses', []))}, categories={len(data.get('categories', []))}")
    except Exception as e:
        fail("crm_list_statuses", str(e))

    # 18. crm_list_categories (validates entityTypeId resolution)
    print("18. crm_list_categories")
    try:
        raw = await crm_list_categories(entity_type="deal")
        data = json.loads(raw)
        ok("crm_list_categories", f"categories={len(data.get('categories', []))}, entityTypeId={data.get('entity_type_id')}")
    except Exception as e:
        fail("crm_list_categories", str(e))

    # 19. crm_find_duplicates (Read search only)
    print("19. crm_find_duplicates")
    try:
        raw = await crm_find_duplicates(type="EMAIL", values=["probe@example.com"])
        data = json.loads(raw)
        ok("crm_find_duplicates", f"searched_values={len(data.get('searched_values', []))}")
    except Exception as e:
        fail("crm_find_duplicates", str(e))

    # 20. pipeline_summary
    print("20. pipeline_summary")
    try:
        raw = await pipeline_summary()
        data = json.loads(raw)
        ok("pipeline_summary", f"total_deals={data.get('total_deals')}, pipeline_value={data.get('total_pipeline_value')} {data.get('currency')}")
    except Exception as e:
        fail("pipeline_summary", str(e))

    # 21. stage_history
    print("21. stage_history")
    try:
        raw = await stage_history(entity_type="deal", owner_id=first_deal_id)
        data = json.loads(raw)
        ok("stage_history", f"transitions={data.get('transitions_count')}")
    except Exception as e:
        fail("stage_history", str(e))

    # =======================================================================
    # PART 3: SECURITY GUARDS & VALIDATION TESTS (NO LIVE WRITES)
    # =======================================================================
    print("\n--- Security Guards & Offline Safety Rails ---")

    # 22. Read-Only Guard Verification
    print("22. Read-Only Guard Verification (reject write methods on read client)")
    try:
        _validate_read_method("crm.deal.add")
        fail("read_guard_add", "Failed to reject crm.deal.add write operation on read client!")
    except PermissionError:
        try:
            _validate_read_method("crm.item.delete")
            fail("read_guard_delete", "Failed to reject crm.item.delete write operation on read client!")
        except PermissionError:
            ok("read_guard", "Read client strictly rejects .add and .delete operations")

    # 23. Write Allowlist Rejection
    print("23. Write Allowlist Verification (reject delete, import, stage changes)")
    try:
        _validate_write_method("crm.item.delete")
        fail("write_guard_delete", "Allowed crm.item.delete which is not on write allowlist!")
    except (PermissionError, RuntimeError):
        try:
            _validate_write_method("crm.deal.productrows.set")
            fail("write_guard_productrows_set", "Allowed crm.deal.productrows.set which is out of scope!")
        except (PermissionError, RuntimeError):
            try:
                _validate_write_method("crm.status.add")
                fail("write_guard_status_add", "Allowed crm.status.add which alters pipeline funnels!")
            except (PermissionError, RuntimeError):
                ok("write_allowlist", "Write allowlist rejects delete, batch, and stage/settings mutations")

    # =======================================================================
    # Summary
    # =======================================================================
    print("\n" + "=" * 60)
    passed = sum(1 for _, s, _ in results if s is True)
    failed = sum(1 for _, s, _ in results if s is False)
    skipped = sum(1 for _, s, _ in results if s is None)
    print(f"Results: {passed} passed, {failed} failed, {skipped} skipped")
    print("No records were created or modified in Bitrix24.")
    print("=" * 60 + "\n")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
