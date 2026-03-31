#!/usr/bin/env python3
"""
Fetches FFA Curves Excel from SharePoint, extracts BSI 63 forward curve data,
and writes bsi-data.json. Runs as a GitHub Action or standalone script.

Supports two auth modes:
  1. Public sharing link (no credentials needed)
  2. Azure AD app credentials (for private files) via env vars:
     - AZURE_TENANT_ID
     - AZURE_CLIENT_ID
     - AZURE_CLIENT_SECRET
     - SP_SITE_URL (e.g. https://lightshipcloud-my.sharepoint.com/personal/bsco_lightshipderivatives_com)
     - SP_FILE_PATH (e.g. /personal/bsco_lightshipderivatives_com/Documents/FFA Curves .xlsx)
"""

import json, os, re, struct, sys
from datetime import datetime, timezone
from io import BytesIO
from zipfile import ZipFile
from xml.etree import ElementTree as ET
import urllib.request, urllib.error

# ── SharePoint config ──
SP_SITE = os.environ.get(
    "SP_SITE_URL",
    "https://lightshipcloud-my.sharepoint.com/personal/bsco_lightshipderivatives_com"
)
SP_FILE = os.environ.get(
    "SP_FILE_PATH",
    "/personal/bsco_lightshipderivatives_com/Documents/FFA Curves .xlsx"
)
SP_DOWNLOAD_URL = f"{SP_SITE}/_api/web/GetFileByServerRelativeUrl('{SP_FILE.replace(' ', '%20')}')/$value"

# SharePoint sharing link (fallback for public access)
SP_SHARE_URL = (
    "https://lightshipcloud-my.sharepoint.com/personal/bsco_lightshipderivatives_com/"
    "_layouts/15/download.aspx?sourcedoc=%7Ba462d8e1-43ac-459a-b88e-96836a347fbc%7D"
)

OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "bsi-data.json")


def get_azure_token():
    """Get an OAuth2 token using Azure AD client credentials flow."""
    tenant = os.environ.get("AZURE_TENANT_ID")
    client_id = os.environ.get("AZURE_CLIENT_ID")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET")
    if not all([tenant, client_id, client_secret]):
        return None

    token_url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
    # Scope for SharePoint
    scope = f"{SP_SITE.rstrip('/')}/.default"
    data = (
        f"grant_type=client_credentials"
        f"&client_id={client_id}"
        f"&client_secret={urllib.parse.quote(client_secret)}"
        f"&scope={urllib.parse.quote(scope)}"
    ).encode()

    req = urllib.request.Request(token_url, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        import urllib.parse
        resp = urllib.request.urlopen(req, timeout=15)
        token_data = json.loads(resp.read())
        return token_data.get("access_token")
    except Exception as e:
        print(f"  Azure token error: {e}")
        return None


def download_excel():
    """Download the FFA Curves Excel file. Tries multiple methods."""

    # Method 1: Azure AD app credentials (most reliable)
    print("Trying Azure AD app credentials...")
    token = get_azure_token()
    if token:
        try:
            req = urllib.request.Request(SP_DOWNLOAD_URL)
            req.add_header("Authorization", f"Bearer {token}")
            req.add_header("Accept", "application/octet-stream")
            resp = urllib.request.urlopen(req, timeout=30)
            data = resp.read()
            if len(data) > 1000:
                print(f"  Success via Azure AD! ({len(data)} bytes)")
                return data
        except Exception as e:
            print(f"  Azure AD download failed: {e}")

    # Method 2: Public sharing link (if file is shared as "Anyone with the link")
    print("Trying public sharing link...")
    try:
        req = urllib.request.Request(SP_SHARE_URL)
        req.add_header("User-Agent", "Mozilla/5.0")
        resp = urllib.request.urlopen(req, timeout=30)
        data = resp.read()
        if len(data) > 1000 and data[:2] == b"PK":  # Valid ZIP/XLSX
            print(f"  Success via public link! ({len(data)} bytes)")
            return data
        else:
            print(f"  Got {len(data)} bytes but doesn't look like XLSX")
    except Exception as e:
        print(f"  Public link failed: {e}")

    # Method 3: Graph API shares endpoint (for shared links)
    print("Trying Microsoft Graph shares endpoint...")
    import base64
    share_url = (
        "https://lightshipcloud-my.sharepoint.com/:x:/r/personal/"
        "bsco_lightshipderivatives_com/_layouts/15/doc2.aspx"
        "?sourcedoc=%7Ba462d8e1-43ac-459a-b88e-96836a347fbc%7D&action=view"
    )
    encoded = base64.b64encode(share_url.encode()).decode().rstrip("=").replace("/", "_").replace("+", "-")
    share_id = "u!" + encoded
    graph_url = f"https://graph.microsoft.com/v1.0/shares/{share_id}/driveItem/content"

    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        req = urllib.request.Request(graph_url, headers=headers)
        resp = urllib.request.urlopen(req, timeout=30)
        data = resp.read()
        if len(data) > 1000:
            print(f"  Success via Graph API! ({len(data)} bytes)")
            return data
    except Exception as e:
        print(f"  Graph API failed: {e}")

    return None


def parse_xlsx(data):
    """Parse XLSX and extract BSI 63 forward curve values from column S of Curves sheet."""
    with ZipFile(BytesIO(data)) as zf:
        # Find which sheet is "Curves"
        wb_xml = zf.read("xl/workbook.xml")
        rels_xml = zf.read("xl/_rels/workbook.xml.rels")

        # Parse workbook to find sheet names
        ns_wb = {"": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
        wb_root = ET.fromstring(wb_xml)
        sheets = {}
        for s in wb_root.iter("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}sheet"):
            name = s.get("name")
            r_id = s.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
            sheets[r_id] = name

        # Parse rels to find sheet file paths
        ns_rel = {"": "http://schemas.openxmlformats.org/package/2006/relationships"}
        rels_root = ET.fromstring(rels_xml)
        rels = {}
        for r in rels_root.iter("{http://schemas.openxmlformats.org/package/2006/relationships}Relationship"):
            rels[r.get("Id")] = r.get("Target")

        # Find Curves sheet file
        curves_file = None
        for r_id, name in sheets.items():
            if name == "Curves":
                curves_file = "xl/" + rels[r_id]
                break

        if not curves_file:
            raise ValueError(f"Sheet 'Curves' not found. Available sheets: {list(sheets.values())}")

        # Parse shared strings
        shared_strings = []
        try:
            ss_xml = zf.read("xl/sharedStrings.xml")
            ss_root = ET.fromstring(ss_xml)
            ns_ss = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
            for si in ss_root.iter(f"{{{ns_ss}}}si"):
                text_parts = []
                for t in si.iter(f"{{{ns_ss}}}t"):
                    if t.text:
                        text_parts.append(t.text)
                shared_strings.append("".join(text_parts))
        except KeyError:
            pass

        # Parse the Curves sheet
        sheet_xml = zf.read(curves_file)
        sheet_root = ET.fromstring(sheet_xml)
        ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"

        # Extract target cells
        target_cells = {"S4", "S5", "S6", "S7", "S8", "S9", "S10", "S11", "S12", "S13", "S14", "S15"}
        cell_values = {}

        for row in sheet_root.iter(f"{{{ns}}}row"):
            for c in row.iter(f"{{{ns}}}c"):
                ref = c.get("r")
                if ref not in target_cells:
                    continue
                cell_type = c.get("t", "n")
                v_elem = c.find(f"{{{ns}}}v")
                if v_elem is None or v_elem.text is None:
                    cell_values[ref] = None
                    continue
                if cell_type == "s":
                    idx = int(v_elem.text)
                    cell_values[ref] = shared_strings[idx] if idx < len(shared_strings) else None
                else:
                    try:
                        cell_values[ref] = float(v_elem.text)
                    except ValueError:
                        cell_values[ref] = None

        # Build BSI 63 array: [Mar, Apr, May, Jun, Jul, Q3, Q4, Q1-27, Cal27, Cal28, Cal29]
        # S9 (Q2-26) is skipped
        bsi = [
            cell_values.get("S4"),   # Mar
            cell_values.get("S5"),   # Apr
            cell_values.get("S6"),   # May
            cell_values.get("S7"),   # Jun
            cell_values.get("S8"),   # Jul
            cell_values.get("S10"),  # Q3 2026
            cell_values.get("S11"),  # Q4 2026
            cell_values.get("S12"),  # Q1 2027
            cell_values.get("S13"),  # Cal 27
            cell_values.get("S14"),  # Cal 28
            cell_values.get("S15"),  # Cal 29
        ]

        # Convert to int where possible
        bsi = [int(v) if v is not None and v == int(v) else v for v in bsi]

        return bsi


def main():
    print(f"=== BSI 63 Sync - {datetime.now(timezone.utc).isoformat()} ===")

    # Download
    data = download_excel()
    if not data:
        print("ERROR: Could not download FFA Curves Excel from any source.")
        print("Set AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET as GitHub secrets.")
        sys.exit(1)

    # Parse
    print("Parsing Excel...")
    bsi_values = parse_xlsx(data)
    print(f"  Extracted BSI 63 values: {bsi_values}")

    non_null = sum(1 for v in bsi_values if v is not None)
    if non_null < 5:
        print(f"ERROR: Only {non_null} non-null values found. Something is wrong.")
        sys.exit(1)

    # Build JSON
    result = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "bsiValues": bsi_values,
        "labels": ["Mar", "Apr", "May", "Jun", "Jul", "Q3 2026", "Q4 2026", "Q1 2027", "Cal 27", "Cal 28", "Cal 29"]
    }

    # Check if data has changed
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r") as f:
                existing = json.load(f)
            if existing.get("bsiValues") == bsi_values:
                print("Data unchanged, skipping update.")
                return
        except Exception:
            pass

    # Write
    with open(OUTPUT_FILE, "w") as f:
        json.dump(result, f, indent=2)

    print(f"  Written to {OUTPUT_FILE}")
    print("  Done!")


if __name__ == "__main__":
    main()
