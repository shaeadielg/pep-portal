"""
pep_process.py
--------------
Reads a Sent_POs export from currentorders\, fetches prices from the
Pepstores portal, and outputs pepcombined.xls formatted identically to
Rachit's macro, with one extra column at the end for price.

Folder structure:
  <script_dir>\
    currentorders\   <- drop portal export here
    oldorders\       <- archived files land here
    pepcombined.xls  <- output (FoxPro-ready + price column)

Usage:
    python pep_process.py
"""

import requests
import json
import time
import os
import sys
import glob
import shutil
import datetime

import pandas as pd
import xlwt

from dotenv import load_dotenv
import os

load_dotenv()

username = os.getenv("DEFAULT_USERNAME")
password = os.getenv("DEFAULT_PASSWORD")

# --- CONFIGURATION ------------------------------------------------------------
USERNAME   = username
PASSWORD   = password

BASE_URL   = "https://pepstores-prod.centricsoftware.com"
API_URL    = f"{BASE_URL}/csi-requesthandler/RequestHandler"
THROTTLE   = 0.3
BATCH_SIZE = 20

SCRIPT_DIR     = os.path.dirname(os.path.abspath(__file__))
CURRENT_FOLDER = os.path.join(SCRIPT_DIR, "currentorders")
OLD_FOLDER     = os.path.join(SCRIPT_DIR, "oldorders")
OUTPUT_FILE    = os.path.join(SCRIPT_DIR, "pepcombined.xls")

# Columns to convert to DD MM YYYY text — matched by name, not position
DATE_COLS_BY_NAME = {"Shipment Booking date", "To Into DC Date", "PEP Sent Date"}

# Columns to keep as Excel serial numbers — matched by name
SERIAL_DATE_COLS  = {"Ship From Date", "Ship to Date", "From Into DC Date", "Into Store Date"}

# Rachit's exact 28 column names in order — we enforce this regardless of export
RACHIT_COLUMNS = [
    "Supplier", "", "Supplier PO", "Status (Display)", "Promo",
    "MMS Update Response", "PEP Buyer", "Style Code", "Style", "CC",
    "Season", "Factory", "--Latest Qty", "Ship From Date", "Ship to Date",
    "Shipment Booking date", "Loading Port Name", "DC", "From Into DC Date",
    "To Into DC Date", "Into Store Date", "Reject PI Reason", "PEP Sent Date",
    "Buyer Comments", "Supplier Comments", "Artwork Submission Ref No",
    "Latest Character Approved Stage", "Latest Status Date",
]

# Right-align headers for these column names
RIGHT_ALIGN_HEADER_NAMES = {
    "--Latest Qty", "Ship From Date", "Ship to Date", "From Into DC Date",
    "To Into DC Date", "Into Store Date", "PEP Sent Date", "Latest Status Date"
}
# ------------------------------------------------------------------------------


def api_post(session, payload):
    params  = {"request.preventCache": str(int(time.time() * 1000))}
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer":      f"{BASE_URL}/WebAccess/home.html",
    }
    resp = session.post(API_URL, params=params, data=payload,
                        headers=headers, timeout=120)
    resp.raise_for_status()
    text = resp.text.strip().lstrip("(").rstrip(")")
    return json.loads(text)


def login(session):
    try:
        data = api_post(session, {
            "Fmt.Version": "2",
            "LoginID":     USERNAME,
            "Password":    PASSWORD,
            "Module":      "DataSource",
            "Operation":   "SimpleLogin",
            "OutputJSON":  "1",
        })
        if data.get("Status") == "Successful":
            print("[+] Login successful")
            return True
        print(f"[-] Login failed: {data.get('Status')}")
        return False
    except Exception as e:
        print(f"[-] Login error: {e}")
        return False


def fetch_all_sent_pos(session):
    qry_xml = (
        '<?xml version="1.0" encoding="utf-8" ?>'
        '<Query>'
        '<Node Parameter="Type" Op="EQ" Value="PurchasedOrder"/>'
        '<Attribute Id="State" Op="NE" SValue="PurchasedOrderState:Abandoned"/>'
        '<Attribute Id="po_display_status" Op="EQ" SValue="SENT"/>'
        '</Query>'
    )
    data = api_post(session, {
        "Fmt.Version":   "2",
        "Fmt.AC.Rights": "Current",
        "Fmt.Attr.Info": "Mid",
        "Module":        "Search",
        "Operation":     "QueryByXML",
        "OutputJSON":    "1",
        "Qry.XML":       qry_xml,
    })
    nodes = data.get("NODES", {}).get("ResultNode", [])
    print(f"[+] Found {len(nodes)} SENT POs on portal")
    return nodes


def fetch_prices_batch(session, url_list):
    """Returns { (po_number, style_code): price_float }"""
    price_map     = {}
    total_batches = -(-len(url_list) // BATCH_SIZE)

    for i in range(0, len(url_list), BATCH_SIZE):
        batch = url_list[i:i + BATCH_SIZE]
        print(f"  Batch {i // BATCH_SIZE + 1}/{total_batches}...")

        payload_list = [
            ("Fmt.Version",   "2"),
            ("Fmt.AC.Rights", "Current"),
            ("Fmt.Attr.Info", "Mid"),
            ("Module",        "Search"),
            ("Operation",     "QueryByURL"),
            ("OutputJSON",    "1"),
        ]
        for url in batch:
            payload_list.append(("Qry.URL", url))

        try:
            data  = api_post(session, payload_list)
            nodes = data.get("NODES", {}).get("ResultNode", [])
            for node in nodes:
                po_num     = str(node.get("$Name", "")).strip()
                style_code = str(node.get("p_purchasedorder_style_code", "")).strip()
                price      = node.get("p_po_local_avg_cost_price")
                if price is None:
                    price = node.get("p_po_latest_becp")
                if po_num and style_code:
                    price_map[(po_num, style_code)] = (
                        float(price) if price is not None else None
                    )
        except Exception as e:
            print(f"  [!] Batch error: {e}")

        time.sleep(THROTTLE)

    return price_map


def to_ddmmyyyy(val):
    """Convert any date value to DD/MM/YYYY plain text string."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    if isinstance(val, str):
        val = val.lstrip("'").strip()
        if not val:
            return ""
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                return datetime.datetime.strptime(val, fmt).strftime("%d %m %Y")
            except ValueError:
                continue
        return val
    if hasattr(val, "strftime"):
        return val.strftime("%d %m %Y")
    return str(val)


def find_source_file():
    skip = {"pepcombined.xls", "pepcombined.xlsx"}
    files = [
        f for f in glob.glob(os.path.join(CURRENT_FOLDER, "*.xls*"))
        if os.path.basename(f).lower() not in skip
    ]
    return files[0] if files else None


_last_archive = ""  # Exposed for GUI summary messagebox

def archive_files(source_file, timestamp):
    global _last_archive
    prefix   = f"IMP{timestamp}_"
    src_name = os.path.basename(source_file)
    out_name = os.path.basename(OUTPUT_FILE)

    dest_src = os.path.join(OLD_FOLDER, prefix + src_name)
    try:
        shutil.move(source_file, dest_src)
        print(f"[+] Moved   : {src_name} -> oldorders\\{prefix + src_name}")
    except Exception as e:
        print(f"[!] Warning : could not move source file - {e}")

    dest_out = os.path.join(OLD_FOLDER, prefix + out_name)
    _last_archive = dest_out
    try:
        shutil.copy2(OUTPUT_FILE, dest_out)
        print(f"[+] Archived: {out_name} -> oldorders\\{prefix + out_name}")
    except Exception as e:
        print(f"[!] Warning : could not archive output file - {e}")


def write_xls(df, output_path, group_label=''):
    """
    Write DataFrame to Excel 95 (.xls) matching Rachit's exact format:
    - Sheet name: Sheet
    - Font: Calibri 11pt, colour #333333
    - Header row 1: bold, white fill, dark grey text, left-aligned
      (right-aligned for numeric/date cols)
    - Row 2: blank (group header row from original export)
    - Data rows 3+: no fill, Calibri 11pt #333333, left-aligned
    - Price column at end: same data style
    """
    wb = xlwt.Workbook()
    ws = wb.add_sheet("Sheet")

    # ── Colour palette tweaks ─────────────────────────────────────────────────
    # xlwt colour index 0x08 onwards are custom — patch index 8 to #333333
    # xlwt has limited palette; closest built-in to #333333 is black (index 0)
    # We'll use black for text and white fill for headers to match exactly

    DARK_GREY  = 0x08   # We'll redefine this slot
    WHITE      = 0x01
    NO_FILL    = 0x00

    # Patch the palette: slot 8 = #333333
    xlwt.add_palette_colour("dark_grey_333", DARK_GREY)
    wb.set_colour_RGB(DARK_GREY, 51, 51, 51)

    # ── Style builders ────────────────────────────────────────────────────────
    def make_style(bold=False, font_colour=DARK_GREY, fill_colour=None,
                   fill_pattern=0, h_align=1, num_format=None):
        style = xlwt.XFStyle()

        font = xlwt.Font()
        font.name   = "Calibri"
        font.height = 220          # 11pt = 220 in xlwt units (20 * point size)
        font.bold   = bold
        font.colour_index = font_colour
        style.font  = font

        align = xlwt.Alignment()
        align.horz  = h_align      # 1=left, 2=centre, 3=right
        align.vert  = xlwt.Alignment.VERT_BOTTOM
        style.alignment = align

        if fill_colour is not None:
            pat = xlwt.Pattern()
            pat.pattern = fill_pattern if fill_pattern else xlwt.Pattern.SOLID_PATTERN
            pat.pattern_fore_colour = fill_colour
            style.pattern = pat

        if num_format:
            style.num_format_str = num_format

        return style

    # Header: bold, dark grey text, white fill, left-aligned (or right for numeric)
    hdr_left  = make_style(bold=True,  font_colour=DARK_GREY,
                           fill_colour=WHITE, fill_pattern=1, h_align=1)
    hdr_right = make_style(bold=True,  font_colour=DARK_GREY,
                           fill_colour=WHITE, fill_pattern=1, h_align=3)

    # Data: normal, dark grey, no fill, left-aligned
    data_style = make_style(bold=False, font_colour=DARK_GREY, h_align=1)

    # Price column data style
    price_style = make_style(bold=False, font_colour=DARK_GREY, h_align=3,
                             num_format="R#,##0.0000")

    # Qty column
    qty_style   = make_style(bold=False, font_colour=DARK_GREY, h_align=3,
                             num_format="#,##0")

    # ── Row 1: Headers ────────────────────────────────────────────────────────
    for col_idx, col_name in enumerate(df.columns):
        style = hdr_right if col_name in RIGHT_ALIGN_HEADER_NAMES else hdr_left
        ws.write(0, col_idx, col_name, style)

    # ── Row 2: Blank group row (matches Rachit's structure) ───────────────────
    # Col A gets the group label from original export row 2
    ws.write(1, 0, group_label, data_style)
    for col_idx in range(1, len(df.columns)):
        ws.write(1, col_idx, "", data_style)

    # ── Rows 3+: Data ─────────────────────────────────────────────────────────
    for row_idx, row in enumerate(df.itertuples(index=False), start=2):
        for col_idx, val in enumerate(row):
            col_name = df.columns[col_idx]

            # Normalise NaN/None to empty string
            if val is None or (isinstance(val, float) and pd.isna(val)):
                ws.write(row_idx, col_idx, "", data_style)
            elif col_name == "Supplier Cost Price (ZAR)":
                ws.write(row_idx, col_idx,
                         val if val is not None else "", price_style)
            elif col_name == "--Latest Qty":
                try:
                    ws.write(row_idx, col_idx, int(val), qty_style)
                except:
                    ws.write(row_idx, col_idx, str(val), data_style)
            elif hasattr(val, "strftime"):
                # Timestamp — write as Excel serial number to match Rachit's format
                import datetime
                epoch = datetime.datetime(1899, 12, 30)
                serial = (val.to_pydatetime().replace(tzinfo=None) - epoch).days
                ws.write(row_idx, col_idx, serial, data_style)
            elif isinstance(val, float) and (val != val):
                ws.write(row_idx, col_idx, "", data_style)
            else:
                ws.write(row_idx, col_idx, str(val) if val is not None else "", data_style)

    wb.save(output_path)


def main():
    # ── Validate folders ──────────────────────────────────────────────────────
    for folder in [CURRENT_FOLDER, OLD_FOLDER]:
        if not os.path.isdir(folder):
            print(f"[-] Folder not found: {folder}")
            sys.exit(1)

    # ── Find source file ──────────────────────────────────────────────────────
    source_file = find_source_file()
    if not source_file:
        print(f"[-] No Excel files found in: {CURRENT_FOLDER}")
        sys.exit(1)

    print(f"[+] Processing: {os.path.basename(source_file)}")

    # ── Read spreadsheet — skip All/All filter row (row index 1 = excel row 2)
    try:
        # Read raw first to capture the group row value (row 2 in Excel)
        df_raw      = pd.read_excel(source_file, header=None)
        group_label = str(df_raw.iloc[1, 0]) if len(df_raw) > 1 else ""
        if group_label in ("nan", "None"):
            group_label = ""

        df = pd.read_excel(source_file, header=0, skiprows=[1],
                           dtype={"Supplier PO": str})
    except Exception as e:
        print(f"[-] Error reading file: {e}")
        sys.exit(1)

    # Clean PO and Style Code columns
    df["Supplier PO"] = (df["Supplier PO"]
                         .astype(str).str.strip()
                         .str.replace(r"\.0$", "", regex=True))
    df["Style Code"]  = df["Style Code"].astype(str).str.strip()

    if df.empty:
        print("[-] File has no data rows.")
        sys.exit(1)

    print(f"[+] Read {len(df)} rows from export")

    # ── Login + fetch prices ──────────────────────────────────────────────────
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36"
    })

    if not login(session):
        sys.exit(1)

    po_nodes = fetch_all_sent_pos(session)
    if not po_nodes:
        print("[-] No POs returned from portal.")
        sys.exit(1)

    po_to_url = {}
    for node in po_nodes:
        po_num = str(node.get("$Name", "")).strip()
        url    = node.get("p_po_url") or node.get("$URL", "")
        if po_num and url and not url.startswith("centric://"):
            po_to_url[po_num] = url

    print(f"[+] Fetching prices ({len(po_to_url)} POs)...")
    price_map = fetch_prices_batch(session, list(po_to_url.values()))
    print(f"[+] Price keys built: {len(price_map)}")

    # ── Match prices ──────────────────────────────────────────────────────────
    def lookup_price(row):
        key = (str(row["Supplier PO"]).strip(), str(row["Style Code"]).strip())
        return price_map.get(key)

    matched   = df.apply(lookup_price, axis=1).notna().sum()
    unmatched = df.apply(lookup_price, axis=1).isna().sum()
    print(f"[+] Matched: {matched} | Unmatched: {unmatched}")

    # ── Enforce Rachit's exact 28-column structure ───────────────────────────
    # Rename Unnamed: 1 -> blank first
    df.rename(columns={"Unnamed: 1": ""}, inplace=True)

    # Keep only Rachit's columns, in his order, filling missing ones with blank
    for col in RACHIT_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df = df[RACHIT_COLUMNS].copy()

    # ── Add price column at end (after column enforcement) ───────────────────
    df["Supplier Cost Price (ZAR)"] = df.apply(lookup_price, axis=1)

    # ── Convert date columns by name ─────────────────────────────────────────
    date_col_names = [c for c in DATE_COLS_BY_NAME if c in df.columns]
    for col in date_col_names:
        df[col] = df[col].apply(to_ddmmyyyy)

    # ── Write output ──────────────────────────────────────────────────────────
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    write_xls(df, OUTPUT_FILE, group_label)
    print(f"[+] Output: {OUTPUT_FILE}")

    # ── Archive ───────────────────────────────────────────────────────────────
    archive_files(source_file, timestamp)

    print(f"\n-- Complete ----------------------------------------")
    print(f"  Records processed   : {len(df)}")
    print(f"  Prices matched      : {matched}")
    print(f"  Unmatched           : {unmatched}")
    print(f"  Date cols converted : {', '.join(date_col_names)}")
    print(f"  Output              : pepcombined.xls")
    print(f"  Timestamp           : {timestamp}")
    print(f"----------------------------------------------------")


if __name__ == "__main__":
    main()
