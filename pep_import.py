"""
pep_import.py
-------------
Joshtex - PEP Import
Northern Textile Mills SA (PTY) Ltd

Reads a Sent_POs export from currentorders\\, fetches prices from the
Pepstores portal, formats output identically to the existing macro
(Excel 95, date conversions), adds a Supplier Cost Price column at the
end, then archives files.

Credentials stored in P:\\permissions.db (table: pep_import).
Admin password required to update credentials.

Usage: python pep_import.py  (or double-click the compiled exe)
"""

# ── Imports ────────────────────────────────────────────────────────────────────
import tkinter as tk
from tkinter import scrolledtext, messagebox, simpledialog
import threading
import sqlite3
import requests
import json
import time
import os
import sys
import glob
import shutil
import datetime
import queue

import pandas as pd
import xlwt

from dotenv import load_dotenv
import os

load_dotenv()

username = os.getenv("DEFAULT_USERNAME")
password = os.getenv("DEFAULT_PASSWORD")
admin_pass = os.getenv("ADMIN_PASS")

# ── Configuration ──────────────────────────────────────────────────────────────
# When compiled with PyInstaller --onefile, __file__ points to the temp
# extraction folder. sys.executable gives the actual exe location instead.
if getattr(sys, "frozen", False):
    SCRIPT_DIR = os.path.dirname(os.path.abspath(sys.executable))
else:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH      = r"P:\permissions.db"
LOG_DIR      = os.path.join(SCRIPT_DIR, "logs")
ADMIN_PASS   = admin_pass
APP_NAME     = "Pep Import"

DEFAULT_USER = username
DEFAULT_PASS = password

BASE_URL     = "https://pepstores-prod.centricsoftware.com"
API_URL      = f"{BASE_URL}/csi-requesthandler/RequestHandler"
THROTTLE     = 0.3
BATCH_SIZE   = 20

CURRENT_FOLDER = os.path.join(SCRIPT_DIR, "currentorders")
OLD_FOLDER     = os.path.join(SCRIPT_DIR, "oldorders")
OUTPUT_FILE    = os.path.join(SCRIPT_DIR, "pepcombined.xls")

DATE_COLS_BY_NAME = {"Shipment Booking date", "To Into DC Date", "PEP Sent Date"}
SERIAL_DATE_COLS  = {"Ship From Date", "Ship to Date", "From Into DC Date", "Into Store Date"}

RACHIT_COLUMNS = [
    "Supplier", "", "Supplier PO", "Status (Display)", "Promo",
    "MMS Update Response", "PEP Buyer", "Style Code", "Style", "CC",
    "Season", "Factory", "--Latest Qty", "Ship From Date", "Ship to Date",
    "Shipment Booking date", "Loading Port Name", "DC", "From Into DC Date",
    "To Into DC Date", "Into Store Date", "Reject PI Reason", "PEP Sent Date",
    "Buyer Comments", "Supplier Comments", "Artwork Submission Ref No",
    "Latest Character Approved Stage", "Latest Status Date",
]

RIGHT_ALIGN_HEADER_NAMES = {
    "--Latest Qty", "Ship From Date", "Ship to Date", "From Into DC Date",
    "To Into DC Date", "Into Store Date", "PEP Sent Date", "Latest Status Date"
}

_last_archive = ""

# ── Database ───────────────────────────────────────────────────────────────────
def db_get_credentials():
    try:
        conn = sqlite3.connect(DB_PATH)
        cur  = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pep_import (
                id INTEGER PRIMARY KEY, app_name TEXT NOT NULL,
                username TEXT NOT NULL, password TEXT NOT NULL, updated TEXT
            )
        """)
        cur.execute("SELECT username, password FROM pep_import WHERE app_name=? LIMIT 1", (APP_NAME,))
        row = cur.fetchone()
        conn.commit()
        conn.close()
        if row:
            return row[0], row[1]
    except Exception:
        pass
    return DEFAULT_USER, DEFAULT_PASS


def db_save_credentials(username, password):
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pep_import (
            id INTEGER PRIMARY KEY, app_name TEXT NOT NULL,
            username TEXT NOT NULL, password TEXT NOT NULL, updated TEXT
        )
    """)
    cur.execute("SELECT id FROM pep_import WHERE app_name=?", (APP_NAME,))
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if cur.fetchone():
        cur.execute("UPDATE pep_import SET username=?, password=?, updated=? WHERE app_name=?",
                    (username, password, now, APP_NAME))
    else:
        cur.execute("INSERT INTO pep_import (app_name, username, password, updated) VALUES (?,?,?,?)",
                    (APP_NAME, username, password, now))
    conn.commit()
    conn.close()

# ── API helpers ────────────────────────────────────────────────────────────────
def api_post(session, payload, username, password):
    params  = {"request.preventCache": str(int(time.time() * 1000))}
    headers = {"Content-Type": "application/x-www-form-urlencoded",
               "Referer": f"{BASE_URL}/WebAccess/home.html"}
    resp = session.post(API_URL, params=params, data=payload,
                        headers=headers, timeout=60)
    resp.raise_for_status()
    text = resp.text.strip().lstrip("(").rstrip(")")
    return json.loads(text)


def login(session, username, password):
    try:
        data = api_post(session, {
            "Fmt.Version": "2", "LoginID": username, "Password": password,
            "Module": "DataSource", "Operation": "SimpleLogin", "OutputJSON": "1",
        }, username, password)
        if data.get("Status") == "Successful":
            print("[+] Login successful")
            return True
        print(f"[-] Login failed: {data.get('Status')}")
        return False
    except Exception as e:
        print(f"[-] Login error: {e}")
        return False


def fetch_all_sent_pos(session, username, password):
    qry_xml = (
        '<?xml version="1.0" encoding="utf-8" ?><Query>'
        '<Node Parameter="Type" Op="EQ" Value="PurchasedOrder"/>'
        '<Attribute Id="State" Op="NE" SValue="PurchasedOrderState:Abandoned"/>'
        '<Attribute Id="po_display_status" Op="EQ" SValue="SENT"/>'
        '</Query>'
    )
    data = api_post(session, {
        "Fmt.Version": "2", "Fmt.AC.Rights": "Current", "Fmt.Attr.Info": "Mid",
        "Module": "Search", "Operation": "QueryByXML", "OutputJSON": "1", "Qry.XML": qry_xml,
    }, username, password)
    nodes = data.get("NODES", {}).get("ResultNode", [])
    print(f"[+] Found {len(nodes)} SENT POs on portal")
    return nodes


def fetch_prices_batch(session, url_list, username, password):
    price_map     = {}
    total_batches = -(-len(url_list) // BATCH_SIZE)
    for i in range(0, len(url_list), BATCH_SIZE):
        batch = url_list[i:i + BATCH_SIZE]
        print(f"  Batch {i // BATCH_SIZE + 1}/{total_batches}...")
        payload_list = [
            ("Fmt.Version", "2"), ("Fmt.AC.Rights", "Current"),
            ("Fmt.Attr.Info", "Mid"), ("Module", "Search"),
            ("Operation", "QueryByURL"), ("OutputJSON", "1"),
        ]
        for url in batch:
            payload_list.append(("Qry.URL", url))
        try:
            data  = api_post(session, payload_list, username, password)
            nodes = data.get("NODES", {}).get("ResultNode", [])
            for node in nodes:
                po_num     = str(node.get("$Name", "")).strip()
                style_code = str(node.get("p_purchasedorder_style_code", "")).strip()
                price      = node.get("p_po_local_avg_cost_price") or node.get("p_po_latest_becp")
                if po_num and style_code:
                    price_map[(po_num, style_code)] = float(price) if price is not None else None
        except Exception as e:
            print(f"  [!] Batch error: {e}")
        time.sleep(THROTTLE)
    return price_map

# ── Helpers ────────────────────────────────────────────────────────────────────
def to_ddmmyyyy(val):
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
    skip  = {"pepcombined.xls", "pepcombined.xlsx"}
    files = [f for f in glob.glob(os.path.join(CURRENT_FOLDER, "*.xls*"))
             if os.path.basename(f).lower() not in skip]
    return files[0] if files else None


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

# ── Excel writer ───────────────────────────────────────────────────────────────
def write_xls(df, output_path, group_label=""):
    wb = xlwt.Workbook()
    ws = wb.add_sheet("Sheet")

    DARK_GREY = 0x08
    WHITE     = 0x01
    xlwt.add_palette_colour("dark_grey_333", DARK_GREY)
    wb.set_colour_RGB(DARK_GREY, 51, 51, 51)

    def make_style(bold=False, font_colour=DARK_GREY, fill_colour=None,
                   fill_pattern=0, h_align=1, num_format=None):
        style = xlwt.XFStyle()
        font  = xlwt.Font()
        font.name, font.height, font.bold, font.colour_index = "Calibri", 220, bold, font_colour
        style.font = font
        align = xlwt.Alignment()
        align.horz, align.vert = h_align, xlwt.Alignment.VERT_BOTTOM
        style.alignment = align
        if fill_colour is not None:
            pat = xlwt.Pattern()
            pat.pattern = fill_pattern if fill_pattern else xlwt.Pattern.SOLID_PATTERN
            pat.pattern_fore_colour = fill_colour
            style.pattern = pat
        if num_format:
            style.num_format_str = num_format
        return style

    hdr_left  = make_style(bold=True, fill_colour=WHITE, fill_pattern=1, h_align=1)
    hdr_right = make_style(bold=True, fill_colour=WHITE, fill_pattern=1, h_align=3)
    data_style = make_style(h_align=1)
    price_style = make_style(h_align=3, num_format="R#,##0.0000")
    qty_style   = make_style(h_align=3, num_format="#,##0")

    for col_idx, col_name in enumerate(df.columns):
        style = hdr_right if col_name in RIGHT_ALIGN_HEADER_NAMES else hdr_left
        ws.write(0, col_idx, col_name, style)

    ws.write(1, 0, group_label, data_style)
    for col_idx in range(1, len(df.columns)):
        ws.write(1, col_idx, "", data_style)

    epoch = datetime.datetime(1899, 12, 30)
    for row_idx, row in enumerate(df.itertuples(index=False), start=2):
        for col_idx, val in enumerate(row):
            col_name = df.columns[col_idx]
            if val is None or (isinstance(val, float) and pd.isna(val)):
                ws.write(row_idx, col_idx, "", data_style)
            elif col_name == "Supplier Cost Price (ZAR)":
                ws.write(row_idx, col_idx, val if val is not None else "", price_style)
            elif col_name == "--Latest Qty":
                try:
                    ws.write(row_idx, col_idx, int(val), qty_style)
                except Exception:
                    ws.write(row_idx, col_idx, str(val), data_style)
            elif hasattr(val, "strftime"):
                serial = (val.to_pydatetime().replace(tzinfo=None) - epoch).days
                ws.write(row_idx, col_idx, serial, data_style)
            elif isinstance(val, float) and val != val:
                ws.write(row_idx, col_idx, "", data_style)
            else:
                ws.write(row_idx, col_idx, str(val) if val is not None else "", data_style)

    wb.save(output_path)

# ── Core import logic ──────────────────────────────────────────────────────────
def run_import(username, password):
    for folder in [CURRENT_FOLDER, OLD_FOLDER]:
        if not os.path.isdir(folder):
            print(f"[-] Folder not found: {folder}")
            sys.exit(1)

    source_file = find_source_file()
    if not source_file:
        print(f"[-] No Excel files found in: {CURRENT_FOLDER}")
        sys.exit(1)

    print(f"[+] Processing: {os.path.basename(source_file)}")

    try:
        df_raw      = pd.read_excel(source_file, header=None)
        group_label = str(df_raw.iloc[1, 0]) if len(df_raw) > 1 else ""
        if group_label in ("nan", "None"):
            group_label = ""
        df = pd.read_excel(source_file, header=0, skiprows=[1], dtype={"Supplier PO": str})
    except Exception as e:
        print(f"[-] Error reading file: {e}")
        sys.exit(1)

    df["Supplier PO"] = df["Supplier PO"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    df["Style Code"]  = df["Style Code"].astype(str).str.strip()
    df.rename(columns={"Unnamed: 1": ""}, inplace=True)

    if df.empty:
        print("[-] File has no data rows.")
        sys.exit(1)

    print(f"[+] Read {len(df)} rows from export")

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"})

    if not login(session, username, password):
        sys.exit(1)

    po_nodes = fetch_all_sent_pos(session, username, password)
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
    price_map = fetch_prices_batch(session, list(po_to_url.values()), username, password)
    print(f"[+] Price keys built: {len(price_map)}")

    def lookup_price(row):
        key = (str(row["Supplier PO"]).strip(), str(row["Style Code"]).strip())
        return price_map.get(key)

    matched   = df.apply(lookup_price, axis=1).notna().sum()
    unmatched = df.apply(lookup_price, axis=1).isna().sum()
    print(f"[+] Matched: {matched} | Unmatched: {unmatched}")

    # Enforce Rachit's exact 28-column structure
    for col in RACHIT_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df = df[RACHIT_COLUMNS].copy()

    # Add price column at end
    df["Supplier Cost Price (ZAR)"] = df.apply(lookup_price, axis=1)

    # Convert date columns
    date_col_names = [c for c in DATE_COLS_BY_NAME if c in df.columns]
    for col in date_col_names:
        df[col] = df[col].apply(to_ddmmyyyy)

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    write_xls(df, OUTPUT_FILE, group_label)
    print(f"[+] Output: {OUTPUT_FILE}")

    archive_files(source_file, timestamp)

    print(f"\n-- Complete ----------------------------------------")
    print(f"  Records processed   : {len(df)}")
    print(f"  Prices matched      : {matched}")
    print(f"  Unmatched           : {unmatched}")
    print(f"  Date cols converted : {', '.join(date_col_names)}")
    print(f"  Output              : pepcombined.xls")
    print(f"  Timestamp           : {timestamp}")
    print(f"----------------------------------------------------")

# ── GUI ────────────────────────────────────────────────────────────────────────
def ensure_log_dir():
    os.makedirs(LOG_DIR, exist_ok=True)

def get_log_path():
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(LOG_DIR, f"pep_import_{ts}.log")


class PepImportApp:
    def __init__(self, root):
        self.root      = root
        self.root.title("Joshtex - PEP Import")
        self.root.resizable(False, False)
        self.root.configure(bg="#f0f0f0")
        self.log_queue = queue.Queue()
        self.log_file  = None
        self.running   = False
        self._summary  = None
        self._build_ui()
        self._set_icon()
        self._poll_log_queue()

    def _set_icon(self):
        try:
            icon_path = os.path.join(SCRIPT_DIR, "joshtex.ico")
            if os.path.exists(icon_path):
                self.root.iconbitmap(icon_path)
        except Exception:
            pass

    def _build_ui(self):
        header = tk.Frame(self.root, bg="#121212", pady=10)
        header.pack(fill=tk.X)
        tk.Label(header, text="Joshtex  —  PEP Import",
                 font=("Calibri", 16, "bold"), fg="#D4AF37", bg="#121212").pack()
        tk.Label(header, text="Northern Textile Mills SA (PTY) Ltd  |  Ladysmith",
                 font=("Calibri", 9), fg="#a08820", bg="#121212").pack()

        log_frame = tk.Frame(self.root, bg="#f0f0f0", padx=12, pady=8)
        log_frame.pack(fill=tk.BOTH, expand=True)
        tk.Label(log_frame, text="Output Log", font=("Calibri", 9, "bold"),
                 bg="#f0f0f0", fg="#333333").pack(anchor=tk.W)

        self.log_box = scrolledtext.ScrolledText(
            log_frame, width=72, height=20, font=("Consolas", 9),
            bg="#1e1e1e", fg="#d4d4d4", insertbackground="white",
            state=tk.DISABLED, relief=tk.FLAT, bd=1
        )
        self.log_box.pack(fill=tk.BOTH, expand=True)
        self.log_box.tag_config("ok",      foreground="#4ec9b0")
        self.log_box.tag_config("warn",    foreground="#dcdcaa")
        self.log_box.tag_config("err",     foreground="#f44747")
        self.log_box.tag_config("info",    foreground="#9cdcfe")
        self.log_box.tag_config("dim",     foreground="#858585")
        self.log_box.tag_config("default", foreground="#d4d4d4")

        self.status_var = tk.StringVar(value="Ready")
        tk.Label(self.root, textvariable=self.status_var, font=("Calibri", 9),
                 bg="#e0e0e0", fg="#555555", anchor=tk.W, padx=8, pady=3).pack(fill=tk.X)

        btn_frame = tk.Frame(self.root, bg="#f0f0f0", padx=12, pady=10)
        btn_frame.pack(fill=tk.X)
        self.start_btn = tk.Button(
            btn_frame, text="▶  Start Import", font=("Calibri", 10, "bold"),
            bg="#D4AF37", fg="#121212", activebackground="#b8962e",
            activeforeground="#121212", relief=tk.FLAT, padx=18, pady=6,
            cursor="hand2", command=self._start_import
        )
        self.start_btn.pack(side=tk.LEFT, padx=(0, 8))
        tk.Button(btn_frame, text="⚙  Settings", font=("Calibri", 10),
                  bg="#e0e0e0", fg="#333333", activebackground="#cccccc",
                  relief=tk.FLAT, padx=12, pady=6, cursor="hand2",
                  command=self._open_settings).pack(side=tk.LEFT)
        tk.Button(btn_frame, text="Clear Log", font=("Calibri", 9),
                  bg="#f0f0f0", fg="#888888", activebackground="#e0e0e0",
                  relief=tk.FLAT, padx=10, pady=6, cursor="hand2",
                  command=self._clear_log).pack(side=tk.RIGHT)

    def log(self, message, tag="default"):
        ts   = datetime.datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {message}\n"
        self.log_queue.put((line, tag))
        if self.log_file:
            try:
                with open(self.log_file, "a", encoding="utf-8") as f:
                    f.write(line)
            except Exception:
                pass

    def _poll_log_queue(self):
        while not self.log_queue.empty():
            line, tag = self.log_queue.get()
            self.log_box.config(state=tk.NORMAL)
            self.log_box.insert(tk.END, line, tag)
            self.log_box.see(tk.END)
            self.log_box.config(state=tk.DISABLED)
        self.root.after(100, self._poll_log_queue)

    def _clear_log(self):
        self.log_box.config(state=tk.NORMAL)
        self.log_box.delete("1.0", tk.END)
        self.log_box.config(state=tk.DISABLED)

    def _open_settings(self):
        pwd = simpledialog.askstring("Admin Password", "Enter admin password:",
                                     show="*", parent=self.root)
        if pwd is None:
            return
        if pwd != ADMIN_PASS:
            messagebox.showerror("Access Denied", "Incorrect admin password.")
            return

        win = tk.Toplevel(self.root)
        win.title("Joshtex - Update Portal Credentials")
        win.resizable(False, False)
        win.grab_set()
        win.configure(bg="#f0f0f0")
        try:
            icon_path = os.path.join(SCRIPT_DIR, "joshtex.ico")
            if os.path.exists(icon_path):
                win.iconbitmap(icon_path)
        except Exception:
            pass

        current_user, current_pass = db_get_credentials()
        tk.Label(win, text="Update Portal Credentials",
                 font=("Calibri", 11, "bold"), bg="#f0f0f0", fg="#121212").grid(
            row=0, column=0, columnspan=2, padx=16, pady=(14, 8), sticky=tk.W)
        tk.Label(win, text="Username:", bg="#f0f0f0", font=("Calibri", 10)).grid(
            row=1, column=0, padx=16, pady=4, sticky=tk.E)
        user_var = tk.StringVar(value=current_user)
        tk.Entry(win, textvariable=user_var, width=35, font=("Calibri", 10)).grid(
            row=1, column=1, padx=16, pady=4)
        tk.Label(win, text="Password:", bg="#f0f0f0", font=("Calibri", 10)).grid(
            row=2, column=0, padx=16, pady=4, sticky=tk.E)
        pass_var = tk.StringVar(value=current_pass)
        tk.Entry(win, textvariable=pass_var, width=35, font=("Calibri", 10), show="*").grid(
            row=2, column=1, padx=16, pady=4)

        def save():
            u = user_var.get().strip()
            p = pass_var.get().strip()
            if not u or not p:
                messagebox.showwarning("Missing", "Username and password cannot be empty.", parent=win)
                return
            try:
                db_save_credentials(u, p)
                messagebox.showinfo("Joshtex - Credentials Saved",
                                    "Credentials updated successfully.", parent=win)
                win.destroy()
            except Exception as e:
                messagebox.showerror("Error", f"Could not save credentials:\n{e}", parent=win)

        btn_row = tk.Frame(win, bg="#f0f0f0")
        btn_row.grid(row=3, column=0, columnspan=2, pady=12)
        tk.Button(btn_row, text="Save", font=("Calibri", 10, "bold"),
                  bg="#D4AF37", fg="#121212", relief=tk.FLAT,
                  padx=14, pady=5, command=save).pack(side=tk.LEFT, padx=6)
        tk.Button(btn_row, text="Cancel", font=("Calibri", 10),
                  bg="#e0e0e0", relief=tk.FLAT, padx=10, pady=5,
                  command=win.destroy).pack(side=tk.LEFT)

    def _start_import(self):
        if self.running:
            return
        self.running = True
        self.start_btn.config(state=tk.DISABLED, text="Running...")
        self.status_var.set("Running import...")
        ensure_log_dir()
        self.log_file = get_log_path()
        self.log(f"Log file: {self.log_file}", "dim")
        self.log("Starting PEP Import...", "info")
        threading.Thread(target=self._run_import, daemon=True).start()

    def _run_import(self):
        import builtins
        original_print = builtins.print
        self._summary  = {}

        try:
            def capturing_print(*args, **kwargs):
                msg = " ".join(str(a) for a in args)
                # Determine log colour
                if msg.startswith("[+]"):
                    tag = "ok"
                elif msg.startswith("[-]") or msg.startswith("[!]"):
                    tag = "err"
                elif msg.startswith("  Batch") or msg.startswith("  ["):
                    tag = "dim"
                elif msg.startswith("--") or msg.startswith("  "):
                    tag = "info"
                else:
                    tag = "default"
                self.log(msg, tag)
                # Parse summary values
                if "Records processed" in msg:
                    try: self._summary["records"] = int(msg.split(":")[1].strip())
                    except Exception: pass
                elif "Prices matched" in msg:
                    try: self._summary["matched"] = int(msg.split(":")[1].strip())
                    except Exception: pass
                elif "Unmatched" in msg:
                    try: self._summary["unmatched"] = int(msg.split(":")[1].strip())
                    except Exception: pass
                elif "Output" in msg and "pepcombined" in msg:
                    try: self._summary["output"] = msg.split(":", 1)[1].strip()
                    except Exception: pass
                elif "Timestamp" in msg and "_" in msg:
                    try: self._summary["raw_ts"] = msg.split(":", 1)[1].strip()
                    except Exception: pass

            builtins.print = capturing_print

            username, password = db_get_credentials()
            self.log(f"Using credentials for: {username}", "dim")
            run_import(username, password)

            if "raw_ts" in self._summary:
                try:
                    ts = self._summary["raw_ts"].strip()
                    self._summary["timestamp"] = (
                        ts[6:8] + "/" + ts[4:6] + "/" + ts[0:4] +
                        " " + ts[9:11] + ":" + ts[11:13] + ":" + ts[13:15]
                    )
                except Exception:
                    self._summary["timestamp"] = self._summary.get("raw_ts", "")

            self._summary["output"] = self._summary.get("output", OUTPUT_FILE)

            if not self._summary.get("records"):
                self._summary = None

        except SystemExit as e:
            if str(e) != "0":
                self.log(f"Import exited: {e}", "err")
            self._summary = None
        except Exception as e:
            self.log(f"Unexpected error: {e}", "err")
            import traceback
            self.log(traceback.format_exc(), "err")
            self._summary = None
        finally:
            builtins.print = original_print
            self.running   = False
            self.root.after(0, self._import_done)

    def _import_done(self):
        self.start_btn.config(state=tk.NORMAL, text="▶  Start Import")
        self.status_var.set("Done — ready for next import")
        self.log("Import process finished.", "ok")

        if self._summary:
            s   = self._summary
            ts  = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            arc = "IMP" + ts + "_pepcombined.xls"

            self.log("─" * 52, "dim")
            self.log("PEP DATA PROCESSING COMPLETE", "ok")
            self.log("─" * 52, "dim")
            self.log(f"Records processed      : {s.get('records', 'N/A')}", "info")
            self.log(f"Prices matched         : {s.get('matched', 'N/A')}", "info")
            self.log(f"Prices unmatched       : {s.get('unmatched', 'N/A')}", "info")
            self.log("Date cols converted    : P, W  (DD MM YYYY)", "info")
            self.log("File format            : Excel 95", "info")
            self.log(f"Timestamp              : {s.get('timestamp', '')}", "info")
            self.log(f"Working file           : {s.get('output', '')}", "info")
            self.log(f"Archive file           : {arc}", "info")
            self.log(f"Log file               : {self.log_file}", "dim")
            self.log("─" * 52, "dim")

            msg = (
                "PEP data processing complete!\n\n"
                "Records processed:        " + str(s.get("records", "N/A")) + "\n"
                "Prices matched:           " + str(s.get("matched", "N/A")) + "\n"
                "Prices unmatched:         " + str(s.get("unmatched", "N/A")) + "\n"
                "Date columns converted:   P, W (DD MM YYYY)\n"
                "File format:              Excel 95\n\n"
                "Timestamp:  " + s.get("timestamp", "") + "\n\n"
                "Working file:  " + str(s.get("output", "")) + "\n"
                "Archive file:  " + arc + "\n\n"
                "Log file:  " + str(self.log_file)
            )
            messagebox.showinfo("Joshtex - Import Complete", msg)


def main():
    root = tk.Tk()
    root.geometry("620x520")
    PepImportApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
