"""
pep_process_gui.py
------------------
GUI wrapper for the PEP Import process.
Logs output to screen and to a log file.
Credentials stored in P:\\permissions.db (table: pep_import)
Admin password required to change credentials.
"""

import tkinter as tk
from tkinter import scrolledtext, messagebox, simpledialog
import threading
import sqlite3
import os
import sys
import datetime
import queue

from dotenv import load_dotenv
import os

load_dotenv()

username = os.getenv("DEFAULT_USERNAME")
password = os.getenv("DEFAULT_PASSWORD")
admin_pass = os.getenv("ADMIN_PASS")

# ── Paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
DB_PATH      = r"P:\permissions.db"
LOG_DIR      = os.path.join(SCRIPT_DIR, "logs")
ADMIN_PASS   = admin_pass

DEFAULT_USER = username
DEFAULT_PASS = password
APP_NAME     = "Pep Import"
# ──────────────────────────────────────────────────────────────────────────────


def ensure_log_dir():
    os.makedirs(LOG_DIR, exist_ok=True)


def get_log_path():
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(LOG_DIR, f"pep_import_{ts}.log")


def db_get_credentials():
    """Read credentials from permissions.db. Returns (username, password)."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur  = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pep_import (
                id       INTEGER PRIMARY KEY,
                app_name TEXT    NOT NULL,
                username TEXT    NOT NULL,
                password TEXT    NOT NULL,
                updated  TEXT
            )
        """)
        cur.execute(
            "SELECT username, password FROM pep_import WHERE app_name=? LIMIT 1",
            (APP_NAME,)
        )
        row = cur.fetchone()
        conn.commit()
        conn.close()
        if row:
            return row[0], row[1]
    except Exception as e:
        pass
    return DEFAULT_USER, DEFAULT_PASS


def db_save_credentials(username, password):
    """Save or update credentials in permissions.db."""
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pep_import (
            id       INTEGER PRIMARY KEY,
            app_name TEXT    NOT NULL,
            username TEXT    NOT NULL,
            password TEXT    NOT NULL,
            updated  TEXT
        )
    """)
    cur.execute(
        "SELECT id FROM pep_import WHERE app_name=?", (APP_NAME,)
    )
    existing = cur.fetchone()
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if existing:
        cur.execute(
            "UPDATE pep_import SET username=?, password=?, updated=? WHERE app_name=?",
            (username, password, now, APP_NAME)
        )
    else:
        cur.execute(
            "INSERT INTO pep_import (app_name, username, password, updated) VALUES (?,?,?,?)",
            (APP_NAME, username, password, now)
        )
    conn.commit()
    conn.close()


class PepImportApp:
    def __init__(self, root):
        self.root  = root
        self.root.title("Joshtex - PEP Import")
        self.root.resizable(False, False)
        self.root.configure(bg="#f0f0f0")
        self.log_queue   = queue.Queue()
        self.log_file    = None
        self.running     = False

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
        # ── Header ─────────────────────────────────────────────────────────────
        header = tk.Frame(self.root, bg="#121212", pady=10)
        header.pack(fill=tk.X)
        tk.Label(
            header, text="Joshtex  —  PEP Import", font=("Calibri", 16, "bold"),
            fg="#D4AF37", bg="#121212"
        ).pack()
        tk.Label(
            header, text="Northern Textile Mills SA (PTY) Ltd  |  Ladysmith",
            font=("Calibri", 9), fg="#a08820", bg="#121212"
        ).pack()

        # ── Log box ────────────────────────────────────────────────────────────
        log_frame = tk.Frame(self.root, bg="#f0f0f0", padx=12, pady=8)
        log_frame.pack(fill=tk.BOTH, expand=True)

        tk.Label(
            log_frame, text="Output Log", font=("Calibri", 9, "bold"),
            bg="#f0f0f0", fg="#333333"
        ).pack(anchor=tk.W)

        self.log_box = scrolledtext.ScrolledText(
            log_frame, width=72, height=20,
            font=("Consolas", 9), bg="#1e1e1e", fg="#d4d4d4",
            insertbackground="white", state=tk.DISABLED,
            relief=tk.FLAT, bd=1
        )
        self.log_box.pack(fill=tk.BOTH, expand=True)

        # Colour tags
        self.log_box.tag_config("ok",      foreground="#4ec9b0")
        self.log_box.tag_config("warn",    foreground="#dcdcaa")
        self.log_box.tag_config("err",     foreground="#f44747")
        self.log_box.tag_config("info",    foreground="#9cdcfe")
        self.log_box.tag_config("dim",     foreground="#858585")
        self.log_box.tag_config("default", foreground="#d4d4d4")

        # ── Status bar ─────────────────────────────────────────────────────────
        self.status_var = tk.StringVar(value="Ready")
        status_bar = tk.Label(
            self.root, textvariable=self.status_var,
            font=("Calibri", 9), bg="#e0e0e0", fg="#555555",
            anchor=tk.W, padx=8, pady=3
        )
        status_bar.pack(fill=tk.X)

        # ── Buttons ────────────────────────────────────────────────────────────
        btn_frame = tk.Frame(self.root, bg="#f0f0f0", padx=12, pady=10)
        btn_frame.pack(fill=tk.X)

        self.start_btn = tk.Button(
            btn_frame, text="▶  Start Import",
            font=("Calibri", 10, "bold"),
            bg="#D4AF37", fg="#121212", activebackground="#b8962e",
            activeforeground="#121212", relief=tk.FLAT,
            padx=18, pady=6, cursor="hand2",
            command=self._start_import
        )
        self.start_btn.pack(side=tk.LEFT, padx=(0, 8))

        tk.Button(
            btn_frame, text="⚙  Settings",
            font=("Calibri", 10),
            bg="#e0e0e0", fg="#333333", activebackground="#cccccc",
            relief=tk.FLAT, padx=12, pady=6, cursor="hand2",
            command=self._open_settings
        ).pack(side=tk.LEFT)

        tk.Button(
            btn_frame, text="Clear Log",
            font=("Calibri", 9),
            bg="#f0f0f0", fg="#888888", activebackground="#e0e0e0",
            relief=tk.FLAT, padx=10, pady=6, cursor="hand2",
            command=self._clear_log
        ).pack(side=tk.RIGHT)

    # ── Logging ────────────────────────────────────────────────────────────────
    def log(self, message, tag="default"):
        ts  = datetime.datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {message}\n"
        self.log_queue.put((line, tag))
        if self.log_file:
            try:
                with open(self.log_file, "a", encoding="utf-8") as f:
                    f.write(line)
            except:
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

    # ── Settings ───────────────────────────────────────────────────────────────
    def _open_settings(self):
        pwd = simpledialog.askstring(
            "Admin Password", "Enter admin password:",
            show="*", parent=self.root
        )
        if pwd is None:
            return
        if pwd != ADMIN_PASS:
            messagebox.showerror("Access Denied", "Incorrect admin password.")
            return

        # Show credentials dialog
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

        tk.Label(win, text="Username:", bg="#f0f0f0",
                 font=("Calibri", 10)).grid(
            row=1, column=0, padx=16, pady=4, sticky=tk.E)
        user_var = tk.StringVar(value=current_user)
        tk.Entry(win, textvariable=user_var, width=35,
                 font=("Calibri", 10)).grid(
            row=1, column=1, padx=16, pady=4)

        tk.Label(win, text="Password:", bg="#f0f0f0",
                 font=("Calibri", 10)).grid(
            row=2, column=0, padx=16, pady=4, sticky=tk.E)
        pass_var = tk.StringVar(value=current_pass)
        tk.Entry(win, textvariable=pass_var, width=35,
                 font=("Calibri", 10), show="*").grid(
            row=2, column=1, padx=16, pady=4)

        def save():
            u = user_var.get().strip()
            p = pass_var.get().strip()
            if not u or not p:
                messagebox.showwarning("Missing", "Username and password cannot be empty.", parent=win)
                return
            try:
                db_save_credentials(u, p)
                messagebox.showinfo("Joshtex - Credentials Saved", "Credentials updated successfully.", parent=win)
                win.destroy()
            except Exception as e:
                messagebox.showerror("Error", f"Could not save credentials:\n{e}", parent=win)

        btn_row = tk.Frame(win, bg="#f0f0f0")
        btn_row.grid(row=3, column=0, columnspan=2, pady=12)
        tk.Button(btn_row, text="Save", font=("Calibri", 10, "bold"),
                  bg="#D4AF37", fg="#121212", relief=tk.FLAT,
                  padx=14, pady=5, command=save).pack(side=tk.LEFT, padx=6)
        tk.Button(btn_row, text="Cancel", font=("Calibri", 10),
                  bg="#e0e0e0", relief=tk.FLAT,
                  padx=10, pady=5, command=win.destroy).pack(side=tk.LEFT)

    # ── Import ─────────────────────────────────────────────────────────────────
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
        try:
            # Import the core processing module
            # We redirect its print() to our log box
            import builtins
            original_print = builtins.print

            def patched_print(*args, **kwargs):
                msg = " ".join(str(a) for a in args)
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

            builtins.print = patched_print

            # Run the core import logic
            import importlib.util, types

            # Load pep_process.py as a module
            spec   = importlib.util.spec_from_file_location(
                "pep_process",
                os.path.join(SCRIPT_DIR, "pep_process.py")
            )
            module = importlib.util.module_from_spec(spec)

            spec.loader.exec_module(module)

            # Override credentials AFTER module is loaded
            username, password = db_get_credentials()
            module.USERNAME = username
            module.PASSWORD = password

            # Capture summary by intercepting print output
            self._summary = {}

            def capturing_print(*args, **kwargs):
                msg = " ".join(str(a) for a in args)
                if "Records processed" in msg:
                    try: self._summary["records"] = int(msg.split(":")[1].strip())
                    except: pass
                elif "Prices matched" in msg:
                    try: self._summary["matched"] = int(msg.split(":")[1].strip())
                    except: pass
                elif "Unmatched" in msg:
                    try: self._summary["unmatched"] = int(msg.split(":")[1].strip())
                    except: pass
                elif "Output" in msg and "pepcombined" in msg:
                    try: self._summary["output"] = msg.split(":", 1)[1].strip()
                    except: pass
                elif "Timestamp" in msg and "_" in msg:
                    try: self._summary["raw_ts"] = msg.split(":", 1)[1].strip()
                    except: pass
                patched_print(*args, **kwargs)

            builtins.print = capturing_print
            module.main()

            # Format timestamp DD/MM/YYYY HH:MM:SS
            if "raw_ts" in self._summary:
                try:
                    ts = self._summary["raw_ts"].strip()
                    self._summary["timestamp"] = (
                        ts[6:8] + "/" + ts[4:6] + "/" + ts[0:4] +
                        " " + ts[9:11] + ":" + ts[11:13] + ":" + ts[13:15]
                    )
                except:
                    self._summary["timestamp"] = self._summary.get("raw_ts", "")

            self._summary["output"] = self._summary.get("output",
                os.path.join(SCRIPT_DIR, "pepcombined.xls"))

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
            self.running = False
            self.root.after(0, self._import_done)

    def _import_done(self):
        self.start_btn.config(state=tk.NORMAL, text="▶  Start Import")
        self.status_var.set("Done — ready for next import")
        self.log("Import process finished.", "ok")

        if self._summary:
            s   = self._summary
            ts  = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            arc = "IMP" + ts + "_pepcombined.xls"

            # Log the summary
            self.log("─" * 52, "dim")
            self.log("PEP DATA PROCESSING COMPLETE", "ok")
            self.log("─" * 52, "dim")
            self.log(f"Records processed      : {s['records']}", "info")
            self.log(f"Prices matched         : {s['matched']}", "info")
            self.log(f"Prices unmatched       : {s['unmatched']}", "info")
            self.log("Date cols converted    : P, W  (DD MM YYYY)", "info")
            self.log("File format            : Excel 95", "info")
            self.log(f"Timestamp              : {s['timestamp']}", "info")
            self.log(f"Working file           : {s['output']}", "info")
            self.log(f"Archive file           : {arc}", "info")
            self.log(f"Log file               : {self.log_file}", "dim")
            self.log("─" * 52, "dim")

            # Also show messagebox
            msg = (
                "PEP data processing complete!\n\n"
                "Records processed:        " + str(s["records"]) + "\n"
                "Prices matched:           " + str(s["matched"]) + "\n"
                "Prices unmatched:         " + str(s["unmatched"]) + "\n"
                "Date columns converted:   P, W (DD MM YYYY)\n"
                "File format:              Excel 95\n\n"
                "Timestamp:  " + s["timestamp"] + "\n\n"
                "Working file:  " + str(s["output"]) + "\n"
                "Archive file:  " + arc + "\n\n"
                "Log file:  " + str(self.log_file)
            )
            messagebox.showinfo("Joshtex - Import Complete", msg)


def main():
    root = tk.Tk()
    root.geometry("620x520")
    app  = PepImportApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
