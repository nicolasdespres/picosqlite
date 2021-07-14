#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
"""A tiny sqlite view interface in TK intended for teaching.

No dependency apart from python 3.9.
"""

# Documentation: https://tkdocs.com


# TODO(Nicolas Despres): Detailed value view
# TODO(Nicolas Despres): Run SQL script
# TODO(Nicolas Despres): Schema view
# TODO(Nicolas Despres): Schema diagram using dotty?

import sys
import argparse
import os
import sqlite3
from datetime import datetime
import tkinter as tk
import tkinter.ttk as ttk
from tkinter.scrolledtext import ScrolledText
from tkinter.filedialog import askopenfilename
from tkinter.messagebox import askyesno
from contextlib import contextmanager


class Application(tk.Frame):

    NAME = "Pico SQL"
    COMMAND_LOG_HISTORY = 1000
    DATA_VIEW_LIMIT = 1000

    def __init__(self, db_path=None, master=None):
        super().__init__(master)
        self.init_widget()
        self.init_menu()
        self.init_layout()
        self.init_logic()
        if db_path is not None:
            self.open_db(db_path)

    def init_widget(self):
        self.init_statusbar()
        self.pane = ttk.Panedwindow(self, orient=tk.VERTICAL)
        # Tables notebook
        self.tables = ttk.Notebook(self.pane, height=400)
        # Bottom notebook
        self.bottom_nb = ttk.Notebook(self.pane)
        # self.console = tk.Label(text="Console", background="cyan")
        ## Console
        self.init_console()
        ## Detailed view
        self.detailed_view = tk.Frame(self)
        self.bottom_nb.add(self.console, text="Console")
        self.bottom_nb.add(self.detailed_view, text="Details")
        self.pane.add(self.tables)
        self.pane.add(self.bottom_nb)

    def init_statusbar(self):
        self.statusbar = tk.Label(self, anchor="w")
        self._status_stack = []
        self.push_status_text("Ready to open a database.")

    def init_console(self):
        self.console = ttk.Panedwindow(self, orient=tk.VERTICAL)

        ### Query
        self.query_frame = tk.Frame()
        self.query_text = ScrolledText(self.query_frame, wrap="word")
        self.run_query_bt = tk.Button(self.query_frame, text="Run",
                                      command=self.run_query_action)
        self.disable_query()
        self.query_frame.grid(column=0, row=0, sticky="nswe")
        self.query_text.grid(column=0, row=0, sticky="nswe")
        self.run_query_bt.grid(column=1, row=0, sticky="nswe")
        self.query_frame.rowconfigure(0, weight=1)
        self.query_frame.columnconfigure(0, weight=1)

        ### Command log
        self.cmdlog_text = ScrolledText(wrap="word", background="gray",
                                        height=100)
        self.cmdlog_text.MAXLINES = self.COMMAND_LOG_HISTORY
        self.cmdlog_text.configure(state=tk.DISABLED)
        self.cmdlog_text.rowconfigure(0, weight=1)
        self.cmdlog_text.columnconfigure(0, weight=1)
        self.cmdlog_text.tag_configure("error", foreground="red")
        self.cmdlog_text.tag_configure("warning", foreground="orange")

        self.console.add(self.cmdlog_text, weight=4)
        self.console.add(self.query_frame, weight=1)

    def init_menu(self):
        # Doc: https://tkdocs.com/tutorial/menus.html
        #TODO: Add this on Windows: root.option_add('*tearOff', FALSE)
        self.menubar = tk.Menu(self)
        # Set it as the menu of this app top-level window
        self.master.config(menu=self.menubar)

        self.file_menu = tk.Menu(self.menubar)
        self.menubar.add_cascade(label="File", menu=self.file_menu)
        self.file_menu.add_command(label="Open...", command=self.open_action,
                                   accelerator="F2")
        self.file_menu.add_command(label="Close",
                                   command=self.close_action,
                                   state=tk.DISABLED)
        self.file_menu.add_separator()
        self.file_menu.add_command(label="Refresh", command=self.refresh_action,
                                   accelerator="F5")
        self.file_menu.add_command(label="Run query",
                                   command=self.run_query_action,
                                   accelerator="F3")
        self.file_menu.add_command(label="Clear results",
                                   command=self.clear_results_action,
                                   accelerator="F7",
                                   state=tk.DISABLED)
        self.file_menu.add_separator()
        self.file_menu.add_command(label="Exit", command=self.exit_action)

        self.help_menu = tk.Menu(self.menubar)
        self.menubar.add_cascade(label="Help", menu=self.help_menu)
        self.help_menu.add_command(label="About...", command=self.about_action)

    def init_layout(self):
        # Doc: https://tkdocs.com/tutorial/grid.html#resize
        self.grid(column=0, row=0, sticky="nsew")
        self.pane.grid(column=0, row=0, sticky="nsew")
        self.statusbar.grid(column=0, row=1, sticky="swe")
        self.master.rowconfigure(0, weight=1)
        self.master.columnconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)
        self.pane.rowconfigure(0, weight=1)
        self.pane.columnconfigure(0, weight=1)

    def init_logic(self):
        self.db = None
        self.current_db_filename = None
        self.master.title(self.NAME)
        self.result_view_count = 0

    def about_action(self):
        pass

    def exit_action(self):
        sys.exit()

    def destroy(self):
        super().destroy()
        if self.db is not None:
            self.db.close()
        print("Good bye")

    def get_initial_open_dir(self):
        if self.current_db_filename is None:
            return os.path.expanduser("~")
        else:
            return os.path.dirname(self.current_db_filename)

    def open_action(self):
        db_filename = askopenfilename(
            title="SQLite database file",
            filetypes=[("SQLite file", ".sqlite .db .db3"),
                       ("All files", ".*")],
            initialdir=self.get_initial_open_dir())
        if not db_filename:
            return False
        if not self.close_action():
            return False
        self.open_db(db_filename)
        return True

    def close_action(self):
        if self.current_db_filename is None:
            return True
        is_yes = askyesno(
            title="Close DB confirmation",
            message="Are you sure you want to close the database?")
        if not is_yes:
            return False
        self.close_db()
        return True

    def open_db(self, db_filename):
        if self.current_db_filename is not None:
            raise RuntimeError(f"A database is already opened {self.current_db_filename}")
        self.db = sqlite3.connect(db_filename)
        self.load_tables()
        self.file_menu.entryconfigure("Close", state=tk.NORMAL)
        self.file_menu.entryconfigure("Refresh", state=tk.NORMAL)
        self.file_menu.entryconfigure("Run query", state=tk.NORMAL)
        self.enable_query()
        self.current_db_filename = db_filename
        self.master.title(f"{self.NAME} - {db_filename}")
        self.push_status_text("Ready to run query.")

    def close_db(self):
        if self.current_db_filename is None:
            return
        self.db.close()
        self.db = None
        self.master.title(self.NAME)
        self.current_db_filename = None
        self.file_menu.entryconfigure("Close", state=tk.DISABLED)
        self.file_menu.entryconfigure("Refresh", state=tk.DISABLED)
        self.file_menu.entryconfigure("Run query", state=tk.DISABLED)
        self.disable_query()
        self.pop_status_text()
        self.unload_tables()
        self.clear_results_action()

    def push_status_text(self, text):
        self._status_stack.append(text)
        self.set_last_status_text()

    def pop_status_text(self):
        self._status_stack.pop()
        self.set_last_status_text()

    @contextmanager
    def status_context(self, text):
        self.push_status_text(text)
        try:
            yield
        finally:
            self.pop_status_text()

    def set_last_status_text(self):
        self.statusbar['text'] = self._status_stack[-1]

    def enable_query(self):
        self.query_text['state'] = tk.NORMAL
        self.query_text['background'] = "white"
        self.run_query_bt['state'] = tk.NORMAL

    def disable_query(self):
        self.query_text['background'] = "gray"
        self.query_text['state'] = tk.DISABLED
        self.run_query_bt['state'] = tk.DISABLED

    def refresh_action(self):
        self.unload_tables()
        self.load_tables()

    def is_result_view_tab(self, tab_idx):
        return self.tables.tab(tab_idx, option='text').startswith("*")

    def unload_tables(self):
        """Unload all tables view (not result)."""
        for tab_idx in self.tables.tabs():
            if not self.is_result_view_tab(tab_idx):
                self.tables.forget(tab_idx)

    def load_tables(self):
        with self.status_context("Loading tables..."):
            for table_name in list_tables(self.db):
                table_view = self.create_table_view_for_table(table_name)
                self.tables.add(table_view, text=table_name)

    def create_table_view_for_table(self, table_name):
        cursor = self.db.execute(
            f"SELECT * FROM {table_name} LIMIT {self.DATA_VIEW_LIMIT}")
        return self.create_table_view(cursor)

    def create_table_view(self, cursor):
        frame = tk.Frame()
        tree = ttk.Treeview(frame, show="headings")
        ### Scrollbars
        ys = ttk.Scrollbar(frame, orient='vertical', command=tree.yview)
        xs = ttk.Scrollbar(frame, orient='horizontal', command=tree.xview)
        tree['yscrollcommand'] = ys.set
        tree['xscrollcommand'] = xs.set
        frame.rowconfigure(0, weight=1)
        frame.columnconfigure(0, weight=1)
        tree.grid(column=0, row=0, sticky="nsew")
        ys.grid(column=1, row=0, rowspan=2, sticky="nsw")
        xs.grid(column=0, row=1, columnspan=2, sticky="ews")
        ### Fetch
        column_names = tuple(t[0] for t in cursor.description)
        tree['columns'] = column_names
        ### Insert rows
        format_row = RowFormatter(column_names)
        for i, row in enumerate(cursor):
            tree.insert('', i, values=format_row(row))
        ### Configure column
        for i, column_name in enumerate(column_names):
            tree.column(column_name,
                        width=format_row.maxsizes[i] * 8,
                        anchor=format_row.anchor(i))
            tree.heading(column_name, text=column_name)
        return frame

    def run_query_action(self):
        with self.status_context("Running query..."):
            self.run_query(self.get_current_query())

    def get_current_query(self):
        return self.query_text.get('1.0', 'end')

    def run_query(self, query):
        started_at = datetime.now()
        self.log(f"\n-- Run at {started_at}\n")
        self.log(query)
        try:
            cursor = self.db.execute(query)
        except sqlite3.Error as e:
            self.log(f"Error: {e}\n", tag=("error",))
        except sqlite3.Warning as e:
            self.log(f"Warning: {e}\n", tag=("warning",))
        else:
            if cursor.description is None: # No data to fetch.
                self.refresh_action()
            else:
                result_table = self.create_table_view(cursor)
                self.tables.insert(0, result_table,
                                   text=f"*Result-{self.result_view_count}")
                self.result_view_count += 1
                self.file_menu.entryconfigure("Clear results", state=tk.NORMAL)
            stopped_at = datetime.now()
            self.log(f"-- duration: {stopped_at - started_at}")

    def log(self, msg, tag=()):
        if not msg.endswith("\n"):
            msg += "\n"
        write_to_tk_text_log(self.cmdlog_text, msg, tag=tag)
        self.cmdlog_text.see("end")

    def clear_results_action(self):
        """Remove all result tabs."""
        for tab_idx in self.tables.tabs():
            if self.is_result_view_tab(tab_idx):
                self.tables.forget(tab_idx)
        self.result_view_count = 0
        self.file_menu.entryconfigure("Clear results", state=tk.DISABLED)

def write_to_tk_text_log(log, msg, tag=()):
    numlines = int(log.index('end - 1 line').split('.')[0])
    log['state'] = tk.NORMAL
    if numlines >= log.MAXLINES:
        log.delete('1.0', f'{log.MAXLINES - numlines}.0')
    # if log.index('end-1c') != '1.0':
    #     log.insert('end', '\n')
    log.insert('end', msg, tag)
    log['state'] = tk.DISABLED

class RowFormatter:

    def __init__(self, column_names):
        self.column_names = column_names
        self.num_columns = len(self.column_names)
        self.maxsizes = [0] * self.num_columns
        self._update_maxsize(column_names)
        self.types = [None] * self.num_columns

    def __call__(self, row):
        values = format_row_values(row)
        self._update_types(values)
        self._update_maxsize(values)
        return values

    def _update_maxsize(self, values):
        for i, v in enumerate(values):
            if isinstance(v, str):
                s = len(v)
            elif isinstance(v, int):
                s = len(str(v))
            if s > self.maxsizes[i]:
                self.maxsizes[i] = s

    def _update_types(self, values):
        for i, v in enumerate(values):
            self.types[i] = v.__class__

    def anchor(self, column_index):
        t = self.types[column_index]
        if issubclass(t, int):
            return "e"
        else:
            return "w"

def format_row_values(row):
    return tuple(format_row_value(i) for i in row)

def format_row_value(v):
    if isinstance(v, str):
        return repr(v)[1:-1]
    else:
        return v

def list_tables(db):
    cursor = db.execute(
        "SELECT name "
        "FROM sqlite_master "
        "WHERE type='table' AND name NOT LIKE 'sqlite_%';")
    for row in cursor:
        yield row[0]

def print_hierarchy(w, depth=0):
    """Print widget ownership hierarchy."""
    print('  '*depth + w.winfo_class()
          + ' w=' + str(w.winfo_width())
          + ' h=' + str(w.winfo_height())
          + ' x=' + str(w.winfo_x())
          + ' y=' + str(w.winfo_y()))
    for i in w.winfo_children():
        print_hierarchy(i, depth+1)

def start_gui(db_path):
    root = tk.Tk()
    root.geometry("600x800")
    app = Application(db_path=db_path, master=root)
    try:
        app.mainloop()
    except SystemExit:
        app.destroy()
    return 0

def build_cli():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "db_file",
        action="store",
        nargs="?",
        help="Path to the DB file to open.")
    return parser

def main(argv):
    cli = build_cli()
    options = cli.parse_args(argv[1:])
    return start_gui(options.db_file)

if __name__ == "__main__":
    sys.exit(main(sys.argv))

# Fixtures
# ========

# Raises exceptions
"""
INSERT INTO users (pseudo, password)
VALUES (foo1, barbar);
"""

# Insert item
"""
INSERT INTO users (pseudo, password)
VALUES ("foo1", "barbar");
"""

# Delete item
"""
DELETE FROM users WHERE id=5;
"""

# Select data
"""
SELECT * FROM users, posts;
"""
