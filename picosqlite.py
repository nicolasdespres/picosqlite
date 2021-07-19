#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
"""A tiny sqlite view interface in TK intended for teaching.

No dependency apart from python 3.9.
"""

# Documentation: https://tkdocs.com


# TODO(Nicolas Despres): Run SQL script
# TODO(Nicolas Despres): Check whether we can lazily load data using yscrollcommand
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


class SchemaFrame(tk.Frame):

    COLUMNS = ("name", "type", "not null", "default", "PK")

    def __init__(self, master=None):
        super().__init__(master)
        self._tree = ttk.Treeview(self, selectmode='browse',
                                  columns=self.COLUMNS)
        self._ys = ttk.Scrollbar(self, orient='vertical',
                                 command=self._tree.yview)
        self._xs = ttk.Scrollbar(self, orient='horizontal',
                                 command=self._tree.xview)
        self._tree['yscrollcommand'] = self._ys.set
        self._tree['xscrollcommand'] = self._xs.set
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)
        self._tree.grid(column=0, row=0, sticky="nsew")
        self._ys.grid(column=1, row=0, rowspan=2, sticky="nsw")
        self._xs.grid(column=0, row=1, columnspan=2, sticky="ews")
        self._format_row = RowFormatter(self.COLUMNS)

    @property
    def _db(self):
        return self.master.db

    def add_table(self, table_name):
        table_row = (table_name, '', '', '', '')
        self._tree.insert('', 'end', table_name, values=table_row)
        self._format_row(table_row)
        cursor = self._db.execute(f"pragma table_info('{table_name}')")
        for row in cursor:
            cid, name, vtype, notnuill, default_value, primary_key = row
            item_id = f"{table_name}.{name}"
            self._tree.insert(table_name, 'end', item_id,
                              values=self._format_row(row[1:]))
        self._tree.item(table_name, open=True)
        self._tree.column("#0", width=10, stretch=False)

    def finish_table_insertion(self):
        self._format_row.configure_columns(self._tree)

    def clear(self):
        table_items = self._tree.get_children()
        for table_item in table_items:
            self._tree.delete(table_item)

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
        self.tables.bind("<<NotebookTabChanged>>", self.on_view_table_changed)
        self.schema = SchemaFrame(master=self)
        self.tables.add(self.schema, text="%Schema")

        # Bottom notebook
        self.bottom_nb = ttk.Notebook(self.pane)
        self.init_console()
        self.init_detailed_view()
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
        self.cmdlog_text = ScrolledText(wrap="word", background="lightgray",
                                        height=100)
        self.cmdlog_text.MAXLINES = self.COMMAND_LOG_HISTORY
        self.cmdlog_text.configure(state=tk.DISABLED)
        self.cmdlog_text.rowconfigure(0, weight=1)
        self.cmdlog_text.columnconfigure(0, weight=1)
        self.cmdlog_text.tag_configure("error", foreground="red")
        self.cmdlog_text.tag_configure("warning", foreground="orange")

        self.console.add(self.cmdlog_text, weight=4)
        self.console.add(self.query_frame, weight=1)

    def init_detailed_view(self):
        self.detailed_view = ttk.PanedWindow(self, orient=tk.HORIZONTAL)
        self.detailed_view._current_tree = None
        self.detailed_view._current_item_id = None

        self.columns_frame = tk.Frame(self.detailed_view)
        self.columns_list = tk.StringVar(value=[])
        self.columns_listbox = tk.Listbox(self.columns_frame,
                                          selectmode='single',
                                          listvariable=self.columns_list,
                                          width=20)
        self.columns_listbox.bind("<<ListboxSelect>>",
                                  self.on_view_column_changed)
        ys = ttk.Scrollbar(self.columns_frame, orient='vertical',
                           command=self.columns_listbox.yview)
        self.columns_listbox['yscrollcommand'] = ys.set
        self.columns_listbox.grid(column=0, row=0, sticky="nswe")
        ys.grid(column=1, row=0, sticky="nse")
        self.columns_frame.rowconfigure(0, weight=1)
        self.columns_frame.columnconfigure(0, weight=1)

        self.show_text = ScrolledText(wrap="char", background="lightgray")
        self.show_text.configure(state=tk.DISABLED)
        self.show_text.grid(column=2, row=0, sticky="nswe")

        self.detailed_view.add(self.columns_frame, weight=1)
        self.detailed_view.add(self.show_text, weight=4)

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

    def is_admin_view_tab(self, tab_idx):
        return self.tables.tab(tab_idx, option='text').startswith("%")

    def unload_tables(self):
        """Unload all tables view (not result)."""
        for tab_idx in self.tables.tabs():
            if not self.is_result_view_tab(tab_idx) \
               and not self.is_admin_view_tab(tab_idx):
                self.tables.forget(tab_idx)
        self.schema.clear()

    def load_tables(self):
        with self.status_context("Loading tables..."):
            for table_name in list_tables(self.db):
                table_view = self.create_table_view_for_table(table_name)
                self.tables.add(table_view, text=table_name)
                self.schema.add_table(table_name)
            self.schema.finish_table_insertion()

    def create_table_view_for_table(self, table_name):
        cursor = self.db.execute(
            f"SELECT * FROM {table_name} LIMIT {self.DATA_VIEW_LIMIT}")
        return self.create_table_view(cursor)

    def create_table_view(self, cursor):
        frame = tk.Frame()
        tree = ttk.Treeview(frame, show="headings", selectmode='browse')
        tree._selected_column = 0
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
        tree.bind("<<TreeviewSelect>>", self.on_view_row_changed)
        ### Fetch
        column_names = tuple(t[0] for t in cursor.description)
        tree['columns'] = column_names
        ### Insert rows
        format_row = RowFormatter(column_names)
        for row in cursor:
            tree.insert('', 'end', values=format_row(row))
        format_row.configure_columns(tree)
        return frame

    def on_view_table_changed(self, event):
        tables_notebook = event.widget
        selected_tab = tables_notebook.select()
        tree = tables_notebook.nametowidget(selected_tab + ".!treeview")
        self.update_shown_row(tree)

    def on_view_row_changed(self, event):
        tree = event.widget
        self.update_shown_row(tree)

    def update_shown_row(self, tree):
        item_id = tree.focus()
        if not item_id:
            # Reset shown value
            self.detailed_view._current_tree = None
            self.detailed_view._current_item_id = None
            tree._selected_column = 0
            self.columns_list.set([])
            self.show_value('')
            return
        if self.detailed_view._current_tree is not tree:
            self.columns_list.set(tree['columns'])
            self.detailed_view._current_tree = tree
            self.detailed_view._current_item_id = item_id
            self.columns_listbox.selection_clear(0, tk.END)
            self.columns_listbox.selection_set(tree._selected_column)
            self.update_shown_column(tree, item_id)
        if self.detailed_view._current_item_id != item_id:
            self.detailed_view._current_item_id = item_id
            self.update_shown_column(tree, item_id)

    def on_view_column_changed(self, event):
        tree = self.detailed_view._current_tree
        if tree is None:
            return
        item_id = self.detailed_view._current_item_id
        if item_id is None:
            return
        self.update_shown_column(tree, item_id)

    def update_shown_column(self, tree, item_id):
        selection = self.columns_listbox.curselection()
        if not selection:
            return
        selected_item = selection[0]
        tree._selected_column = selected_item
        values = tree.item(item_id, option="values")
        if not values:
            return
        value = values[selected_item]
        self.show_value(unmangle_value(value))

    def show_value(self, value):
        self.show_text["state"] = tk.NORMAL
        set_text_widget_content(self.show_text, value)
        self.show_text["state"] = tk.DISABLED

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
            self.log(f"Error: {e}\n", tags=("error",))
        except sqlite3.Warning as e:
            self.log(f"Warning: {e}\n", tags=("warning",))
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

    def log(self, msg, tags=()):
        if not msg.endswith("\n"):
            msg += "\n"
        write_to_tk_text_log(self.cmdlog_text, msg, tags=tags)
        self.cmdlog_text.see("end")

    def clear_results_action(self):
        """Remove all result tabs."""
        for tab_idx in self.tables.tabs():
            if self.is_result_view_tab(tab_idx):
                self.tables.forget(tab_idx)
        self.result_view_count = 0
        self.file_menu.entryconfigure("Clear results", state=tk.DISABLED)

def write_to_tk_text_log(log, msg, tags=()):
    numlines = int(log.index('end - 1 line').split('.')[0])
    log['state'] = tk.NORMAL
    if numlines >= log.MAXLINES:
        log.delete('1.0', f'{log.MAXLINES - numlines}.0')
    # if log.index('end-1c') != '1.0':
    #     log.insert('end', '\n')
    log.insert('end', msg, tags)
    log['state'] = tk.DISABLED

def set_text_widget_content(text_widget, content, tags=None):
    text_widget.delete('1.0', tk.END)
    text_widget.insert('1.0', content, tags)

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

    def configure_columns(self, tree):
        for i, column_name in enumerate(self.column_names):
            tree.column(column_name,
                        width=self.maxsizes[i] * 8,
                        anchor=self.anchor(i),
                        stretch=False)
            tree.heading(column_name, text=column_name)

def format_row_values(row):
    return tuple(format_row_value(i) for i in row)

def format_row_value(v):
    if isinstance(v, str):
        return mangle_value(v)
    else:
        return v

def mangle_value(v):
    return repr(v)[1:-1]

def unmangle_value(v):
    return eval(f'"""{v}"""')

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
