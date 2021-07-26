#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
"""A tiny sqlite view interface in TK intended for teaching.

No dependency apart from python 3.9.
"""

# Documentation: https://tkdocs.com


# TODO(Nicolas Despres): Schema diagram using dotty?

# WARNING: use single-quote for this string (release script relies on it)!
__version__ = 'git'


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
from tkinter.messagebox import showerror
from tkinter.messagebox import Message
from tkinter.font import nametofont
from contextlib import contextmanager
import re
import threading


def head(it, n=100):
    while n > 0:
        try:
            yield next(it)
        except StopIteration:
            break
        n -= 1

def get_selected_tab_index(notebook):
    widget_name = notebook.select()
    return notebook.index(widget_name)

class SchemaFrame(tk.Frame):

    COLUMNS = ("name", "type", "not null", "default", "PK")
    TAB_NAME = "%Schema"

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
        self._format_row = RowFormatter(self.COLUMNS, self.COLUMNS)

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

    def iter_items(self):
        table_items = self._tree.get_children()
        for table_item in table_items:
            field_items = self._tree.get_children(table_item)
            yield from field_items

    def get_tables_and_fields(self):
        tables = set()
        fields = set()
        for item in self.iter_items():
            table, field = item.split(".")
            tables.add(table)
            fields.add(field)
        return tables, fields

class ColorSyntax:

    SQL_KEYWORDS = (
        "ADD", "CONSTRAINT", "ALTER", "ALTER COLUMN", "ALTER TABLE",
        "ALL", "AND", "ANY", "AS",  "ASC", "BACKUP DATABASE", "BETWEEN",
        "CASE", "CHECK", "COLUMN", "COMMIT", "CONSTRAINT", "CREATE",
        "CREATE DATABASE",
        "CREATE INDEX", "CREATE OR REPLACE VIEW", "CREATE TABLE",
        "CREATE PROCEDURE", "CREATE UNIQUE INDEX", "CREATE VIEW", "DATABASE",
        "DEFAULT", "DELETE", "DESC", "DISTINCT", "DROP", "DROP COLUMN",
        "DROP CONSTRAINT", "DROP DATABASE", "DROP DEFAULT", "DROP INDEX",
        "DROP TABLE", "DROP VIEW", "EXEC", "EXISTS", "FOREIGN KEY", "FROM",
        "FULL OUTER JOIN", "GROUP BY", "HAVING", "IN", "INDEX", "INNER JOIN",
        "INSERT INTO", "INSERT INTO SELECT", "IS NULL", "IS NOT NULL", "JOIN",
        "LEFT JOIN", "LIKE", "LIMIT", "NOT", "NOT NULL", "OR", "ORDER BY",
        "OUTER JOIN", "PRIMARY KEY", "PROCEDURE", "RIGHT JOIN", "ROWNUM",
        "SELECT", "SELECT DISTINCT", "SELECT INTO", "SELECT TOP", "SET",
        "TABLE", "TOP", "TRUNCATE TABLE", "UNION", "UNION ALL", "UNIQUE",
        "UPDATE", "VALUES", "VIEW", "WHERE",
    )

    def __init__(self):
        self.tables = set()
        self.fields = set()
        self._recompile()

    def _recompile(self):
        self._sql_re = re.compile(
            r"""
              (?P<comment>  ^--.*$)
            | (?P<keyword>  \b(?i:%(keywords)s)\b)
            | (?P<table>    \b(?i:%(tables)s)\b)
            | (?P<field>    \b(?i:%(fields)s)\b)
            """ % {
                "keywords": "|".join(re.escape(i) for i in self.SQL_KEYWORDS),
                "tables": "|".join(re.escape(i) for i in self.tables),
                "fields": "|".join(re.escape(i) for i in self.fields),
            },
            re.MULTILINE | re.VERBOSE)

    def configure(self, text):
        text.tag_configure("keyword", foreground="blue")
        text.tag_configure("comment", foreground="yellow")
        text.tag_configure("table", foreground="orange")
        text.tag_configure("field", foreground="green")

    def highlight(self, text, start, end):
        content = text.get(start, end)
        text.tag_remove("keyword", start, end)
        for match in self._sql_re.finditer(content):
            for group_name in match.groupdict():
                match_start, match_end = match.span(group_name)
                token_start = f"{start}+{match_start}c"
                token_end = f"{start}+{match_end}c"
                text.tag_add(group_name, token_start, token_end)

    def set_database_names(self, tables, fields):
        self.tables = tables
        self.fields = fields
        self._recompile()

class Console(ttk.Panedwindow):

    def __init__(self, master=None, run_query_command=None,
                 command_log_maxline=1000):
        super().__init__(master, orient=tk.VERTICAL)

        ### Query
        self.query_frame = tk.Frame()
        self.query_text = ScrolledText(self.query_frame, wrap="word")
        self.query_text.bind("<<Modified>>", self.on_modified_query)
        self.run_query_bt = tk.Button(self.query_frame, text="Run",
                                      command=run_query_command)
        self.disable()
        self.query_frame.grid(column=0, row=0, sticky="nswe")
        self.query_text.grid(column=0, row=0, sticky="nswe")
        self.run_query_bt.grid(column=1, row=0, sticky="nswe")
        self.query_frame.rowconfigure(0, weight=1)
        self.query_frame.columnconfigure(0, weight=1)

        ### Command log
        self.cmdlog_text = ScrolledText(wrap="word", background="lightgray",
                                        height=100)
        self.cmdlog_text.MAXLINES = command_log_maxline
        self.cmdlog_text.configure(state=tk.DISABLED)
        self.cmdlog_text.rowconfigure(0, weight=1)
        self.cmdlog_text.columnconfigure(0, weight=1)
        self.cmdlog_text.tag_configure("error", foreground="red")
        self.cmdlog_text.tag_configure("warning", foreground="orange")

        # Register
        self.add(self.cmdlog_text, weight=4)
        self.add(self.query_frame, weight=1)

        # Syntax coloring
        self.color_syntax = ColorSyntax()
        self.color_syntax.configure(self.query_text)
        self.color_syntax.configure(self.cmdlog_text)

    def enable(self):
        self.query_text['state'] = tk.NORMAL
        self.query_text['background'] = "white"
        self.run_query_bt['state'] = tk.NORMAL

    def disable(self):
        self.query_text['background'] = "gray"
        self.query_text['state'] = tk.DISABLED
        self.run_query_bt['state'] = tk.DISABLED

    def get_current_query(self):
        return self.query_text.get('1.0', 'end')

    def log(self, msg, tags=()):
        if not msg.endswith("\n"):
            msg += "\n"
        start_index = self.cmdlog_text.index("end -1c")
        write_to_tk_text_log(self.cmdlog_text, msg, tags=tags)
        if not tags:
            self.color_syntax.highlight(self.cmdlog_text, start_index, "end")
        self.cmdlog_text.see("end")

    def on_modified_query(self, event):
        self.color_syntax.highlight(self.query_text, "1.0", "end")
        self.query_text.edit_modified(False)

class StatusBar(tk.Frame):

    def __init__(self, master=None):
        super().__init__(master=master)
        self._stack = []
        self.label = tk.Label(self, anchor="w")
        self.progress = ttk.Progressbar(self, orient=tk.HORIZONTAL, length=100)
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)
        self.label.grid(column=0, row=0, sticky="nsew")

    def push(self, message, mode=None, maximum=None):
        self._stack.append(message)
        self.set_last_status_text()

    def pop(self):
        self._stack.pop()
        self.set_last_status_text()
        self.progress.stop()
        self.progress.grid_remove()

    @contextmanager
    def context(self, message):
        self.push(message)
        try:
            yield
        finally:
            self.pop()

    def set_last_status_text(self):
        self.label['text'] = self._stack[-1]

    def start(self, interval=None, **options):
        self.progress.configure(**options)
        self.progress.start()
        self.progress.grid(column=1, row=0, sticky="nse")

class Application(tk.Frame):

    NAME = "Pico SQL"
    COMMAND_LOG_HISTORY = 1000
    DATA_VIEW_PREFETCH_LIMIT = 100

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

        # Bottom notebook
        self.bottom_nb = ttk.Notebook(self.pane)
        self.console = Console(self, run_query_command=self.run_query_action,
                               command_log_maxline=self.COMMAND_LOG_HISTORY)
        self.init_detailed_view()
        self.bottom_nb.add(self.console, text="Console")
        self.bottom_nb.add(self.detailed_view, text="Details")
        self.pane.add(self.tables)
        self.pane.add(self.bottom_nb)

    def init_statusbar(self):
        self.statusbar = StatusBar(self)
        self.statusbar.push("Ready to open a database.")

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

        self.db_menu = tk.Menu(self.menubar)
        self.menubar.add_cascade(label="Database", menu=self.db_menu)
        self.db_menu.add_command(label="Open...", command=self.open_action,
                                 accelerator="F2")
        self.db_menu.add_command(label="Close",
                                 command=self.close_action,
                                 state=tk.DISABLED)
        self.db_menu.add_separator()
        self.db_menu.add_command(label="Refresh", command=self.refresh_action,
                                 accelerator="F5")
        self.db_menu.add_command(label="Run query",
                                 command=self.run_query_action,
                                 accelerator="F3")
        self.db_menu.add_command(label="Clear results",
                                 command=self.clear_results_action,
                                 accelerator="F7",
                                 state=tk.DISABLED)
        self.db_menu.add_command(label="Run script...",
                                 command=self.run_script_action,
                                 state=tk.DISABLED)
        self.db_menu.add_separator()
        self.db_menu.add_command(label="Exit", command=self.exit_action)

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
        dlg = Message(self,
                      title="About",
                      message=f"Pico SQLite version {__version__}\n"
                      "Copyright © Nicolas Desprès from 2021")
        dlg.show()

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
        self.db_menu.entryconfigure("Close", state=tk.NORMAL)
        self.db_menu.entryconfigure("Refresh", state=tk.NORMAL)
        self.db_menu.entryconfigure("Run query", state=tk.NORMAL)
        self.db_menu.entryconfigure("Run script...", state=tk.NORMAL)
        self.console.enable()
        self.current_db_filename = db_filename
        self.master.title(f"{self.NAME} - {db_filename}")
        self.statusbar.push("Ready to run query.")

    def close_db(self):
        if self.current_db_filename is None:
            return
        self.db.close()
        self.db = None
        self.master.title(self.NAME)
        self.current_db_filename = None
        self.db_menu.entryconfigure("Close", state=tk.DISABLED)
        self.db_menu.entryconfigure("Refresh", state=tk.DISABLED)
        self.db_menu.entryconfigure("Run query", state=tk.DISABLED)
        self.db_menu.entryconfigure("Run script...", state=tk.DISABLED)
        self.console.disable()
        self.statusbar.pop()
        self.unload_tables()
        self.clear_results_action()

    def refresh_action(self):
        selected_tab_index = get_selected_tab_index(self.tables)
        self.unload_tables()
        self.load_tables()
        if selected_tab_index is not None \
           and 0 <= selected_tab_index <= self.tables.index('end'):
            self.tables.select(selected_tab_index)

    def is_result_view(self, tab_text):
        return tab_text.startswith("*")

    def is_result_view_tab(self, tab_idx):
        return self.is_result_view(self.tables.tab(tab_idx, option='text'))

    def is_admin_view(self, tab_text):
        return tab_text.startswith("%")

    def unload_tables(self):
        """Unload all tables view (not result)."""
        schema_tab_idx = None
        for tab_idx in self.tables.tabs():
            tab_text = self.tables.tab(tab_idx, option='text')
            if not self.is_result_view(tab_text) \
               and not self.is_admin_view(tab_text):
                self.tables.forget(tab_idx)
            if tab_text == self.schema.TAB_NAME:
                schema_tab_idx = tab_idx
        self.schema.clear()
        if schema_tab_idx is not None:
            self.tables.forget(schema_tab_idx)

    def load_tables(self):
        with self.statusbar.context("Loading tables..."):
            tab_id = self.tables.add(self.schema, text=self.schema.TAB_NAME)
            for table_name in iter_tables(self.db):
                table_view = self.create_table_view_for_table(table_name)
                self.tables.add(table_view, text=table_name)
                self.schema.add_table(table_name)
            self.schema.finish_table_insertion()
            self.console.color_syntax.set_database_names(*self.schema.get_tables_and_fields())


    def create_table_view_for_table(self, table_name):
        cursor = self.db.execute(
            f"SELECT * FROM {table_name}")
        return self.create_table_view(cursor)

    def create_table_view(self, cursor):
        frame = tk.Frame()
        column_ids, column_names = get_column_ids(cursor)
        tree = ttk.Treeview(frame, show="headings", selectmode='browse',
                            columns=column_ids)
        tree._selected_column = 0
        ### Scrollbars
        ys = ttk.Scrollbar(frame, orient='vertical', command=tree.yview)
        xs = ttk.Scrollbar(frame, orient='horizontal', command=tree.xview)
        format_row = RowFormatter(column_ids, column_names)
        tree['yscrollcommand'] = self.get_lazi_loader(cursor, format_row, tree, ys)
        tree['xscrollcommand'] = xs.set
        frame.rowconfigure(0, weight=1)
        frame.columnconfigure(0, weight=1)
        tree.grid(column=0, row=0, sticky="nsew")
        ys.grid(column=1, row=0, rowspan=2, sticky="nsw")
        xs.grid(column=0, row=1, columnspan=2, sticky="ews")
        tree.bind("<<TreeviewSelect>>", self.on_view_row_changed)
        return frame

    def get_lazi_loader(self, cursor, format_row, tree, yscrollbar):
        # TODO(Nicolas Despres): Discard loaded item to free memory and refetch
        #  using SELECT OFFSET. Can be done for regular table view but not
        #  for result view.
        ### Fetch the first hundred rows.
        def fetch_more():
            format_row.reset()
            for row in head(cursor, self.DATA_VIEW_PREFETCH_LIMIT):
                tree.insert('', 'end', values=format_row(row))
            format_row.configure_columns(tree)
        fetch_more()
        ### Lazily fetch item from the cursor as user scroll down the view.
        def lazi_load(begin_index, end_index):
            # print(f"asked to load item between {begin_index} and {end_index}")
            if end_index == "1.0":
                fetch_more()
            return yscrollbar.set(begin_index, end_index)
        return lazi_load

    def on_view_table_changed(self, event):
        tables_notebook = event.widget
        selected_tab = tables_notebook.select()
        if not selected_tab:
            return
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
        with self.statusbar.context("Running query..."):
            self.run_query(self.console.get_current_query())

    def run_query(self, query):
        started_at = datetime.now()
        self.log(f"\n-- Run at {started_at}\n")
        self.log(query)
        try:
            cursor = self.db.execute(query)
        except sqlite3.Error as e:
            self.log_error(e)
        except sqlite3.Warning as e:
            self.log_warning(e)
        else:
            if cursor.description is None: # No data to fetch.
                self.refresh_action()
            else:
                result_table = self.create_table_view(cursor)
                self.tables.insert(0, result_table,
                                   text=f"*Result-{self.result_view_count}")
                self.result_view_count += 1
                self.db_menu.entryconfigure("Clear results", state=tk.NORMAL)
                self.tables.select(0)
            stopped_at = datetime.now()
            self.log(f"-- duration: {stopped_at - started_at}")

    def log(self, msg, tags=()):
        self.console.log(msg, tags=tags)

    def log_error(self, e):
        self.log(f"Error: {e}\n", tags=("error",))

    def log_warning(self, w):
        self.log(f"Warning: {w}\n", tags=("warning",))

    def clear_results_action(self):
        """Remove all result tabs."""
        for tab_idx in self.tables.tabs():
            if self.is_result_view_tab(tab_idx):
                self.tables.forget(tab_idx)
        self.result_view_count = 0
        self.db_menu.entryconfigure("Clear results", state=tk.DISABLED)

    def run_script_action(self):
        script_filename = askopenfilename(
            title="SQLite script file",
            filetypes=[("SQL script", ".sql")],
            initialdir=self.get_initial_open_dir())
        if not script_filename:
            return False
        return self.run_script(script_filename)

    def run_script(self, script_filename):
        try:
            script_file = open(script_filename)
        except OSError as e:
            showerror(title="File error", message=str(e))
            return False
        else:
            self.script_runner = self.create_task(
                ScriptRunner,
                self.current_db_filename, script_file,
                on_finish=self.on_script_finished)
            self.statusbar.push(f"Running script {script_filename}...")
            self.statusbar.start(mode="indeterminate")
            self.console.disable()
            self.db_menu.entryconfigure("Open...", state=tk.DISABLED)
            self.db_menu.entryconfigure("Close", state=tk.DISABLED)
            self.db_menu.entryconfigure("Run query", state=tk.DISABLED)
            self.db_menu.entryconfigure("Run script...", state=tk.DISABLED)
            self.db_menu.entryconfigure("Refresh", state=tk.DISABLED)
            self.script_runner.start()
            return True

    def on_script_finished(self, event):
        self.statusbar.pop()
        self.log(f"-- Run script '{self.script_runner.script_filename}' in {self.script_runner.duration}")
        if self.script_runner.has_error():
            self.log_error(self.script_runner.error)
        elif self.script_runner.has_warning():
            self.log_warning(self.script_runner.error)
        self.refresh_action()
        self.console.enable()
        self.db_menu.entryconfigure("Open...", state=tk.NORMAL)
        self.db_menu.entryconfigure("Close", state=tk.NORMAL)
        self.db_menu.entryconfigure("Run query", state=tk.NORMAL)
        self.db_menu.entryconfigure("Run script...", state=tk.NORMAL)
        self.db_menu.entryconfigure("Refresh", state=tk.NORMAL)

    def create_task(self, task_class, *args, **kwargs):
        return task_class(*args, root=self.master, **kwargs)

class Task(threading.Thread):

    def __init__(self, root=None, **thread_kwargs):
        super().__init__(**thread_kwargs)
        self.root = root

    def event_generate(self, *args, **kwargs):
        self.root.event_generate(*args, **kwargs)

    def bind(self, *args, **kwargs):
        self.root.bind(*args, **kwargs)

class ScriptRunner(Task):

    ON_FINISH_EVENT = "<<ScriptRunnerFinished>>"

    def __init__(self, db_filename, script_file, root=None, on_finish=None):
        super().__init__(
            root=root,
            daemon=True, # Allow exit before finish => no commit
            name='script_runner')
        self.script_file = script_file
        self.db_filename = db_filename
        self.bind(self.ON_FINISH_EVENT, on_finish)

    def run(self):
        self.error = None
        self.started_at = datetime.now()
        db = sqlite3.connect(self.db_filename)
        try:
            script = self.script_file.read()
            db.executescript(script)
        except (sqlite3.Error, sqlite3.Warning) as e:
            self.error = e
        finally:
            self.script_file.close()
            db.close()
            self.stopped_at = datetime.now()
            self.event_generate(self.ON_FINISH_EVENT)

    @property
    def duration(self):
        return self.stopped_at - self.started_at

    @property
    def script_filename(self):
        return self.script_file.name

    def has_error(self):
        return isinstance(self.error, sqlite3.Error)

    def has_warning(self):
        return isinstance(self.error, sqlite3.Warning)

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

def get_column_id(name, seen):
    new_name = name
    i = 1
    while new_name in seen:
        new_name = f"{name}<{i}>"
        i += 1
    seen.add(new_name)
    return new_name

def get_column_ids(cursor):
    """Compute *unique* column name from the cursor description."""
    seen = set()
    ids = []
    names = []
    for t in cursor.description:
        name = t[0]
        id = get_column_id(name, seen)
        names.append(name)
        ids.append(id)
    return ids, names

class RowFormatter:

    def __init__(self, column_ids, column_names):
        self.column_ids = column_ids
        self.column_names = column_names
        self._tree_font = nametofont(ttk.Style().lookup("Treeview", "font"))
        self.reset()

    def reset(self):
        self.num_columns = len(self.column_names)
        self.maxsizes = [0] * self.num_columns
        self._update_maxsize(self.column_names)
        self.types = [type(None)] * self.num_columns
        self.has_formatted = False

    def __call__(self, row):
        self.has_formatted = True
        values = format_row_values(row)
        self._update_types(values)
        self._update_maxsize(values)
        return values

    def _update_maxsize(self, values):
        for i, v in enumerate(values):
            if isinstance(v, str):
                text = v
            elif isinstance(v, int):
                text = str(v)
            width = self._tree_font.measure(text) + 10
            if width > self.maxsizes[i]:
                self.maxsizes[i] = width

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
        if not self.has_formatted:
            return
        for i, (column_id, column_name) in enumerate(zip(self.column_ids, self.column_names)):
            tree.column(column_id,
                        width=min(self.maxsizes[i], 512),
                        anchor=self.anchor(i),
                        stretch=False)
            tree.heading(column_id, text=column_name)

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

def iter_tables(db):
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
