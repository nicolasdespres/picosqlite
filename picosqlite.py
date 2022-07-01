#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
"""
A tiny sqlite view interface in TK intended for teaching.

No dependency apart from python 3.7.
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
from time import time
from time import strftime
import tkinter as tk
import tkinter.ttk as ttk
from tkinter.scrolledtext import ScrolledText
from tkinter.filedialog import askopenfilename
from tkinter.filedialog import asksaveasfilename
from tkinter.messagebox import askyesno
from tkinter.messagebox import showerror
from tkinter.messagebox import showwarning
from tkinter.messagebox import showinfo
from tkinter.messagebox import askquestion
from tkinter.messagebox import Message
from tkinter import messagebox
from tkinter.font import nametofont
import re
import threading
from queue import Queue
from dataclasses import dataclass
from typing import Optional
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import Union
import functools
import traceback
from collections import defaultdict
from pathlib import Path
import warnings
import shlex
import logging


def ensure_file_ext(filename, exts):
    path = Path(filename)
    if path.suffix in exts:
        return str(path)
    else:
        return str(path) + exts[0]


def running_on_windows():
    return os.name == 'nt'


class Request:
    """Collection of request class accepted by the runner thread."""

    @dataclass
    class LoadSchema:
        """Load the database schema into memory."""

    @dataclass
    class ViewTable:
        """Load a slice of a given table for viewing."""

        table_name: str
        offset: int
        limit: int

    @dataclass
    class RunQuery:
        """Run the given query."""

        query: str

    @dataclass
    class RunScript:
        """Run the given script file."""

        script_filename: str
        script: str

    @dataclass
    class CloseDB:
        """Close the opened database."""

    @dataclass
    class DumpDB:
        """Dump the currently loaded DB into the given file named filename."""

        filename: str


@dataclass
class SQLResult:
    request: Any
    started_at: datetime
    stopped_at: datetime
    error: sqlite3.Error
    warning: sqlite3.Warning
    internal_error: Tuple[type, Exception, list]

    @property
    def duration(self):
        return self.stopped_at - self.started_at

    @property
    def has_error(self):
        return self.error is not None \
            or self.internal_error is not None \
            or self.warning is not None


@dataclass
class OpenDB(SQLResult):
    def __init__(self, **kwargs):
        kwargs.setdefault("request", None)
        super().__init__(**kwargs)


ColumnDesc = Tuple[int, str, str, int, Any, int]


@dataclass
class Schema(SQLResult):
    """The result of the LoadSchema request."""

    schema: Optional[Dict[str, List[ColumnDesc]]] = None


Row = Tuple[Any, ...]
Rows = List[Row]
ColumnIDS = Tuple[str, ...]
ColumnNames = Tuple[str, ...]


@dataclass
class TableRows(SQLResult):
    """Response when loading data from a table to view it."""

    rows: Optional[Rows] = None
    column_ids: Optional[ColumnIDS] = None
    column_names: Optional[ColumnNames] = None


@dataclass
class QueryResult(SQLResult):
    rows: Optional[Rows] = None
    truncated: bool = False
    column_ids: Optional[ColumnIDS] = None
    column_names: Optional[ColumnNames] = None


class Task(threading.Thread):

    def __init__(self, root=None, **thread_kwargs):
        super().__init__(**thread_kwargs)
        self.root = root


def handler(result_type=None):
    assert result_type is not None

    def handle(func):
        @functools.wraps(func)
        def wrapper(self, request, *args, **kwargs):
            error = None
            warning = None
            internal_error = None
            payload = {}
            started_at = datetime.now()
            try:
                payload = func(self, request, *args, **kwargs)
            except sqlite3.Error as e:
                error = e
            except sqlite3.Warning as w:
                warning = w
            except Exception:
                internal_error = sys.exc_info()
            finally:
                stopped_at = datetime.now()
                return result_type(
                    request=request,
                    started_at=started_at,
                    stopped_at=stopped_at,
                    error=error,
                    warning=warning,
                    internal_error=internal_error,
                    **payload)
        return wrapper
    return handle


class SQLRunner(Task):
    """
    Run SQL query in a different thread to allow interruption.

    Warning: public method (not starting with '_') may safely be called from
    an other thread.
    """

    def __init__(self, db_filename, root=None,
                 process_result=None):
        super().__init__(root=root, name='SQLRunner')
        self._db_filename = db_filename
        if not callable(process_result):
            raise TypeError("process_result must be callable")
        self._process_result = process_result
        self._requests_q = Queue()
        self._results_q = Queue()
        self._lock = threading.Lock()
        self._db = None
        self._is_processing = False
        self._is_closing = False

    @property
    def db_filename(self):
        return self._db_filename

    @property
    def last_modification_time(self):
        if os.path.exists(self._db_filename):
            return os.path.getmtime(self._db_filename)

    def put_request(self, request):
        self._requests_q.put(request)

    def get_result(self):
        return self._results_q.get_nowait()

    @property
    def is_processing(self):
        with self._lock:
            return self._is_processing

    @property
    def is_closing(self):
        with self._lock:
            return self._is_closing

    def close(self):
        if not self._is_closing:
            self.put_request(Request.CloseDB())
            with self._lock:
                self._is_closing = True
        self.join(timeout=1.0)
        return not self.is_alive()

    def interrupt(self):
        with self._lock:
            if self._db is None:
                return
            self._db.interrupt()

    def force_interrupt(self, delay=1.0):
        started_at = time()
        while self.is_processing and time() - started_at < delay:
            self.interrupt()
        return not self.is_processing

    @property
    def in_transaction(self):
        with self._lock:
            if self._db is None:
                return False
            return self._db.in_transaction

    def _open_db(self):
        assert self._db is None
        error = None
        warning = None
        started_at = datetime.now()
        internal_error = None
        try:
            with self._lock:
                self._db = sqlite3.connect(self._db_filename)
        except sqlite3.Error as e:
            error = e
        except sqlite3.Warning as w:
            warning = w
        except Exception:
            internal_error = sys.exc_info()
        finally:
            stopped_at = datetime.now()
            result = OpenDB(
                    started_at=started_at,
                    stopped_at=stopped_at,
                    error=error,
                    warning=warning,
                    internal_error=internal_error)
            self._push_result(result)

    def run(self):
        self._open_db()
        while self._db is not None:
            request = self._requests_q.get()
            if isinstance(request, Request.CloseDB):
                with self._lock:
                    self._db.close()
                    self._db = None
                    self._is_closing = False
            else:
                request_name = type(request).__name__
                try:
                    handler = getattr(self, f"_handle_{request_name}")
                except AttributeError:
                    raise TypeError(f"unsupported request {request_name}")
                else:
                    with self._lock:
                        self._is_processing = True
                    try:
                        result = handler(request)
                    finally:
                        with self._lock:
                            self._is_processing = False
                    self._push_result(result)

    def _push_result(self, result: SQLResult):
        self._results_q.put(result)
        self.root.after_idle(self._process_result)

    @handler(result_type=Schema)
    def _handle_LoadSchema(self, request: Request.LoadSchema):
        schema = {}
        table_names = self.list_tables()
        for table_name in table_names:
            fields = list(self._execute(f"pragma table_info('{table_name}')"))
            schema[table_name] = fields
        return dict(schema=schema)

    def _handle_CloseDB(self, request: Request.CloseDB):
        raise RuntimeError("should never be called")

    @handler(result_type=TableRows)
    def _handle_ViewTable(self, request: Request.ViewTable):
        cursor = self._execute(
            f"SELECT * FROM {request.table_name} "
            f"LIMIT {request.limit} OFFSET {request.offset}")
        column_ids, column_names = get_column_ids(cursor)
        rows = list(cursor)
        return dict(rows=rows,
                    column_ids=column_ids,
                    column_names=column_names)

    @handler(result_type=QueryResult)
    def _handle_RunQuery(self, request: Request.RunQuery):
        if len(request.query) == 0:
            return dict()
        query = request.query.strip()
        if query.startswith("."):
            return self._handle_directive(shlex.split(query), request)
        else:
            cursor = self._execute(query)
            if cursor.description is None:  # No data to fetch.
                return dict()
            else:
                column_ids, column_names = get_column_ids(cursor)
                rows, truncated = eat_atmost(cursor)
                return dict(rows=rows, truncated=truncated,
                            column_ids=column_ids, column_names=column_names)

    def _handle_directive(self, argv, request: Request.RunQuery):
        directive = argv[0][1:]
        handler_name = f"_handle_directive_{directive}"
        try:
            handler = getattr(self, handler_name)
        except AttributeError:
            raise RuntimeError(f"invalid directive: '{directive}'")
        else:
            return handler(argv, request)

    def _handle_directive_run(self, argv, request):
        if len(argv) != 2:
            raise RuntimeError(f".run expects 1 argument, not {len(argv)}")
        self._run_script(argv[1])
        return dict()

    def _run_script(self, filename):
        with open(filename) as stream:
            self._executescript(stream.read())

    def _execute(self, *args, **kwargs):
        with self._lock:
            return self._db.execute(*args, **kwargs)

    def _executescript(self, *args, **kwargs):
        with self._lock:
            return self._db.executescript(*args, **kwargs)

    def list_tables(self):
        with self._lock:
            return list(iter_tables(self._db))

    def _handle_directive_dump(self, argv, request):
        if len(argv) != 2:
            raise RuntimeError(f".dump expects 1 argument, not {len(argv)}")
        self._dump(argv[1])
        return {}

    def _dump(self, filename):
        with open(filename, "w") as stream, \
             self._lock:
            for line in self._db.iterdump():
                stream.write('%s\n' % (line,))


def head(it, n=100):
    while n > 0:
        try:
            yield next(it)
        except StopIteration:
            break
        n -= 1


def eat_atmost(it, n=1000):
    objects = []
    for i, obj in enumerate(it):
        if i >= n:
            break
        objects.append(obj)
    try:
        next(it)
    except StopIteration:
        truncated = False
    else:
        truncated = True
    return objects, truncated


def get_selected_tab_index(notebook):
    widget_name = notebook.select()
    if not widget_name:  # Rarely happen when no tables are present.
        return None
    return notebook.index(widget_name)


def sqlite_type_to_py(vtype):
    # See sqlite type affinity: https://www.sqlite.org/datatype3.html
    vtype = vtype.upper()
    if "INT" in vtype or "BOOL" in vtype or "DEC" in vtype or "DATE" in vtype:
        return int
    elif 'REAL' in vtype or 'FLOA' in vtype or "DOUB" in vtype:
        return float
    elif 'CHAR' in vtype or "TEXT" in vtype or "CLOB" in vtype:
        return str
    elif 'BLOB' in vtype:
        return bytes
    else:
        warnings.warn(
            f"unsupported sqlite type '{vtype}'; falling back to int")
        return int


def escape_sqlite_str(text):
    return "'" + text.replace("'", "''") + "'"


@dataclass
class Field:
    cid: int
    name: str
    vtype: Union[str, None, int, float]
    notnull: bool
    default_value: Any
    primary_key: bool

    @classmethod
    def from_sqlite(cls, cid, name, vtype, notnull,
                    default_value, primary_key):
        return cls(cid=cid,
                   name=name,
                   vtype=sqlite_type_to_py(vtype),
                   notnull=(notnull == 1),
                   default_value=default_value,
                   primary_key=(primary_key == 1))

    def escape(self, value):
        if self.vtype is str:
            return escape_sqlite_str(value)
        else:
            return str(value)


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
        self.tables = defaultdict(dict)

    @property
    def _db(self):
        return self.master.db

    def add_table(self, table_name, fields):
        table_row = (table_name, '', '', '', '')
        self._tree.insert('', 'end', table_name, values=table_row)
        self._format_row(table_row)
        for field in fields:
            cid, name, vtype, notnull, default_value, primary_key = field
            self.tables[table_name][name] = Field.from_sqlite(*field)
            item_id = f"{table_name}.{name}"
            self._tree.insert(table_name, 'end', item_id,
                              values=self._format_row(field[1:]))
        self._tree.item(table_name, open=True)
        self._tree.column("#0", width=20, stretch=False)

    def finish_table_insertion(self):
        self._format_row.configure_columns(self._tree)

    def clear(self):
        table_items = self._tree.get_children()
        for table_item in table_items:
            self._tree.delete(table_item)

    def get_table_primary_key(self, table_name):
        try:
            fields = self.tables[table_name]
        except KeyError:
            return
        else:
            for name, info in fields.items():
                if info.primary_key:
                    return info

    def get_field_by_id(self, table_name, cid):
        try:
            fields = self.tables[table_name]
        except KeyError:
            return
        else:
            for name, info in fields.items():
                if info.cid == cid:
                    return info


class ColorSyntax:

    # SQL_STRING = r"'(?:\\'|[^'])*'"  # version with backslash escape
    SQL_STRING = r"'(?:''|[^'])*'"

    SQL_KEYWORDS = (
        "ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "BACKUP",
        "BETWEEN", "BY", "CASE", "CHECK", "COLUMN", "CONSTRAINT",
        "CREATE", "DATABASE", "DEFAULT", "DELETE", "DESC", "DISTINCT",
        "DROP", "EXEC", "EXISTS", "FOREIGN", "FROM", "FULL", "GROUP",
        "HAVING", "IN", "INDEX", "INNER", "INSERT", "INTO", "IS", "ISNULL",
        "JOIN",
        "KEY", "LEFT", "LIKE", "LIMIT", "NOT", "NULL", "OFFSET", "OR",
        "ORDER", "OUTER", "PRIMARY", "PROCEDURE", "REPLACE", "RIGHT",
        "ROWNUM", "SELECT", "SET", "TABLE", "TOP", "TRUNCATE", "UNION",
        "UNIQUE", "UPDATE", "VALUES", "VIEW", "WHERE", "ON", "OFF",
        "REFERENCES",
    )

    SQL_DIRECTIVES = ("BEGIN", "COMMIT", "RELEASE", "ROLLBACK", "SAVEPOINT",
                      "PRAGMA")

    SQL_DATATYPES = (
        "INT", "INTEGER", "NUMERIC",
        "TEXT", "CHAR", "VARCHAR",
        "BLOB",
        "REAL", "FLOAT", "DOUBLE",
        "NULL"
    )

    INTERNALS = ("run", "dump")

    def __init__(self):
        self.tables = set()
        self.fields = set()
        self._recompile()

    def _recompile(self):

        def mk_regex_any_word(words):
            return "|".join(re.escape(i) for i in words)

        self._sql_re = re.compile(
            r"""
              (?P<comment>    ^\s*--.*$)
            | (?P<keyword>    \b(?i:%(keywords)s)\b)
            | (?P<table>      \b(?i:%(tables)s)\b)
            | (?P<field>      \b(?i:%(fields)s)\b)
            | (?P<directive>  \b(?i:%(directives)s)\b)
            | (?P<datatypes>  \b(?i:%(datatypes)s)\b)
            | (?P<internal>   ^\s*\.(?i:%(internals)s)\b)
            | (?P<string>     (%(string)s))
            """ % {
                "keywords": mk_regex_any_word(self.SQL_KEYWORDS),
                "tables": mk_regex_any_word(self.tables),
                "fields": mk_regex_any_word(self.fields),
                "directives": mk_regex_any_word(self.SQL_DIRECTIVES),
                "datatypes": mk_regex_any_word(self.SQL_DATATYPES),
                "internals": mk_regex_any_word(self.INTERNALS),
                "string": self.SQL_STRING,
            },
            re.MULTILINE | re.VERBOSE)

    def configure(self, text):
        text.tag_configure("keyword", foreground="blue")
        text.tag_configure("comment", foreground="yellow")
        text.tag_configure("table", foreground="orange")
        text.tag_configure("field", foreground="green")
        text.tag_configure("directive", foreground="blue", underline=True)
        text.tag_configure("datatypes", foreground="green", underline=True)
        text.tag_configure("internal", foreground="purple")
        text.tag_configure("string", foreground="red")

    def highlight(self, text, start, end):
        content = text.get(start, end)
        text.tag_remove("keyword", start, end)
        text.tag_remove("comment", start, end)
        text.tag_remove("table", start, end)
        text.tag_remove("field", start, end)
        text.tag_remove("directive", start, end)
        text.tag_remove("datatypes", start, end)
        text.tag_remove("internal", start, end)
        text.tag_remove("string", start, end)
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

        # **Query**
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

        # **Command log**
        self.cmdlog_text = ScrolledText(wrap="word", background="lightgray",
                                        height=100)
        self.cmdlog_text.MAXLINES = command_log_maxline
        self.cmdlog_text.configure(state=tk.DISABLED)
        self.cmdlog_text.rowconfigure(0, weight=1)
        self.cmdlog_text.columnconfigure(0, weight=1)
        self.cmdlog_text.tag_configure("error", foreground="red")
        self.cmdlog_text.tag_configure("warning", foreground="orange")

        # **Register**
        self.add(self.cmdlog_text, weight=4)
        self.add(self.query_frame, weight=1)

        # **Syntax coloring**
        self.color_syntax = ColorSyntax()
        self.color_syntax.configure(self.query_text)
        self.color_syntax.configure(self.cmdlog_text)

    def enable(self):
        self.query_text['state'] = tk.NORMAL
        self._update_run_query_bt_state()

    def disable(self):
        self.query_text['state'] = tk.DISABLED
        self.run_query_bt['state'] = tk.DISABLED

    def get_current_query(self):
        return self.query_text.get('1.0', 'end').strip()

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
        self._update_run_query_bt_state()

    def _get_run_query_bt_state(self):
        if self._is_valid_query(self.get_current_query()):
            return tk.NORMAL
        else:
            return tk.DISABLED

    def _update_run_query_bt_state(self):
        self.run_query_bt['state'] = self._get_run_query_bt_state()

    def _is_valid_query(self, query):
        return sqlite3.complete_statement(query) \
            or query.lstrip().startswith(".")

    def clear(self):
        self.cmdlog_text.configure(state=tk.NORMAL)
        clear_text_widget_content(self.cmdlog_text)
        self.cmdlog_text.configure(state=tk.DISABLED)


class StatusBar(tk.Frame):

    def __init__(self, master=None):
        super().__init__(master=master)
        self.label = tk.Label(self, anchor="w")
        self.progress = ttk.Progressbar(self, orient=tk.HORIZONTAL, length=100)
        self._in_transaction = tk.Label(self, anchor="center")
        self.set_in_transaction(False)
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)
        self.label.grid(column=0, row=0, sticky="nsew")
        self._configure_db_status()
        self._temporary_text = None

    def _configure_db_status(self):
        self._in_transaction.grid(column=1, row=0, sticky="nse")

    def stop(self):
        self.progress.stop()
        self.progress.grid_forget()
        self._configure_db_status()

    def start(self, interval=None, **options):
        self.progress.configure(**options)
        self.progress.start()
        self.progress.grid(column=1, row=0, sticky="nse")
        self._in_transaction.grid_forget()

    def set_in_transaction(self, whether):
        if whether:
            self._in_transaction.configure(text="IN", background="red")
        else:
            self._in_transaction.configure(text="OK", background="green")

    def show(self, text, delay=None):
        """Show _text_ during delay second in the status bar."""
        # Clear any pending temporary text.
        if self._temporary_text is not None:
            self.after_cancel(self._temporary_text)
            self._temporary_text = None

        def remove():
            self.label['text'] = self._text
            self._temporary_text = None

        self.label['text'] = text
        if delay is None:
            self._text = text
        else:
            self._temporary_text = self.after(int(delay*1000), remove)
        self.update_idletasks()


class StatusMessage:
    READY_TO_OPEN = "Ready to open a database"
    READY = "Ready to run queries."


class TableView(tk.Frame):

    LIMIT = 100

    def __init__(self, master=None, on_treeview_selected=None):
        super().__init__(master=master)
        self.tree = ttk.Treeview(self, show="headings", selectmode='browse')
        self.tree._selected_column = 0
        # **Scrollbars**
        self.ys = ttk.Scrollbar(self, orient='vertical',
                                command=self.tree.yview)
        xs = ttk.Scrollbar(self, orient='horizontal', command=self.tree.xview)
        self.tree['yscrollcommand'] = self.ys.set
        self.tree['xscrollcommand'] = xs.set
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)
        self.tree.grid(column=0, row=0, sticky="nsew")
        self.ys.grid(column=1, row=0, rowspan=2, sticky="nsw")
        xs.grid(column=0, row=1, columnspan=2, sticky="ews")
        self.tree.bind("<<TreeviewSelect>>", on_treeview_selected)


class NamedTableView(TableView):

    @dataclass
    class State:
        begin_offset: int
        end_offset: int
        visible_item: int

        @property
        def is_empty(self):
            return self.begin_offset == 0 and self.end_offset == 0

    def __init__(self, fetcher=None, **kwargs):
        super().__init__(**kwargs)
        self.fetcher = fetcher
        self.tree._table_name = self.table_name
        self.tree['selectmode'] = 'extended'
        self.tree['yscrollcommand'] = self.lazy_load
        self.tree.bind("<Configure>", self.on_tree_configure)
        font = nametofont(ttk.Style().lookup("Treeview", "font"))
        self.linespace = font.metrics("linespace")
        self.begin_offset = 0
        self.end_offset = 0  # excluded
        self.fetching = False
        self.previous_visible_item = None

    @property
    def table_name(self):
        return self.fetcher.table_name

    @property
    def nb_view_items(self):
        return self.end_offset - self.begin_offset

    def get_visible_item(self):
        ys_begin, ys_end = self.ys.get()
        return int(self.nb_view_items * ys_begin) + self.begin_offset

    def insert(self, rows, column_ids, column_names, offset, limit):
        ys_begin, ys_end = self.ys.get()
        logging.debug(f"insert into {self.fetcher.table_name} {len(rows)} (asked {limit}) at {offset}; current=[{self.begin_offset}, {self.end_offset}]; visible=[{ys_begin}, {ys_end}]")  # noqa: E501
        format_row = RowFormatter(column_ids, column_names)
        if self.begin_offset == 0 and self.end_offset == 0:
            assert self.nb_view_items == 0
            self.begin_offset = offset
            self.end_offset = offset
        if self.previous_visible_item is not None:
            visible_item = self.previous_visible_item
            self.previous_visible_item = None
        else:
            visible_item = self.get_visible_item()
        if offset == self.end_offset:  # append to the end?
            # Insert new items at the end
            for row in rows:
                self.tree.insert('', 'end',
                                 iid=self.end_offset,
                                 values=format_row(row))
                self.end_offset += 1
            # Delete exceeded items from the beginning.
            while self.nb_view_items > self.limit:
                self.tree.delete(self.begin_offset)
                self.begin_offset += 1
        elif offset + limit == self.begin_offset:  # insert from the beginning?
            # Insert new items from the beginning
            for row in reversed(rows):
                self.begin_offset -= 1
                self.tree.insert('', 0,
                                 iid=self.begin_offset,
                                 values=format_row(row))
            # Delete exceeded items at the end.
            while self.nb_view_items > self.limit:
                self.end_offset -= 1
                self.tree.delete(self.end_offset)
        else:
            raise ValueError(
                f"wrong fetched window ! "
                f"current=[{self.begin_offset}, {self.end_offset}]; "
                f"fetched=[{offset}, {offset+limit}]")
        format_row.configure_columns(self.tree)
        # Prevent auto-scroll down after inserting items.
        if self.nb_view_items > 0:
            if not self.tree.exists(visible_item):
                # Scrollbar lower bound may lag during fast scrolling.
                visible_item = \
                    int(self.nb_view_items * 3/8) \
                    + self.begin_offset
            self.tree.see(visible_item)
        self.fetching = False

    def lazy_load(self, begin_index, end_index):
        logging.debug(f"lazy_load({begin_index}, {end_index})")
        limit = self.limit - self.nb_view_items
        if limit < self.inc_limit:
            limit = self.inc_limit
        if self.begin_offset > 0 and float(begin_index) <= 0.2:
            logging.debug("fetch down")
            offset = self.begin_offset - limit
            if offset < 0:
                offset = 0
            limit = self.begin_offset - offset
            self.fetch(offset, limit)
        if float(end_index) >= 0.8:
            logging.debug("fetch up")
            self.fetch(self.end_offset, limit)
        return self.ys.set(begin_index, end_index)

    def on_tree_configure(self, event):
        logging.debug("on_tree_configure")
        self.limit = round(event.height / self.linespace) * 4
        self._update_inc_limit()

    def _update_inc_limit(self):
        self.inc_limit = self.limit // 4

    def fetch(self, offset, limit):
        # Prevent interleaved fetch requests.
        if self.fetching:
            return
        self.fetching = True
        logging.debug(f"fetch {offset}, {limit}")
        self.fetcher(offset, limit)

    def save_state(self):
        return self.State(begin_offset=self.begin_offset,
                          end_offset=self.end_offset,
                          visible_item=self.get_visible_item())

    def restore_state(self, state):
        if state.is_empty:
            return
        logging.debug("restore_state %r", state)
        self.clear_all()
        self.previous_visible_item = state.visible_item
        self.limit = state.end_offset - state.begin_offset
        self._update_inc_limit()
        self.fetch(state.begin_offset, self.limit)

    def clear_all(self):
        assert self.end_offset >= self.begin_offset
        while self.end_offset > self.begin_offset:
            self.end_offset -= 1
            self.tree.delete(self.end_offset)


class Fetcher:

    def __init__(self, app, table_name):
        self.table_name = table_name
        self.app = app

    def __call__(self, offset, limit):
        self.app.statusbar.show(
            f"Loading {limit} records from {offset} "
            f"in table '{self.table_name}'...", delay=0.5)
        self.app.sql.put_request(
            Request.ViewTable(table_name=self.table_name,
                              offset=offset,
                              limit=limit))


class ResultTableView(TableView):

    def append(self, rows, column_ids, column_names, truncated):
        if len(rows) == 0:
            return
        format_row = RowFormatter(column_ids, column_names)
        for row in rows:
            self.tree.insert('', 'end', values=format_row(row))
        format_row.configure_columns(self.tree)
        if truncated:
            self.tree.insert('', 'end', values=["..."] * len(rows[0]))


class DBMenu:
    NEW = "New..."
    OPEN = "Open..."
    CLOSE = "Close"
    DUMP = "Dump..."
    REFRESH = "Refresh"
    RUN_QUERY = "Run query"
    CLEAR_RESULT = "Clear current result"
    CLEAR_ALL_RESULTS = "Clear all results"
    CLEAR_CONSOLE = "Clear console"
    RUN_SCRIPT = "Run script..."
    INTERRUPT = "Interrupt"
    DELETE_ROWS = "Delete rows"
    EXIT = "Exit"


class Application(tk.Frame):

    NAME = "Pico SQLite"
    COMMAND_LOG_HISTORY = 1000

    def __init__(self, db_path=None,
                 query_or_script=None, master=None):
        super().__init__(master)
        self.init_widget()
        self.init_menu()
        self.init_layout()
        self.init_logic()
        if db_path is not None:
            self.open_db(db_path)
        if query_or_script is not None:
            if os.path.isfile(query_or_script):
                self.run_script(query_or_script)
            else:
                self.run_query(query_or_script)

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
        self.statusbar.show(StatusMessage.READY_TO_OPEN)

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

        self.show_frame = tk.Frame(self.detailed_view)
        self.show_text = ScrolledText(self.show_frame, wrap="char")
        self.show_text.grid(column=0, row=0, sticky="nswe")
        self.update_bt = tk.Button(
            self.show_frame, text="Update",
            state=tk.DISABLED,
            command=self.update_value_action)
        self.update_bt.grid(column=0, row=1, sticky="nswe")
        self.show_frame.rowconfigure(0, weight=1)
        self.show_frame.columnconfigure(0, weight=1)

        self.detailed_view.add(self.columns_frame, weight=1)
        self.detailed_view.add(self.show_frame, weight=4)

    def init_menu(self):
        # Doc: https://tkdocs.com/tutorial/menus.html
        self.master.option_add('*tearOff', False)
        self.menubar = tk.Menu(self)
        # Set it as the menu of this app top-level window
        self.master.config(menu=self.menubar)

        self.db_menu = tk.Menu(self.menubar)
        self.menubar.add_cascade(label="Database", menu=self.db_menu)
        self.db_menu.add_command(label=DBMenu.NEW, command=self.new_action)
        self.db_menu.add_command(label=DBMenu.OPEN, command=self.open_action,
                                 accelerator="F2")
        self.db_menu.add_command(label=DBMenu.CLOSE,
                                 command=self.close_action,
                                 state=tk.DISABLED)
        self.db_menu.add_separator()
        self.db_menu.add_command(label=DBMenu.REFRESH,
                                 command=self.refresh_action,
                                 accelerator="F5",
                                 state=tk.DISABLED)
        self.db_menu.add_command(label=DBMenu.RUN_QUERY,
                                 command=self.run_query_action,
                                 accelerator="F3",
                                 state=tk.DISABLED)
        self.db_menu.add_command(label=DBMenu.CLEAR_RESULT,
                                 command=self.clear_result_action,
                                 accelerator="F7",
                                 state=tk.DISABLED)
        self.db_menu.add_command(label=DBMenu.CLEAR_ALL_RESULTS,
                                 command=self.clear_all_results_action,
                                 state=tk.DISABLED)
        self.db_menu.add_command(label=DBMenu.CLEAR_CONSOLE,
                                 command=self.clear_console,
                                 state=tk.DISABLED)
        self.db_menu.add_command(label=DBMenu.RUN_SCRIPT,
                                 command=self.run_script_action,
                                 state=tk.DISABLED)
        self.db_menu.add_command(label=DBMenu.INTERRUPT,
                                 command=self.interrupt_action,
                                 accelerator="F12",
                                 state=tk.DISABLED)
        self.db_menu.add_command(label=DBMenu.DELETE_ROWS,
                                 command=self.delete_rows_action,
                                 accelerator="F9",
                                 state=tk.DISABLED)
        self.db_menu.add_separator()
        self.db_menu.add_command(label=DBMenu.DUMP,
                                 command=self.dump_action,
                                 state=tk.DISABLED)
        self.db_menu.add_separator()
        self.db_menu.add_command(label=DBMenu.EXIT, command=self.exit_action)

        self.help_menu = tk.Menu(self.menubar)
        self.menubar.add_cascade(label="Help", menu=self.help_menu)
        self.help_menu.add_command(label="About...", command=self.about_action)

        # On Windows, we have to explicitly bind the accelerator...
        if running_on_windows():
            self.master.bind_all("<F2>", lambda _: self.open_action())
            self.master.bind_all("<F3>", lambda _: self.run_query_action())
            self.master.bind_all("<F5>", lambda _: self.refresh_action())
            self.master.bind_all("<F7>", lambda _: self.clear_result_action())
            self.master.bind_all("<F9>", lambda _: self.delete_rows_action())
            self.master.bind_all("<F12>", lambda _: self.interrupt_action())

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
        self.sql = None
        self.table_views = {}
        self.table_view_saved_states = {}
        self.master.title(self.NAME)
        self.result_view_count = 0
        self.selected_table_index = None
        self.last_refresed_at = None

    def about_action(self):
        PY_VERSION = str(sys.version_info.major) + "." \
            + str(sys.version_info.minor) + "." \
            + str(sys.version_info.micro)
        dlg = Message(parent=self,
                      title="About",
                      message=f"Pico SQLite version {__version__}\n"
                      f"(running on Python version {PY_VERSION})\n\n"
                      "Copyright © 2021-today Nicolas Desprès")
        dlg.show()

    def exit_action(self):
        if self.sql is not None:
            ans = askquestion(
                parent=self,
                title="Confirmation",
                message="Do you really want to quit?")
            if ans == 'no':
                return
        if self.safely_close_db():
            sys.exit()
        else:
            assert self.sql is not None
            if self.sql.is_closing:
                dlg = Message(parent=self,
                              title="Force quit",
                              message="Failed to close the database.\n\n"
                              "Do you want to force quit?",
                              icon=messagebox.ERROR,
                              type=messagebox.YESNO)
                ans = dlg.show()
                if ans == 'yes':
                    sys.exit(1)

    def destroy(self):
        super().destroy()
        if self.sql is not None:
            logging.info("Try to close...")
            if self.sql.close():
                logging.info("done closing.")
            else:
                if self.sql.is_processing:
                    logging.info("Force interrupting...")
                    self.sql.force_interrupt()
                    logging.info("Retry to close...")
                    if self.sql.close():
                        logging.info("done closing.")
                    else:
                        logging.error("Failed to close... Too bad!")
        logging.info("Good bye")

    def get_initial_open_dir(self):
        if self.sql is None:
            return os.path.expanduser("~")
        else:
            return os.path.dirname(self.sql.db_filename)

    def new_action(self):
        db_filename = asksaveasfilename(
            parent=self,
            title="SQLite database file",
            filetypes=[("SQLite file", ".db .db3 .sqlite"),
                       ("All files", ".*")],
            initialdir=self.get_initial_open_dir())
        if not db_filename:
            return False
        db_filename = ensure_file_ext(db_filename, (".db", ".db3", ".sqlite"))
        if not self.close_action():
            return False
        self.open_db(db_filename)
        return True

    def open_action(self):
        db_filename = askopenfilename(
            title="SQLite database file",
            filetypes=[("SQLite file", ".sqlite .db .db3"),
                       ("All files", ".*")],
            initialdir=self.get_initial_open_dir(),
            parent=self)
        if not db_filename:
            return False
        if not self.close_action():
            return False
        self.open_db(db_filename)
        return True

    def close_action(self):
        if self.sql is None:
            return True
        if self.sql.in_transaction:
            is_yes = askyesno(
                parent=self,
                title="SQL",
                icon="warning",
                message=""
                "You are in the middle of a transaction.\n\n"
                "Do you really want to close the DB and "
                "lose the uncommitted data ?")
        else:
            is_yes = askyesno(
                title="Close DB confirmation",
                message="Are you sure you want to close the database?",
                parent=self)
        if not is_yes:
            return False
        self.close_db()
        return True

    def open_db(self, db_filename):
        if self.sql is not None:
            raise RuntimeError(
                f"A database is already opened {self.sql.db_filename}")
        self.sql = self.create_task(SQLRunner, db_filename,
                                    process_result=self.on_sql_result)
        self.sql.start()

    def close_db(self):
        if not self.safely_close_db():
            return
        self.master.title(self.NAME)
        self.db_menu.entryconfigure(DBMenu.CLOSE, state=tk.DISABLED)
        self.db_menu.entryconfigure(DBMenu.DUMP, state=tk.DISABLED)
        self.db_menu.entryconfigure(DBMenu.REFRESH, state=tk.DISABLED)
        self.db_menu.entryconfigure(DBMenu.RUN_QUERY, state=tk.DISABLED)
        self.db_menu.entryconfigure(DBMenu.RUN_SCRIPT, state=tk.DISABLED)
        self.db_menu.entryconfigure(DBMenu.INTERRUPT, state=tk.DISABLED)
        self.console.disable()
        self.statusbar.show(StatusMessage.READY_TO_OPEN)
        self.statusbar.set_in_transaction(False)
        self.unload_tables()
        self.clear_all_results_action()
        self.last_refreshed_at = None

    def safely_close_db(self):
        if self.sql is None:
            return True
        if self.sql.is_processing:
            ans = askquestion(
                parent=self,
                title="SQL",
                message="Do you want to interrupt the execution?")
            if ans == 'yes':
                self.interrupt_action()
            return False
        if not self.sql.close():
            showerror(parent=self,
                      title="Thread error",
                      message="Failed to close database.")
            return False
        self.sql = None
        return True

    def on_sql_result(self):
        if self.sql is None or self.sql.is_closing:
            return
        result = self.sql.get_result()
        result_name = type(result).__name__
        handler_name = f"on_sql_{result_name}"
        try:
            handler = getattr(self, handler_name)
        except AttributeError:
            raise TypeError(f"unsupported SQL result type {result_name}")
        else:
            handler(result)

    def on_sql_OpenDB(self, result: OpenDB):
        if result.has_error:
            self.show_result_error(result, "Database opening")
            self.sql.join()  # Wait for the thread to finish.
            self.sql = None
        else:
            self.master.title(f"{self.NAME} - {self.sql.db_filename}")
            self.log(f"-- Database successfully opened in {result.duration}")
            self.last_refreshed_at = self.sql.last_modification_time
            self.load_tables()

    def on_sql_Schema(self, result: Schema):
        if result.has_error:
            # May happen when database is locked.
            self.log("-- Loading schema")
            self.log_error_and_warning(result)
            self.statusbar.show("Failed to load database schema!")
            self.close_db()
            return
        schema = result.schema
        assert schema is not None  # None only when there are errors.
        field_names = set()
        self.tables.add(self.schema, text=self.schema.TAB_NAME)
        for table_name, fields in schema.items():
            for field in fields:
                field_names.add(field[1])
            self.schema.add_table(table_name, fields)
            table_view = self.create_table_view(
                NamedTableView,
                fetcher=Fetcher(self, table_name))
            self.table_views[table_name] = table_view
            self.tables.add(table_view, text=table_name)
            if table_name in self.table_view_saved_states:
                table_view.restore_state(
                    self.table_view_saved_states[table_name])
        self.schema.finish_table_insertion()
        self.console.color_syntax.set_database_names(self.table_views.keys(),
                                                     field_names)
        if self.selected_table_index is not None \
           and 0 <= self.selected_table_index < self.tables.index('end'):
            self.tables.select(self.selected_table_index)
            self.selected_table_index = None
        self.table_view_saved_states = {}
        self.db_menu.entryconfigure(DBMenu.CLOSE, state=tk.NORMAL)
        self.db_menu.entryconfigure(DBMenu.DUMP, state=tk.NORMAL)
        self.db_menu.entryconfigure(DBMenu.REFRESH, state=tk.NORMAL)
        self.db_menu.entryconfigure(DBMenu.RUN_QUERY, state=tk.NORMAL)
        self.db_menu.entryconfigure(DBMenu.RUN_SCRIPT, state=tk.NORMAL)
        self.db_menu.entryconfigure(DBMenu.INTERRUPT, state=tk.DISABLED)
        self.console.enable()
        self.statusbar.show(StatusMessage.READY)

    def on_sql_TableRows(self, result: TableRows):
        table_view = self.table_views[result.request.table_name]
        self.log_error_and_warning(result)
        last_mtime = self.sql.last_modification_time
        if last_mtime is None:
            showinfo(
                parent=self,
                title="Database",
                message="Your database file has been deleted.")
            self.close_db()
            return
        do_refresh = result.has_error
        if self.last_refreshed_at != last_mtime:
            showinfo(
                parent=self,
                title="Database",
                message="Your database has been modified "
                "from an outside process.")
            do_refresh = True
        if do_refresh:
            self.refresh_action()
        else:
            table_view.insert(result.rows,
                              result.column_ids, result.column_names,
                              result.request.offset, result.request.limit)

    def refresh_action(self):
        self.selected_table_index = get_selected_tab_index(self.tables)
        self.table_view_saved_states = {
            n: tv.save_state()
            for n, tv in self.table_views.items()
        }
        self.unload_tables()
        self.load_tables()
        if self.sql is not None:
            self.last_refreshed_at = self.sql.last_modification_time

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
        self.table_views.clear()
        if schema_tab_idx is not None:
            self.tables.forget(schema_tab_idx)

    def load_tables(self):
        if self.sql is None:  # No database opened
            return
        self.statusbar.show("Loading database schema...")
        self.sql.put_request(Request.LoadSchema())

    def create_table_view(self, table_type, **kwargs):
        return table_type(on_treeview_selected=self.on_view_row_changed,
                          **kwargs)

    def on_view_table_changed(self, event):
        tables_notebook = event.widget
        selected_tab = tables_notebook.select()
        if not selected_tab:
            return
        is_result_tab = self.is_result_view_tab(selected_tab)
        self.db_menu.entryconfigure(
            DBMenu.CLEAR_RESULT,
            state=tk.NORMAL if is_result_tab else tk.DISABLED)
        table_view = tables_notebook.nametowidget(selected_tab)
        if isinstance(table_view, SchemaFrame):
            self.reset_shown_value()
            return
        self.update_shown_row(table_view.tree)

    def on_view_row_changed(self, event):
        tree = event.widget
        self.update_shown_row(tree)

    def reset_shown_value(self):
        self.detailed_view._current_tree = None
        self.detailed_view._current_item_id = None
        self.columns_list.set([])
        self.show_value('')
        self.update_bt['state'] = tk.DISABLED

    def update_shown_row(self, tree):
        selection = tree.selection()
        selected_count = len(selection)
        if selected_count == 1:
            item_id = selection[0]
        else:
            item_id = ''
        self.db_menu.entryconfigure(
            DBMenu.DELETE_ROWS,
            state=tk.NORMAL if selected_count >= 1 else tk.DISABLED)
        if not item_id:
            self.reset_shown_value()
            tree._selected_column = 0
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
        self.update_bt['state'] = tk.NORMAL

    def show_value(self, value):
        set_text_widget_content(self.show_text, value)

    def update_value_action(self):
        if self.sql is None:
            return
        if self.detailed_view._current_tree is None:
            return
        if self.detailed_view._current_item_id is None:
            return
        self.update_value(self.detailed_view._current_tree,
                          self.detailed_view._current_item_id)

    def update_value(self, tree, item_id):
        table_name = tree._table_name
        field = self.schema.get_field_by_id(table_name, tree._selected_column)
        pk = self.schema.get_table_primary_key(table_name)
        if pk is None:
            showerror(parent=self,
                      title="Schema error",
                      message=f"No primary key for table {table_name}")
            return False
        values = tree.item(item_id, option="values")
        pk_value = values[pk.cid]
        new_value = self.show_text.get('1.0', 'end')
        ans = askquestion(
            parent=self,
            title="Update confirmation",
            message="Are you sure you want to change the value of "
            f"field '{field.name}' of row with {pk.name} = {pk_value} "
            f"in table '{table_name}'?")
        if ans == 'no':
            return False
        query = "UPDATE {} SET {} = {} WHERE {} = {};"\
            .format(table_name,
                    field.name,
                    field.escape(new_value),
                    pk.name,
                    pk_value)
        self.run_query(query)
        return True

    def run_query_action(self):
        self.run_query(self.console.get_current_query())

    def run_query(self, query):
        if len(query) == 0:
            return
        self.statusbar.show("Running query...")
        self.statusbar.start(mode="indeterminate")
        self.sql.put_request(Request.RunQuery(query=query))
        self.console.run_query_bt.configure(
            text="Stop", command=self.interrupt_action)
        self.enable_sql_execution_state()

    def on_sql_QueryResult(self, result: QueryResult):
        self.log(f"\n-- Run at {result.started_at}\n")
        self.log(result.request.query)
        self.log_error_and_warning(result)
        footer_parts = [f"-- duration: {result.duration}"]
        self.console.run_query_bt.configure(
            text="Run", command=self.run_query_action)
        self.statusbar.stop()
        self.statusbar.show(StatusMessage.READY)
        if result.rows is None:  # No data fetched.
            # Refresh because it is probably an insert/delete operation.
            self.refresh_action()
        else:
            tab_name = f"*Result-{self.result_view_count}"
            result_table = self.create_table_view(ResultTableView)
            result_table.append(result.rows,
                                result.column_ids,
                                result.column_names,
                                result.truncated)
            self.tables.insert(0, result_table,
                               text=tab_name)
            self.result_view_count += 1
            self.db_menu.entryconfigure(DBMenu.CLEAR_RESULT,
                                        state=tk.NORMAL)
            self.db_menu.entryconfigure(DBMenu.CLEAR_ALL_RESULTS,
                                        state=tk.NORMAL)
            self.tables.select(0)
            footer_parts.append(f"(see <{tab_name}>)")
        self.log(" ".join(footer_parts))
        self.disable_sql_execution_state()
        self.statusbar.set_in_transaction(self.sql.in_transaction)

    def interrupt_action(self):
        self.statusbar.show("Interrupting...")
        if self.sql.force_interrupt():
            self.statusbar.show(StatusMessage.READY)
        else:
            self.statusbar.show("Failed to interrupt!")

    def clear_console(self):
        self.console.clear()
        self.db_menu.entryconfigure(DBMenu.CLEAR_CONSOLE, state=tk.DISABLED)

    def log(self, msg, tags=()):
        self.console.log(msg, tags=tags)
        self.db_menu.entryconfigure(DBMenu.CLEAR_CONSOLE, state=tk.NORMAL)

    def log_error(self, e):
        self.log(f"Error: {e}\n", tags=("error",))

    def log_warning(self, w):
        self.log(f"Warning: {w}\n", tags=("warning",))

    def log_internal_error(self, etype, value, tb):
        self.log("Internal Error!!!\n", tags=("error",))
        for line in traceback.format_exception(etype, value, tb):
            self.log(line, tags=("error",))

    def log_error_and_warning(self, result):
        if result.error is not None:
            self.log_error(result.error)
        if result.warning is not None:
            self.log_warning(result.warning)
        if result.internal_error is not None:
            self.log_internal_error(*result.internal_error)

    def clear_result_action(self):
        tab_idx = self.tables.select()
        if not tab_idx:  # No tab selected.
            return
        if self.is_result_view_tab(tab_idx):
            self.tables.forget(tab_idx)

    def clear_all_results_action(self):
        """Remove all result tabs."""
        for tab_idx in self.tables.tabs():
            if self.is_result_view_tab(tab_idx):
                self.tables.forget(tab_idx)
        self.result_view_count = 0
        self.db_menu.entryconfigure(DBMenu.CLEAR_ALL_RESULTS,
                                    state=tk.DISABLED)

    def run_script_action(self):
        if self.sql is None:
            return
        script_filename = askopenfilename(
            title="SQLite script file",
            filetypes=[("SQL script", ".sql")],
            initialdir=self.get_initial_open_dir(),
            parent=self)
        if not script_filename:
            return False
        if self.sql.in_transaction:
            ans = askquestion(
                parent=self,
                title="Commit confirmation",
                message=""
                "You are in the middle of a transaction.\n\n"
                "Do you want to commit your changes?")
            if ans == 'no':
                return False
        return self.run_script(script_filename)

    def run_script(self, script_filename):
        self.run_query(f".run {shlex.quote(script_filename)}")

    def enable_sql_execution_state(self):
        self.console.disable()
        self.db_menu.entryconfigure(DBMenu.RUN_QUERY, state=tk.DISABLED)
        self.db_menu.entryconfigure(DBMenu.RUN_SCRIPT, state=tk.DISABLED)
        self.db_menu.entryconfigure(DBMenu.REFRESH, state=tk.DISABLED)
        self.db_menu.entryconfigure(DBMenu.INTERRUPT, state=tk.NORMAL)

    def disable_sql_execution_state(self):
        self.console.enable()
        self.db_menu.entryconfigure(DBMenu.RUN_QUERY, state=tk.NORMAL)
        self.db_menu.entryconfigure(DBMenu.RUN_SCRIPT, state=tk.NORMAL)
        self.db_menu.entryconfigure(DBMenu.REFRESH, state=tk.NORMAL)
        self.db_menu.entryconfigure(DBMenu.INTERRUPT, state=tk.DISABLED)

    def create_task(self, task_class, *args, **kwargs):
        return task_class(*args, root=self.master, **kwargs)

    def delete_rows_action(self):
        if self.sql is None:
            return False
        selected_tab = self.tables.select()
        if not selected_tab:
            return False
        table_view = self.tables.nametowidget(selected_tab)
        if not isinstance(table_view, NamedTableView):
            return False
        selection = table_view.tree.selection()
        pk = self.schema.get_table_primary_key(table_view.table_name)
        if pk is None:
            showerror(parent=self,
                      title="Schema error",
                      message="No primary key "
                      f"for table {table_view.table_name}")
            return False
        ans = askquestion(
            parent=self,
            title="Delete confirmation",
            message="Are you sure you want to "
            f"delete {len(selection)} rows from "
            f"table '{table_view.table_name}'?")
        if ans == 'no':
            return False
        ids = ", ".join(str(table_view.tree.item(i, 'values')[0])
                        for i in selection)
        query = f"""\
        DELETE FROM {table_view.table_name} WHERE {pk.name} IN ({ids});
        """
        self.run_query(query)
        return True

    def dump_action(self):
        if self.sql is None:
            return False
        dump_filename = asksaveasfilename(
            parent=self,
            title="SQLite dump file",
            filetypes=[("SQLite script file", ".sql"),
                       ("All files", ".*")],
            initialdir=self.get_initial_open_dir())
        if not dump_filename:
            return False
        dump_filename = ensure_file_ext(dump_filename, (".sql",))
        self.dump(dump_filename)
        return True

    def dump(self, filename):
        assert self.sql is not None
        self.run_query(f".dump {shlex.quote(filename)}")

    def show_result_error(self, result, prefix):
        if result.error is not None:
            showerror(parent=self,
                      title=f"{prefix} error",
                      message=str(result.error))
        elif result.warning is not None:
            showwarning(parent=self,
                        title=f"{prefix} warning",
                        message=str(result.warning))
        elif result.interal_error is not None:
            showerror(parent=self,
                      title=f"{prefix} internal error",
                      message=str(result.internal_error))
        else:
            raise RuntimeError("unexpected result state error")


def write_to_tk_text_log(log, msg, tags=()):
    numlines = int(log.index('end - 1 line').split('.')[0])
    log['state'] = tk.NORMAL
    if numlines >= log.MAXLINES:
        log.delete('1.0', f'{log.MAXLINES - numlines}.0')
    # if log.index('end-1c') != '1.0':
    #     log.insert('end', '\n')
    log.insert('end', msg, tags)
    log['state'] = tk.DISABLED


def clear_text_widget_content(text_widget):
    text_widget.delete('1.0', tk.END)


def set_text_widget_content(text_widget, content, tags=None):
    clear_text_widget_content(text_widget)
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
        tree.configure(columns=self.column_ids)
        for i, (column_id, column_name) in enumerate(zip(self.column_ids,
                                                         self.column_names)):
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


def log_widget_hierarchy(w, depth=0):
    """Print widget ownership hierarchy."""
    logging.info('  '*depth + w.winfo_class()
                 + ' w=' + str(w.winfo_width())
                 + ' h=' + str(w.winfo_height())
                 + ' x=' + str(w.winfo_x())
                 + ' y=' + str(w.winfo_y()))
    for i in w.winfo_children():
        log_widget_hierarchy(i, depth+1)


def start_gui(db_path, query_or_script=None):
    root = tk.Tk()
    root.geometry("600x800")
    app = Application(db_path=db_path,
                      query_or_script=query_or_script,
                      master=root)
    root.protocol('WM_DELETE_WINDOW', app.exit_action)
    try:
        root.mainloop()
    except SystemExit:
        app.destroy()
    return 0


def rev_dict(d):
    return {v: k for k, v in d.items()}


LOG_LEVEL2STR = {
    logging.DEBUG: "debug",
    logging.INFO: "info",
    logging.WARNING: "warning",
    logging.ERROR: "error",
    logging.CRITICAL: "critical",
}
LOG_STR2LEVEL = rev_dict(LOG_LEVEL2STR)


def get_default_log_file():
    if running_on_windows():
        logdir = Path(os.environ["LOCALAPPDATA"])/"PicoSQLite"/"Logs"
        filename = strftime("%Y-%m-%d--%H-%M-%S") \
            + " PicoSQLite-Log.txt"
        return logdir/filename
    else:
        return None


def started_by_window_launcher():
    return Path(sys.executable).stem.endswith('w')


def running_without_console():
    return running_on_windows() and started_by_window_launcher()


def mkdir_p(path):
    """Similar to ``mkdir -p path``."""
    try:
        os.makedirs(path)
    except FileExistsError:
        pass


LOGGER_INITIALIZED = False

def init_logger(filename=None, level=None):
    logger_options = {}
    if filename is not None:
        mkdir_p(os.path.dirname(filename))
        logger_options['filename'] = os.fspath(filename)
        logger_options['filemode'] = 'w'
        # Not supported in version bellow
        if sys.version_info >= (3, 9):
            logger_options['encoding'] = 'utf8'
    else:
        logger_options['stream'] = sys.stdout
    logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s',
                        level=level,
                        **logger_options)
    logging.info('Starting')
    global LOGGER_INITIALIZED
    LOGGER_INITIALIZED = True


def respawn_without_console():
    if not running_on_windows():
        return
    win_exec = Path(sys.executable).parent/"pythonw.exe"
    if not win_exec.is_file():
        return
    argv = sys.argv.copy()
    argv.insert(0, os.fspath(win_exec))
    argv.append('--no-respawn')
    logging.info("cwd: %s", os.getcwd())
    logging.info("respawning: %r", argv)
    os.execv(win_exec, argv)


def build_cli():

    def log_level(text):
        try:
            return LOG_STR2LEVEL[text]
        except KeyError:
            raise argparse.ArgumentTypeError(
                "invalid log level '{}' (pick one in {})"
                .format(text, ", ".join(LOG_STR2LEVEL.keys())))

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "--no-respawn",
        action="store_true",
        help="Do not try to respawn without console on Windows.")
    parser.add_argument(
        "-l", "--logfile",
        action="store",
        help="Path to the log file.")
    parser.add_argument(
        "-v", "--verbose",
        type=log_level,
        default=LOG_LEVEL2STR[logging.INFO],
        action="store",
        help="Log verbose level.")
    parser.add_argument(
        "db_file",
        action="store",
        nargs="?",
        help="Path to the DB file to open.")
    parser.add_argument(
        "query_or_script",
        nargs='?',
        action="store",
        help="Script or query to run after start-up.")
    return parser


def main(argv):
    cli = build_cli()
    options = cli.parse_args(argv[1:])

    # Force to use a log file is running without a console.
    logfile = options.logfile
    if logfile is None and running_without_console():
        logfile = get_default_log_file()
    init_logger(filename=logfile, level=options.verbose)

    # Respawn without console
    if not options.no_respawn and not running_without_console():
        respawn_without_console()

    return start_gui(options.db_file,
                     query_or_script=options.query_or_script)


def protected_main(argv):
    global LOGGER_INITIALIZED
    LOGGER_INITIALIZED = False
    status = 0
    try:
        status = main(argv)
    except Exception:
        if LOGGER_INITIALIZED:
            logging.exception("Internal error")
        sys.stdout.flush()
        sys.stderr.flush()
        print("=" * 50, flush=True)
        traceback.print_exception(*sys.exc_info())
        if running_on_windows():
            input("Press ENTER to quit.")
    finally:
        return status


if __name__ == "__main__":
    sys.exit(protected_main(sys.argv))
