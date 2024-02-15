#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
#
# Copyright (c) 2021-today, Nicolas Desprès
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
"""A tiny sqlite view interface in TK intended for teaching.

No dependency apart from python 3.7.
"""

# Documentation: https://tkdocs.com

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
from typing import Type
import functools
import traceback
from collections import defaultdict
from pathlib import Path
import shlex
import logging
import subprocess as sp


LOGGER = logging.getLogger("picosqlite")
# Prevent logger to log anything until it is configured by init_logger.
# It may happens in the uncaught exception handler, if an exception is raised
# before the logger is completely initialized.
LOGGER.addHandler(logging.NullHandler())


def ensure_file_ext(filename, exts):
    path = Path(filename)
    if path.suffix in exts:
        return str(path)
    else:
        return str(path) + exts[0]


def running_on_windows():
    return sys.platform == 'win32'


def running_on_mac_os():
    return sys.platform == "darwin"


def running_on_linux():
    return sys.platform == "linux"


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

    def _repr(self, **extra_attrs):
        """Build repr of the instance in sub-classes."""
        attrs = dict(
            request=repr(self.request),
            started_at=self.started_at.isoformat(),
            stopped_at=self.stopped_at.isoformat(),
            error=repr(self.error),
            warning=repr(self.warning),
            internal_error=repr(self.internal_error),
            **extra_attrs)
        return str(type(self).__class__) \
            + "(" + ", ".join(f"{k}={v}" for k, v in attrs.items()) + ")"

    def __repr__(self):
        return self._repr()


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


def repr_long_rows(rows):
    if rows is None:
        return repr(None)
    else:
        return f"[...{len(rows)} items...]"


@dataclass
class TableRows(SQLResult):
    """Response when loading data from a table to view it."""

    rows: Optional[Rows] = None
    column_ids: Optional[ColumnIDS] = None
    column_names: Optional[ColumnNames] = None

    def __repr__(self):
        return self._repr(
            rows=repr_long_rows(self.rows),
            column_ids=repr(self.column_ids),
            column_names=repr(self.column_names),
        )


@dataclass
class QueryResult(SQLResult):
    rows: Optional[Rows] = None
    truncated: bool = False
    column_ids: Optional[ColumnIDS] = None
    column_names: Optional[ColumnNames] = None

    def __repr__(self):
        return self._repr(
            rows=repr_long_rows(self.rows),
            truncated=repr(self.truncated),
            column_ids=repr(self.column_ids),
            column_names=repr(self.column_names),
        )


class Task(threading.Thread):

    def __init__(self, root=None, **thread_kwargs):
        super().__init__(**thread_kwargs)
        assert root is not None
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
                if not isinstance(payload, dict):
                    # Save the payload type before to force it as an empty.
                    payload_type = type(payload)
                    # Force an empty dict so that the finally block works ok.
                    payload = {}
                    # Raise a proper error explaining the problem.
                    raise TypeError(
                        f"handler function {func.__name__} did not return a "
                        f"dict object, but a {payload_type.__name__}")
            except sqlite3.Error as e:
                error = e
            except sqlite3.Warning as w:
                warning = w
            except DirectiveError as e:
                error = e
            except Exception:
                internal_error = sys.exc_info()
            finally:
                stopped_at = datetime.now()
                assert isinstance(payload, dict)
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
    """Run SQL query in a different thread to allow interruption.

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
        # Protect parallel access to _db, _is_processing and _is_closing.
        # They could be accessed by the main GUI thread and the runner thread
        # at the same time.
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
        LOGGER.debug("put request: %r", request)
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
        assert self._db is not None
        schema = {}
        with self._lock:
            for table_name in self.list_tables():
                fields = list(self._db.execute(
                    f"pragma table_info('{table_name}')"))
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
            return self._handle_directive(parse_directive(query), request)
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
            raise DirectiveNotFound(argv)
        else:
            try:
                ret = handler(argv, request)
            except DirectiveError as e:
                e.directive = directive
                e.argv = argv
                raise e
            else:
                assert isinstance(ret, dict)
                return ret

    def _handle_directive_run(self, argv, request):
        if len(argv) != 2:
            raise DirectiveError(f"expects 1 argument, not {len(argv)}")
        self._run_script(argv[1])
        return dict()

    def _run_script(self, filename):
        with open(filename, mode='r', encoding='utf-8') as stream:
            self._executescript(stream.read())

    def _execute(self, *args, **kwargs):
        assert self._db is not None
        with self._lock:
            return self._db.execute(*args, **kwargs)

    def _executescript(self, *args, **kwargs):
        assert self._db is not None
        with self._lock:
            return self._db.executescript(*args, **kwargs)

    def list_tables(self):
        assert self._lock.locked()
        # Return a list and not a generator so that we can execute command
        # while iterating it.
        return list(iter_tables(self._db))

    def _handle_directive_dump(self, argv, request):
        if len(argv) != 2:
            raise DirectiveError(f"expects 1 argument, not {len(argv)}")
        self._dump(argv[1])
        return {}

    def _dump(self, filename):
        assert self._db is not None
        with open(filename, mode="w", encoding="utf-8") as stream, \
             self._lock:
            for line in self._db.iterdump():
                stream.write('%s\n' % (line,))

    def _handle_directive_drop_all_tables(self, argv, request):
        argc = len(argv)
        if argc != 1:
            raise DirectiveError(f"expects no argument, not {argc}")
        self._drop_all_tables()
        return {}

    def _drop_all_tables(self):
        assert self._db is not None
        with self._lock:
            for table_name in self.list_tables():
                self._db.execute(f"drop table {table_name};")


def parse_directive(text):
    return shlex.split(text.strip().rstrip(";"))


class DirectiveError(Exception):

    def __init__(self, message, argv=None, directive=None):
        self.message = message
        self.argv = argv
        self.directive = directive
        if self.directive is None and self.argv is not None:
            self.directive = self.argv[0]

    def __str__(self):
        return f".{self.directive}: {self.message}"


class DirectiveNotFound(DirectiveError):

    def __init__(self, argv):
        super().__init__("", argv=argv)

    def __str__(self):
        return f"invalid directive '{self.directive}'"


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
    elif 'NONE' in vtype:
        return None
    else:
        # May happen for column created without providing a type
        # in 'create table'.
        LOGGER.warning(
            f"unsupported sqlite type '{vtype}'; falling back to None")
        return None


def escape_sqlite_str(text):
    return "'" + text.replace("'", "''") + "'"


@dataclass
class Field:
    cid: int
    name: str
    vtype: Union[Type[Union[str, int, float, bytes]], None]
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

    INTERNALS = ("run", "dump", "drop_all_tables")

    def __init__(self):
        self._recompile()

    def _recompile(self):

        def mk_regex_any_word(words):
            return "|".join(re.escape(i) for i in words)

        self._sql_re = re.compile(
            r"""
              (?P<comment>    --.*$)
            | (?P<keyword>    \b(?i:%(keywords)s)\b)
            | (?P<directive>  \b(?i:%(directives)s)\b)
            | (?P<datatypes>  \b(?i:%(datatypes)s)\b)
            | (?P<internal>   ^\s*\.(?i:%(internals)s)\b)
            | (?P<string>     (%(string)s))
            """ % {
                "keywords": mk_regex_any_word(self.SQL_KEYWORDS),
                "directives": mk_regex_any_word(self.SQL_DIRECTIVES),
                "datatypes": mk_regex_any_word(self.SQL_DATATYPES),
                "internals": mk_regex_any_word(self.INTERNALS),
                "string": self.SQL_STRING,
            },
            re.MULTILINE | re.VERBOSE)

    def configure(self, text):
        keyword_fg = "#7F0055"
        field_fg = "green"
        text.tag_configure("keyword", foreground=keyword_fg)
        text.tag_configure("comment", foreground="#555555")
        text.tag_configure("directive", foreground=keyword_fg, underline=True)
        text.tag_configure("datatypes", foreground=field_fg, underline=True)
        text.tag_configure("internal", foreground="#E311E3")
        text.tag_configure("string", foreground="#8DC76F")

    def highlight(self, text, start, end):
        content = text.get(start, end)
        text.tag_remove("keyword", start, end)
        text.tag_remove("comment", start, end)
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


class Console(ttk.Panedwindow):

    def __init__(self, master=None, run_query_command=None,
                 command_log_maxlines=1000,
                 runnable_state_update_callback=None):
        super().__init__(master, orient=tk.VERTICAL)
        self._runnable_state_update_callback = runnable_state_update_callback
        self.command_log_maxlines = command_log_maxlines

        # **Query**
        self.query_frame = tk.Frame()
        self.query_text = ScrolledText(self.query_frame, wrap="word",
                                       background="white", foreground="black")
        self.query_text.bind("<<Modified>>", self.on_modified_query)
        assert run_query_command is not None
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
                                        foreground="black", height=100)
        self.cmdlog_text.configure(state=tk.DISABLED)
        self.cmdlog_text.rowconfigure(0, weight=1)
        self.cmdlog_text.columnconfigure(0, weight=1)
        self.cmdlog_text.tag_configure("error", foreground="#CC0000")
        self.cmdlog_text.tag_configure("warning", foreground="#f57010")

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
        if self._runnable_state_update_callback is not None:
            self._runnable_state_update_callback(tk.DISABLED)

    def get_current_query(self):
        return self.query_text.get('1.0', 'end').strip()

    def log(self, msg, tags=()):
        if not msg.endswith("\n"):
            msg += "\n"
        start_index = self.cmdlog_text.index("end -1c")
        write_to_tk_text_log(self.cmdlog_text, msg,
                             maxlines=self.command_log_maxlines,
                             tags=tags)
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
        state = self._get_run_query_bt_state()
        self.run_query_bt['state'] = state
        if self._runnable_state_update_callback is not None:
            self._runnable_state_update_callback(state)

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


def get_treeview_row_height():
    """Get the approximate height of a TreeView's row."""
    font = nametofont(ttk.Style().lookup("Treeview", "font"))
    return font.metrics("linespace")


class NamedTableView(TableView):

    # We load BUFFER_SIZE_FACTOR more items than we actually show.
    BUFFER_SIZE_FACTOR = 4

    @dataclass
    class State:
        """The state of the current view."""

        begin_window: int
        end_window: int
        visible_item: int

        @property
        def is_empty(self):
            return self.begin_window == 0 and self.end_window == 0

    def __init__(self, fetcher=None, **kwargs):
        super().__init__(**kwargs)
        assert fetcher is not None
        self.fetcher = fetcher
        self.tree['selectmode'] = 'extended'
        self.tree['yscrollcommand'] = self.lazy_load
        self.tree.bind("<Configure>", self.on_tree_configure)
        self.row_height = get_treeview_row_height()
        # The offset of the first and last (excluded) rows currently
        # loaded into the tree view.
        self.begin_window = 0
        self.end_window = 0  # excluded
        self.previous_visible_item = None
        # The limit that cannot be exceeded by the window size.
        self.max_window_size = None

    @property
    def table_name(self):
        return self.fetcher.table_name

    @property
    def nb_view_items(self):
        """Return the number of items currently loaded in the view."""
        return self.end_window - self.begin_window

    def row_from_fraction(self, fraction: float):
        """Compute the row from its fraction in the loaded window."""
        return int(self.nb_view_items * fraction) + self.begin_window

    def get_visible_item(self):
        values = self.ys.get()
        # In some rare cases, ys.get() may not return two values...
        if len(values) != 2:
            return
        ys_begin, ys_end = values
        return self.row_from_fraction(ys_begin)

    def insert(self, rows, column_ids, column_names, offset, limit):
        assert len(rows) <= limit
        first_row = offset
        last_row = offset + len(rows)  # excluded
        # If the table is empty or if the requested offset is over the last
        # row, we may have fetched no row.
        if first_row == last_row:
            LOGGER.debug("no row fetched between %d and %d",
                         first_row, last_row)
            return
        # Log that we are inserting rows in the tree view.
        ys_values = self.ys.get()
        # In some rare cases, ys.get() may not return two values...
        if len(ys_values) == 2:
            ys_begin, ys_end = ys_values
            LOGGER.debug(
                "inserting %d row(s) (asked %d) into %s from %d to %d; "
                "current=[%d, %d]; visible=[%d, %d]",
                len(rows), limit, self.table_name, first_row, last_row,
                self.begin_window, self.end_window,
                ys_begin, ys_end)
            del ys_begin, ys_end
        del ys_values
        # Build the row formatter.
        format_row = RowFormatter(column_ids, column_names)
        # If we currently have no item loaded at all.
        if self.begin_window == 0 and self.end_window == 0:
            assert self.nb_view_items == 0
            self.begin_window = self.end_window = first_row
        # Nothing to do, if the fetched area is within the current window.
        if self.begin_window <= first_row and last_row <= self.end_window:
            LOGGER.debug("fetched rows are within current window")
            return
        # TODO: Better explain what this code is meant to!
        # TODO: Can we get rid of self.previous_visible_item, by memorizing
        #       visible_item as an instance variable?
        if self.previous_visible_item is not None:
            visible_item = self.previous_visible_item
            self.previous_visible_item = None
        else:
            visible_item = self.get_visible_item()
        # Append part of the range beyond the current window, if the fetched
        # rows start inside the current window and potentially extend beyond.
        if self.begin_window <= first_row <= self.end_window:
            # Insert new items at the end
            excess = last_row - self.end_window
            LOGGER.debug("append %d items", excess)
            self._append_rows(rows[-excess:], format_row)
            # Delete exceeded items from the beginning.
            while self.nb_view_items > self.max_window_size:
                self.tree.delete(self.begin_window)
                self.begin_window += 1
        # Insert at the beginning part of the range before the current window,
        # if the fetched rows finished in the current window and potentially
        # start before.
        elif self.begin_window <= last_row <= self.end_window:
            # Insert new items from the beginning
            excess = self.begin_window - first_row
            LOGGER.debug("insert %d items at the beginning", excess)
            for row in reversed(rows[:excess]):
                self.begin_window -= 1
                self.tree.insert('', 0,
                                 iid=self.begin_window,
                                 values=format_row(row))
            # Delete exceeded items at the end.
            while self.nb_view_items > self.max_window_size:
                self.end_window -= 1
                self.tree.delete(self.end_window)
        # May happens if range entirely overlaps the current window, or
        # range is non-contiguous with the current window. This can be the
        # case if the windows is enlarged quickly or if we jump to another
        # part of the table far way from the current shown area.
        else:
            # Completely clear the tree view and insert all the items.
            LOGGER.debug(
                "fully overlapping or non-contiguous fetched ranges: "
                "window=[%d, %d]; fetched=[%d, %d]",
                self.begin_window, self.end_window,
                first_row, last_row)
            self.clear_all()
            self.begin_window = self.end_window = first_row
            self._append_rows(rows, format_row)
        # Adjust TreeView's column width to the newly inserted rows.
        format_row.configure_columns(self.tree)
        # Prevent auto-scroll down after inserting items.
        if self.nb_view_items > 0 and visible_item is not None:
            if not self.tree.exists(visible_item):
                # Scrollbar lower bound may lag during fast scrolling.
                visible_item = self.row_from_fraction(3/8)
            self.tree.see(visible_item)

    def _append_rows(self, rows, format_row):
        for row in rows:
            self.tree.insert('', 'end',
                             iid=self.end_window,
                             values=format_row(row))
            self.end_window += 1

    def lazy_load(self, begin_index, end_index):
        LOGGER.debug(f"lazy_load({begin_index}, {end_index})")
        limit = self.max_window_size - self.nb_view_items
        if limit < self.inc_limit:
            limit = self.inc_limit
        if self.begin_window > 0 and float(begin_index) <= 0.2:
            LOGGER.debug("fetch down")
            offset = self.begin_window - limit
            if offset < 0:
                offset = 0
            limit = self.begin_window - offset
            self.fetch(offset, limit)
        if float(end_index) >= 0.8:
            LOGGER.debug("fetch up")
            self.fetch(self.end_window, limit)
        return self.ys.set(begin_index, end_index)

    def on_tree_configure(self, event):
        LOGGER.debug("on_tree_configure")
        self.max_window_size = \
            round(event.height / self.row_height) * self.BUFFER_SIZE_FACTOR
        self._update_inc_limit()

    def _update_inc_limit(self):
        self.inc_limit = self.max_window_size // self.BUFFER_SIZE_FACTOR

    def fetch(self, offset, limit):
        LOGGER.debug(f"fetch {offset}, {limit}")
        self.fetcher(offset, limit)

    def save_state(self):
        return self.State(begin_window=self.begin_window,
                          end_window=self.end_window,
                          visible_item=self.get_visible_item())

    def restore_state(self, state):
        if state.is_empty:
            return
        LOGGER.debug("restore_state %r", state)
        self.clear_all()
        self.previous_visible_item = state.visible_item
        # TODO: better to extract the tree widget height and perform the
        #       same computation as in `on_tree_configure`.
        self.max_window_size = state.end_window - state.begin_window
        self._update_inc_limit()
        self.fetch(state.begin_window, self.max_window_size)

    def clear_all(self):
        assert self.end_window >= self.begin_window
        while self.end_window > self.begin_window:
            self.end_window -= 1
            self.tree.delete(self.end_window)


class Fetcher:

    def __init__(self, app, table_name):
        self.table_name = table_name
        self.app = app

    def __call__(self, offset, limit):
        self.app.statusbar.show(
            f"Loading {limit} records from table '{self.table_name}' "
            f"starting at offset {offset}...", delay=0.5)
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
    EXIT = "Exit"


class ViewMenu:
    REFRESH = "Refresh"
    CLOSE_RESULT = "Close current result tab"
    CLOSE_ALL_RESULTS = "Close all result tabs"


class ConsMenu:
    """Console menu item name."""

    RUN_QUERY = "Run query"
    CLEAR_CONSOLE = "Clear console"
    RUN_SCRIPT = "Run script..."
    INTERRUPT = "Interrupt"
    DELETE_ROWS = "Delete rows"
    DROP_ALL = "Drop all tables"


class Application(tk.Frame):

    NAME = "Pico SQLite"
    COMMAND_LOG_HISTORY = 1000

    def __init__(self, db_path=None, query=None, master=None):
        super().__init__(master)
        self.init_menu()
        self.init_widget()
        self.init_layout()
        self.init_logic()
        if db_path is not None:
            self.open_db(db_path)
        if query is not None:
            self.run_query(query)

    def init_widget(self):
        self.init_statusbar()
        self.pane = ttk.Panedwindow(self, orient=tk.VERTICAL)

        # Tables notebook
        self.tables = ttk.Notebook(self.pane, height=400,
                                   padding=(0, 0, 0, 0))
        self.tables.bind("<<NotebookTabChanged>>", self.on_view_table_changed)
        self.schema = SchemaFrame(master=self)

        # Console
        self.console = Console(
            self, run_query_command=self.run_query_action,
            command_log_maxlines=self.COMMAND_LOG_HISTORY,
            runnable_state_update_callback=self._update_run_query_state)

        self.pane.add(self.tables)
        self.pane.add(self.console)

    def init_statusbar(self):
        self.statusbar = StatusBar(self)
        self.statusbar.show(StatusMessage.READY_TO_OPEN)

    def init_menu(self):
        # Doc: https://tkdocs.com/tutorial/menus.html
        self.master.option_add('*tearOff', False)
        self.menubar = tk.Menu(self)
        # Set it as the menu of this app top-level window
        self.master.configure(menu=self.menubar)
        # **Database menu**
        self.db_menu = tk.Menu(self.menubar)
        self.menubar.add_cascade(label="Database", menu=self.db_menu)
        self.db_menu.add_command(label=DBMenu.NEW, command=self.new_action)
        self.db_menu.add_command(label=DBMenu.OPEN, command=self.open_action,
                                 accelerator="F2")
        self.db_menu.add_command(label=DBMenu.CLOSE,
                                 command=self.close_action,
                                 state=tk.DISABLED)
        self.db_menu.add_separator()
        self.db_menu.add_command(label=DBMenu.DUMP,
                                 command=self.dump_action,
                                 state=tk.DISABLED)
        self.db_menu.add_separator()
        self.db_menu.add_command(label=DBMenu.EXIT, command=self.exit_action)
        # **View menu**
        self.view_menu = tk.Menu(self.menubar)
        self.menubar.add_cascade(label="View", menu=self.view_menu)
        self.view_menu.add_command(label=ViewMenu.REFRESH,
                                   command=self.refresh_action,
                                   accelerator="F5",
                                   state=tk.DISABLED)
        self.view_menu.add_command(label=ViewMenu.CLOSE_RESULT,
                                   command=self.clear_result_action,
                                   accelerator="F7",
                                   state=tk.DISABLED)
        self.view_menu.add_command(label=ViewMenu.CLOSE_ALL_RESULTS,
                                   command=self.clear_all_results_action,
                                   state=tk.DISABLED)
        # **Console menu**
        self.console_menu = tk.Menu(self.menubar)
        self.menubar.add_cascade(label="Console", menu=self.console_menu)
        self.console_menu.add_command(label=ConsMenu.RUN_QUERY,
                                      command=self.run_query_action,
                                      accelerator="F3",
                                      state=tk.DISABLED)
        self.console_menu.add_command(label=ConsMenu.RUN_SCRIPT,
                                      command=self.run_script_action,
                                      state=tk.DISABLED)
        self.console_menu.add_command(label=ConsMenu.INTERRUPT,
                                      command=self.interrupt_action,
                                      accelerator="F12",
                                      state=tk.DISABLED)
        self.console_menu.add_command(label=ConsMenu.CLEAR_CONSOLE,
                                      command=self.clear_console,
                                      state=tk.DISABLED)
        self.console_menu.add_command(label=ConsMenu.DROP_ALL,
                                      command=self.drop_all_tables_action,
                                      state=tk.DISABLED)
        # **Help menu**
        self.help_menu = tk.Menu(self.menubar)
        self.menubar.add_cascade(label="Help", menu=self.help_menu)
        self.help_menu.add_command(label="Open data folder...",
                                   command=self.open_data_folder_action)
        self.help_menu.add_separator()
        self.help_menu.add_command(label="About...", command=self.about_action)

        # Explicitly bind the accelerator...
        self.master.bind_all("<F2>", lambda _: self.open_action())
        self.master.bind_all("<F3>", lambda _: self.run_query_action())
        self.master.bind_all("<F5>", lambda _: self.refresh_action())
        self.master.bind_all("<F7>", lambda _: self.clear_result_action())
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

    def open_data_folder_action(self):
        open_path_in_system_file_manager(get_data_folder())

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
            if self.sql.in_transaction:
                is_yes = askyesno(
                    parent=self,
                    title="SQL",
                    icon="warning",
                    message=""
                    "You are in the middle of a transaction.\n\n"
                    "Do you really want to quit and "
                    "lose the uncommitted data ?")
            else:
                is_yes = askyesno(
                    parent=self,
                    title="Quit confirmation",
                    message="Do you really want to quit?")
            if not is_yes:
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
            LOGGER.info("Try to close...")
            if self.sql.close():
                LOGGER.info("done closing.")
            else:
                if self.sql.is_processing:
                    LOGGER.info("Force interrupting...")
                    self.sql.force_interrupt()
                    LOGGER.info("Retry to close...")
                    if self.sql.close():
                        LOGGER.info("done closing.")
                    else:
                        LOGGER.error("Failed to close... Too bad!")
        LOGGER.info("Good bye")

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
        LOGGER.debug("opening DB")

    def close_db(self):
        if not self.safely_close_db():
            return
        self.master.title(self.NAME)
        self.console.disable()
        self.db_menu.entryconfigure(DBMenu.CLOSE, state=tk.DISABLED)
        self.db_menu.entryconfigure(DBMenu.DUMP, state=tk.DISABLED)
        self.view_menu.entryconfigure(ViewMenu.REFRESH, state=tk.DISABLED)
        self.console_menu.entryconfigure(ConsMenu.RUN_QUERY, state=tk.DISABLED)
        self.console_menu.entryconfigure(ConsMenu.RUN_SCRIPT,
                                         state=tk.DISABLED)
        self.console_menu.entryconfigure(ConsMenu.INTERRUPT, state=tk.DISABLED)
        self.console_menu.entryconfigure(ConsMenu.DROP_ALL, state=tk.DISABLED)
        self.statusbar.show(StatusMessage.READY_TO_OPEN)
        self.statusbar.set_in_transaction(False)
        LOGGER.debug("unload tables when closing DB")
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
        LOGGER.debug("get request's result: %r", result)
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
            LOGGER.debug("load table after database opening")
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
        self.unload_tables()  # Unload tables before to load them.
        self.tables.add(self.schema, text=self.schema.TAB_NAME)
        for table_name, fields in schema.items():
            self.schema.add_table(table_name, fields)
            table_view = NamedTableView(fetcher=Fetcher(self, table_name))
            self.table_views[table_name] = table_view
            self.tables.add(table_view, text=table_name)
            if table_name in self.table_view_saved_states:
                table_view.restore_state(
                    self.table_view_saved_states[table_name])
        self.schema.finish_table_insertion()
        if self.selected_table_index is not None \
           and 0 <= self.selected_table_index < self.tables.index('end'):
            self.tables.select(self.selected_table_index)
            self.selected_table_index = None
        self.table_view_saved_states = {}
        self.db_menu.entryconfigure(DBMenu.CLOSE, state=tk.NORMAL)
        self.disable_sql_execution_state()
        self.statusbar.show(StatusMessage.READY)

    def on_sql_TableRows(self, result: TableRows):
        """Handle rows fetched from table."""
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
            LOGGER.debug("automatic refresh")
            self.refresh_action()
        else:
            table_view.insert(result.rows,
                              result.column_ids, result.column_names,
                              result.request.offset, result.request.limit)

    def refresh_action(self):
        self.selected_table_index = get_selected_tab_index(self.tables)
        self.save_table_view_states()
        LOGGER.debug("load tables for refreshing")
        self.load_tables()
        if self.sql is not None:
            self.last_refreshed_at = self.sql.last_modification_time

    def save_table_view_states(self):
        self.table_view_saved_states = {
            n: tv.save_state()
            for n, tv in self.table_views.items()
        }

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

    def on_view_table_changed(self, event):
        tables_notebook = event.widget
        selected_tab = tables_notebook.select()
        if not selected_tab:
            return
        is_result_tab = self.is_result_view_tab(selected_tab)
        self.view_menu.entryconfigure(
            ViewMenu.CLOSE_RESULT,
            state=tk.NORMAL if is_result_tab else tk.DISABLED)

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
            LOGGER.debug("refresh after query with no result")
            self.refresh_action()
        else:
            tab_name = f"*Result-{self.result_view_count}"
            result_table = ResultTableView()
            result_table.append(result.rows,
                                result.column_ids,
                                result.column_names,
                                result.truncated)
            self.tables.insert(0, result_table,
                               text=tab_name)
            self.result_view_count += 1
            self.view_menu.entryconfigure(ViewMenu.CLOSE_RESULT,
                                          state=tk.NORMAL)
            self.view_menu.entryconfigure(ViewMenu.CLOSE_ALL_RESULTS,
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
        self.console_menu.entryconfigure(ConsMenu.CLEAR_CONSOLE,
                                         state=tk.DISABLED)

    def log(self, msg, tags=()):
        self.console.log(msg, tags=tags)
        self.console_menu.entryconfigure(ConsMenu.CLEAR_CONSOLE,
                                         state=tk.NORMAL)

    def log_error(self, e):
        self.log(f"Error: {e}\n", tags=("error",))

    def log_warning(self, w):
        self.log(f"Warning: {w}\n", tags=("warning",))

    def log_internal_error(self, etype, value, tb):
        self.log("Internal error!!!\n", tags=("error",))
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
        self.view_menu.entryconfigure(ViewMenu.CLOSE_ALL_RESULTS,
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

    def _update_run_query_state(self, state):
        self.console_menu.entryconfigure(ConsMenu.RUN_QUERY, state=state)

    def enable_sql_execution_state(self):
        self.console.disable()
        self.db_menu.entryconfigure(DBMenu.DUMP, state=tk.DISABLED)
        self.view_menu.entryconfigure(ViewMenu.REFRESH, state=tk.DISABLED)
        self.console_menu.entryconfigure(ConsMenu.RUN_SCRIPT,
                                         state=tk.DISABLED)
        self.console_menu.entryconfigure(ConsMenu.INTERRUPT, state=tk.NORMAL)
        self.console_menu.entryconfigure(ConsMenu.DROP_ALL, state=tk.DISABLED)

    def disable_sql_execution_state(self):
        self.console.enable()
        self.db_menu.entryconfigure(DBMenu.DUMP, state=tk.NORMAL)
        self.view_menu.entryconfigure(ViewMenu.REFRESH, state=tk.NORMAL)
        self.console_menu.entryconfigure(ConsMenu.RUN_SCRIPT, state=tk.NORMAL)
        self.console_menu.entryconfigure(ConsMenu.INTERRUPT, state=tk.DISABLED)
        self.console_menu.entryconfigure(ConsMenu.DROP_ALL, state=tk.NORMAL)

    def create_task(self, task_class, *args, **kwargs):
        return task_class(*args, root=self.master, **kwargs)

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

    def drop_all_tables_action(self):
        if self.sql is None:
            return False
        is_yes = askyesno(
            parent=self,
            title="Drop all tables confirmation",
            message="Do you really want to drop all tables of the database?")
        if not is_yes:
            return False
        self.drop_all_tables()
        return True

    def drop_all_tables(self):
        assert self.sql is not None
        self.run_query(".drop_all_tables")


def write_to_tk_text_log(log, msg, maxlines=Application.COMMAND_LOG_HISTORY, tags=()):
    numlines = int(log.index('end - 1 line').split('.')[0])
    log['state'] = tk.NORMAL
    if numlines >= maxlines:
        log.delete('1.0', f'{maxlines - numlines}.0')
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
        return "w" if issubclass(t, str) else "e"

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
    return '' if v is None else v


def iter_tables(db):
    cursor = db.execute(
        "SELECT name "
        "FROM sqlite_master "
        "WHERE type='table' AND name NOT LIKE 'sqlite_%';")
    for row in cursor:
        yield row[0]


def log_widget_hierarchy(w, depth=0):
    """Print widget ownership hierarchy."""
    LOGGER.info('  '*depth + w.winfo_class()
                + ' w=' + str(w.winfo_width())
                + ' h=' + str(w.winfo_height())
                + ' x=' + str(w.winfo_x())
                + ' y=' + str(w.winfo_y()))
    for i in w.winfo_children():
        log_widget_hierarchy(i, depth+1)


def open_path_in_system_file_manager(path):
    if running_on_mac_os():
        # http://stackoverflow.com/a/3520693/261181
        # -R doesn't allow showing hidden folders
        sp.run(["open", os.fspath(path)])
    elif running_on_linux():
        sp.run(["xdg-open", os.fspath(path)])
    elif running_on_windows():
        sp.run(["explorer", os.fspath(path)])
    else:
        raise RuntimeError(
            "do not know how to open path "
            f"on platform '{sys.platform}'")


def start_gui(db_path, query=None):
    root = tk.Tk()
    root.geometry("600x800")
    app = Application(db_path=db_path, query=query, master=root)
    root.protocol('WM_DELETE_WINDOW', app.exit_action)
    root.report_callback_exception = _on_tk_exception
    try:
        root.mainloop()
    except SystemExit:
        app.destroy()
    except Exception:
        report_exception(*sys.exc_info())
    return 0


def _on_tk_exception(etype, value, tb):
    report_exception(etype, value, tb)


def read_all_lines(filename):
    with open(filename) as f:
        return list(f)


def report_exception(etype, value, tb,
                     title: str = "Pico SQLite's internal error"):
    LOGGER.exception(title)
    error_lines = traceback.format_exception(etype, value, tb)
    separator = '-------------------\n'
    header = [
        f"PicoSQLite version: {__version__}\n",
        f"Python version: {sys.version}\n",
        separator,
    ]
    log_content = read_all_lines(LOG_FILENAME)
    LOG_LIMIT = 4096
    if len(log_content) > LOG_LIMIT:
        log_content = log_content[:LOG_LIMIT] + ['--- truncated long log ---']
    log_content.insert(0, separator)
    LongTextDialog(title, "".join(header + error_lines + log_content))


class LongTextDialog(tk.Toplevel):

    def __init__(self, title, text_content, master=None):
        super().__init__(master=master)
        self.title(title)

        self.main_frame = ttk.Frame(master=self)
        self.main_frame.grid(row=0, column=0, sticky="nsew")
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        self.text = ScrolledText(
            master=self.main_frame,
            wrap="none",
            width=80,
            height=10,
            relief="sunken",
            borderwidth=1,
        )
        self.text.grid(row=1, column=0, columnspan=2, sticky="nsew",
                       padx=20, pady=20)
        self.text.delete('1.0', tk.END)
        self.text.insert('end', text_content)
        self.text.see("1.0")

        self.copy_button = ttk.Button(
            master=self.main_frame,
            command=self.copy_action,
            text="Copy to clipboard",
            width=20
        )
        self.copy_button.grid(row=2, column=0, sticky="w",
                              padx=20, pady=(0, 20))

        self.close_button = ttk.Button(
            master=self.main_frame,
            command=self.close_action,
            text="Close",
            default="active",
        )
        self.close_button.grid(row=2, column=1, sticky="w",
                               padx=20, pady=(0, 20))
        self.close_button.focus_set()

        self.main_frame.columnconfigure(0, weight=1)
        self.main_frame.rowconfigure(1, weight=1)

        self.protocol("WM_DELETE_WINDOW", self.close_action)
        self.bind("<Escape>", self.close_action, True)

    def copy_action(self):
        self.clipboard_clear()
        self.clipboard_append(self.text.get("1.0", "end"))

    def close_action(self):
        self.destroy()


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


def get_data_folder():
    if running_on_windows():
        return Path(os.environ["LOCALAPPDATA"])/"PicoSQLite"
    elif running_on_mac_os():
        return Path(os.environ["HOME"])/"Library"/"PicoSQLite"
    else:
        raise RuntimeError("no data folder for platform '{sys.platform}'")


def mk_log_time():
    return strftime("%Y-%m-%d--%H-%M-%S")


def mk_log_filename():
    data_folder = get_data_folder()
    log_dir = data_folder/"Logs"
    filename = mk_log_time() + " PicoSQLite-Log.txt"
    return log_dir/filename


def running_without_console():
    return sys.stdout is None


def mkdir_p(path):
    """Similar to ``mkdir -p path``."""
    try:
        os.makedirs(path)
    except FileExistsError:
        pass

LOG_FILENAME = None

def init_logger(logger, level=logging.INFO):
    if sys.stdout is None:
        console_handler = None
    else:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter(
            "%(name)s: %(levelname)s: %(message)s"))
        console_handler.setLevel(level)

    global LOG_FILENAME
    LOG_FILENAME = mk_log_filename()
    mkdir_p(os.path.dirname(LOG_FILENAME))
    file_handler = logging.FileHandler(LOG_FILENAME,
                                       mode="w", encoding="utf-8")

    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s: %(levelname)s: %(message)s"))
    file_handler.setLevel(logging.DEBUG)

    logger.setLevel(-1)  # pass all messages to handlers
    if console_handler is not None:
        logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.info("Starting")
    logger.info("PicoSQLite version: %s", __version__)
    logger.info("Python version: %s", sys.version)
    logger.info("Started at: %s", mk_log_time())


def respawn_without_console():
    if not running_on_windows():
        return
    win_exec = Path(sys.executable).parent/"pythonw.exe"
    if not win_exec.is_file():
        return
    argv = sys.argv.copy()
    argv.insert(0, os.fspath(win_exec))
    argv.append('--no-respawn')
    LOGGER.info("cwd: %s", os.getcwd())
    LOGGER.info("respawning: %r", argv)
    # Uses subprocess.Popen since os.execv does not work well on some
    # Windows machine (at LCP for instance).
    sp.Popen(argv, executable=win_exec)
    sys.exit(0)


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
        "query",
        nargs='?',
        action="store",
        help="Query to run at start-up.")
    return parser


def main(argv):
    cli = build_cli()
    options = cli.parse_args(argv[1:])
    init_logger(LOGGER, level=options.verbose)
    # Respawn without console
    if not options.no_respawn and not running_without_console():
        respawn_without_console()
    return start_gui(options.db_file, query=options.query)


def protected_main(argv):
    status = 0
    try:
        status = main(argv)
    except Exception:
        LOGGER.exception("Un-caught exception")
        sys.stdout.flush()
        sys.stderr.flush()
        if running_on_windows():
            input("Press ENTER to quit.")
        status = 1
    finally:
        return status


if __name__ == "__main__":
    sys.exit(protected_main(sys.argv))
