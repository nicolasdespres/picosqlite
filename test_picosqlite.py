# -*- encoding: utf-8 -*-
"""Unit test of picosqlite module.
"""


from unittest import TestCase
import re

from picosqlite import ColorSyntax


class TestColorSyntax(TestCase):

    def test_sql_string_pattern(self):
        rx = re.compile(ColorSyntax.SQL_STRING)
        subtestspecs = [
            ("simple %s D", "'asdf'"),
            ("empty %s D", "''"),
            ("escaped %s D", "'abc''def'"),
            ("end escape %s D", "'end'''"),
            ("squote %s D", "''''"),
            ("multiples %s, '2' D", "'1'"),
            ("multiples with escape %s, '2''A' D", "'1''T'"),
        ]
        for subtest, answer in subtestspecs:
            text = subtest % (answer,)
            with self.subTest(text=text):
                mo = rx.search(text)
                self.assertIsNotNone(mo)
                self.assertEqual(answer, mo[0])
