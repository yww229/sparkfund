"""Unit tests for argument, schema and connection settings (no Spark needed)."""

import argparse
import importlib
import os
import unittest
from types import SimpleNamespace

from spark_project import settings


CONFIG_NAMES = (
    "DEMO_DATE_FROM",
    "DEMO_DATE_TO",
    "DEMO_MYSQL_SCHEMA",
    "DEMO_JDBC_URL",
    "DEMO_MYSQL_USER",
    "DEMO_MYSQL_PASSWORD",
    "DEMO_JDBC_DRIVER",
    "DEMO_MODEL_ROOT",
)


def build_args(**overrides):
    values = {
        "run_id": "demo_001",
        "code_version": "abc123",
        "date_from": "2025-01-06",
        "date_to": "2025-01-31",
        "strategy": "direct",
    }
    values.update(overrides)
    return argparse.Namespace(**values)


ETL_SPEC = SimpleNamespace(allows_preagg=False)
PREAGG_SPEC = SimpleNamespace(allows_preagg=True)


class SettingsTest(unittest.TestCase):
    def setUp(self):
        self.saved = dict(os.environ)
        for name in CONFIG_NAMES:
            os.environ.pop(name, None)
        self.settings = importlib.reload(settings)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.saved)
        importlib.reload(settings)

    def test_default_date_window(self):
        self.assertEqual(
            (self.settings.DEFAULT_FROM, self.settings.DEFAULT_TO),
            ("2025-01-06", "2025-01-31"),
        )

    def test_date_window_from_environment(self):
        os.environ["DEMO_DATE_FROM"] = "2024-01-01"
        os.environ["DEMO_DATE_TO"] = "2024-12-31"
        reloaded = importlib.reload(settings)
        self.assertEqual(
            (reloaded.DEFAULT_FROM, reloaded.DEFAULT_TO), ("2024-01-01", "2024-12-31")
        )

    def test_validate_args_accepts_a_valid_submission(self):
        self.settings.validate_args(build_args(), ETL_SPEC)

    def test_validate_args_rejects_bad_run_id(self):
        for run_id in ("", "-leading", "has space", "x" * 65):
            with self.assertRaises(ValueError):
                self.settings.validate_args(build_args(run_id=run_id), ETL_SPEC)

    def test_validate_args_rejects_bad_code_version(self):
        for version in ("", "with space", "x" * 65):
            with self.assertRaises(ValueError):
                self.settings.validate_args(build_args(code_version=version), ETL_SPEC)

    def test_validate_args_rejects_bad_dates(self):
        with self.assertRaises(ValueError):
            self.settings.validate_args(build_args(date_from="2025-1-6"), ETL_SPEC)
        with self.assertRaises(ValueError):
            self.settings.validate_args(build_args(date_from="2025-02-30"), ETL_SPEC)
        with self.assertRaises(ValueError):
            self.settings.validate_args(
                build_args(date_from="2025-02-01", date_to="2025-01-31"), ETL_SPEC
            )

    def test_preagg_requires_a_spec_that_allows_it(self):
        with self.assertRaises(ValueError):
            self.settings.validate_args(build_args(strategy="preagg"), ETL_SPEC)
        self.settings.validate_args(build_args(strategy="preagg"), PREAGG_SPEC)

    def test_mysql_schema_default_and_override(self):
        self.assertEqual(self.settings.mysql_schema(), "spark_project_demo")
        os.environ["DEMO_MYSQL_SCHEMA"] = "team_a_demo"
        self.assertEqual(self.settings.mysql_schema(), "team_a_demo")

    def test_mysql_schema_rejects_unsafe_names(self):
        for schema in ("demo;drop", "demo-name", "x" * 65):
            os.environ["DEMO_MYSQL_SCHEMA"] = schema
            with self.assertRaises(ValueError):
                self.settings.mysql_schema()

    def test_jdbc_settings_requires_every_variable(self):
        with self.assertRaises(ValueError) as caught:
            self.settings.jdbc_settings()
        for name in ("DEMO_JDBC_URL", "DEMO_MYSQL_USER", "DEMO_MYSQL_PASSWORD"):
            self.assertIn(name, str(caught.exception))

    def test_jdbc_settings_rejects_another_schema(self):
        os.environ.update(
            DEMO_JDBC_URL="jdbc:mysql://127.0.0.1:3306/other_schema",
            DEMO_MYSQL_USER="demo",
            DEMO_MYSQL_PASSWORD="secret",
        )
        with self.assertRaises(ValueError):
            self.settings.jdbc_settings()

    def test_jdbc_settings_returns_default_driver(self):
        os.environ.update(
            DEMO_JDBC_URL="jdbc:mysql://127.0.0.1:3306/spark_project_demo?sslMode=DISABLED",
            DEMO_MYSQL_USER="demo",
            DEMO_MYSQL_PASSWORD="secret",
        )
        url, properties = self.settings.jdbc_settings()
        self.assertEqual(url, os.environ["DEMO_JDBC_URL"])
        self.assertEqual(properties["driver"], "com.mysql.cj.jdbc.Driver")
        self.assertEqual(properties["user"], "demo")
        self.assertEqual(properties["password"], "secret")

    def test_model_root_strips_trailing_slash_and_honours_required(self):
        with self.assertRaises(ValueError):
            self.settings.model_root(required=True)
        self.assertEqual(self.settings.model_root(), "")
        os.environ["DEMO_MODEL_ROOT"] = "file:///tmp/fund-models/"
        self.assertEqual(self.settings.model_root(required=True), "file:///tmp/fund-models")


if __name__ == "__main__":
    unittest.main()
