import os
import re
from datetime import date


DEFAULT_FROM = os.environ.get("DEMO_DATE_FROM", "2025-01-06")
DEFAULT_TO = os.environ.get("DEMO_DATE_TO", "2025-01-31")
DEFAULT_SCHEMA = "spark_project_demo"


def validate_args(args, spec):
    if not re.match(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z", args.run_id):
        raise ValueError("Use 1-64 letters, digits, underscores or hyphens in --run-id.")
    if not re.match(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z", args.code_version):
        raise ValueError("--code-version must identify the submitted source.")
    for value in (args.date_from, args.date_to):
        if not re.match(r"[0-9]{4}-[0-9]{2}-[0-9]{2}\Z", value):
            raise ValueError("Use YYYY-MM-DD for the date range.")
        date(*map(int, value.split("-")))
    if args.date_from > args.date_to:
        raise ValueError("The first date must be on or before the last date.")
    if args.strategy == "preagg" and not spec.allows_preagg:
        raise ValueError("Preaggregation is available for daily_fund_type only.")


def mysql_schema():
    schema = os.environ.get("DEMO_MYSQL_SCHEMA") or DEFAULT_SCHEMA
    if not re.match(r"[A-Za-z0-9_]{1,64}\Z", schema):
        raise ValueError("DEMO_MYSQL_SCHEMA must use letters, digits or underscores.")
    return schema


def jdbc_settings():
    names = ("DEMO_JDBC_URL", "DEMO_MYSQL_USER", "DEMO_MYSQL_PASSWORD")
    missing = [name for name in names if not os.environ.get(name)]
    if missing:
        raise ValueError("Missing environment variables: " + ", ".join(missing))
    schema = mysql_schema()
    url = os.environ["DEMO_JDBC_URL"]
    pattern = r"jdbc:mysql://[^/?#@]+/" + re.escape(schema) + r"(?:\?[^#]*)?\Z"
    if not re.match(pattern, url):
        raise ValueError("DEMO_JDBC_URL must target the " + schema + " schema.")
    return url, {
        "driver": os.environ.get("DEMO_JDBC_DRIVER") or "com.mysql.cj.jdbc.Driver",
        "user": os.environ["DEMO_MYSQL_USER"],
        "password": os.environ["DEMO_MYSQL_PASSWORD"],
    }


def model_root(required=False):
    root = os.environ.get("DEMO_MODEL_ROOT", "").strip().rstrip("/")
    if required and not root:
        raise ValueError("Set DEMO_MODEL_ROOT to a model destination visible to Spark.")
    return root
