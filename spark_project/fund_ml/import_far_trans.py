"""Validate the FAR-Trans release and generate a transactional MySQL import."""

import argparse
import csv
import hashlib
import io
import re
import sys
import urllib.error
import urllib.request
import zipfile
from datetime import datetime
from decimal import Context, Decimal, InvalidOperation


# University of Glasgow, DOI 10.5525/gla.researchdata.1658, CC BY 4.0.
RELEASE_URL = "https://researchdata.gla.ac.uk/1658/1/FAR-Trans.zip"
RELEASE_SHA256 = "d0d6cfe027732bab45fdcf9af80eff4fefddc789aedae9bdce0d3d6cfd05cfb6"
RELEASE_COVERAGE_END = "2022-11-29"
MAX_ARCHIVE_BYTES = 16 * 1024 * 1024
EXPECTED_ROW_COUNTS = (32468, 318, 29666)
AMOUNT_PRECISION = Decimal("0.000000000000000001")
AMOUNT_LIMIT = Decimal("1e14")
AMOUNT_CONTEXT = Context(prec=32)
BATCH_ROWS = 500


def read_archive(archive_path):
    if archive_path:
        with open(archive_path, "rb") as stream:
            data = stream.read(MAX_ARCHIVE_BYTES + 1)
    else:
        with urllib.request.urlopen(RELEASE_URL, timeout=60) as stream:
            data = stream.read(MAX_ARCHIVE_BYTES + 1)
    if len(data) > MAX_ARCHIVE_BYTES:
        raise ValueError("FAR-Trans ZIP exceeds the expected size limit.")
    if hashlib.sha256(data).hexdigest() != RELEASE_SHA256:
        raise ValueError("FAR-Trans ZIP does not match the pinned SHA-256.")
    return zipfile.ZipFile(io.BytesIO(data))


def csv_rows(archive, filename, fields):
    with archive.open("FAR-Trans/" + filename + ".csv") as raw:
        with io.TextIOWrapper(raw, encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream, strict=True)
            if not reader.fieldnames or not set(fields).issubset(reader.fieldnames):
                raise ValueError("Unexpected FAR-Trans columns in {}.".format(filename))
            for row in reader:
                if any(row[field] is None for field in fields):
                    raise ValueError("Incomplete FAR-Trans row in {}.".format(filename))
                yield row


def checked_date(value):
    parsed = datetime.strptime(value, "%Y-%m-%d").date()
    if parsed.isoformat() != value:
        raise ValueError("FAR-Trans date must use YYYY-MM-DD.")
    return parsed


def checked_id(value, pattern):
    if re.fullmatch(pattern, value) is None:
        raise ValueError("Invalid identifier in FAR-Trans release.")
    return value


def checked_category(value):
    if not value or len(value) > 64:
        raise ValueError("Missing or oversized FAR-Trans category.")
    return value


def checked_amount(value):
    amount = Decimal(value)
    if not amount.is_finite() or not 0 < amount < AMOUNT_LIMIT:
        raise ValueError("FAR-Trans trade amount must be positive and fit DECIMAL(32,18).")
    amount = amount.quantize(AMOUNT_PRECISION, context=AMOUNT_CONTEXT)
    if not 0 < amount < AMOUNT_LIMIT:
        raise ValueError("FAR-Trans trade amount cannot be represented to 18 decimals.")
    return amount


def distinct_rows(rows, key_size, name):
    seen = set()
    data = []
    for row in rows:
        key = row[:key_size]
        if key in seen:
            raise ValueError("Duplicate business key in {}.".format(name))
        seen.add(key)
        data.append(row)
    return data


def customer_rows(archive):
    fields = ("customerID", "timestamp", "customerType", "riskLevel", "investmentCapacity")
    for row in csv_rows(archive, "customer_information", fields):
        yield (
            checked_id(row["customerID"], r"[A-Za-z0-9]{1,32}"),
            checked_date(row["timestamp"]),
            checked_category(row["customerType"]),
            checked_category(row["riskLevel"]),
            checked_category(row["investmentCapacity"]),
        )


def fund_rows(archive):
    fields = ("ISIN", "minDate", "maxDate")
    limits = distinct_rows(
        ((r["ISIN"], checked_date(r["minDate"]), checked_date(r["maxDate"]))
         for r in csv_rows(archive, "limit_prices", fields)),
        1, "limit_prices",
    )
    limits_by_isin = {isin: (start, end) for isin, start, end in limits}
    fields = ("ISIN", "timestamp", "assetCategory", "assetSubCategory")
    for row in csv_rows(archive, "asset_information", fields):
        if row["assetCategory"] != "MTF":
            continue
        isin = checked_id(row["ISIN"], r"[A-Z0-9]{12}")
        if isin not in limits_by_isin:
            raise ValueError("FAR-Trans fund has no price coverage record.")
        first_price_date, last_price_date = limits_by_isin[isin]
        if first_price_date > last_price_date:
            raise ValueError("FAR-Trans fund price coverage is reversed.")
        yield (
            isin,
            checked_date(row["timestamp"]),
            checked_category(row["assetSubCategory"]),
            first_price_date,
            last_price_date,
        )


def fund_trade_rows(archive, customers, funds):
    fields = ("customerID", "ISIN", "timestamp", "transactionType", "transactionID", "totalValue")
    for row in csv_rows(archive, "transactions", fields):
        if row["ISIN"] not in funds:
            continue
        if row["customerID"] not in customers or row["transactionType"] not in ("Buy", "Sell"):
            raise ValueError("FAR-Trans fund trade has an unknown customer or trade type.")
        yield (
            row["customerID"],
            row["ISIN"],
            checked_date(row["timestamp"]),
            row["transactionType"],
            checked_id(row["transactionID"], r"-?[A-Za-z0-9]{1,31}"),
            checked_amount(row["totalValue"]),
        )


def read_release(archive_path):
    with read_archive(archive_path) as archive:
        customers = distinct_rows(customer_rows(archive), 2, "far_customer")
        funds = distinct_rows(fund_rows(archive), 2, "far_asset")
        trades = distinct_rows(
            fund_trade_rows(archive, {r[0] for r in customers}, {r[0] for r in funds}),
            5, "far_trade",
        )
    observed = (len(customers), len(funds), len(trades))
    if observed != EXPECTED_ROW_COUNTS:
        raise ValueError("FAR-Trans row counts {} differ from {}.".format(observed, EXPECTED_ROW_COUNTS))
    return customers, funds, trades


def sql_literal(value):
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, int):
        return str(value)
    if not isinstance(value, str):
        value = value.isoformat()
    # Encoding strings as hex keeps the import independent of SQL escape modes.
    return "CONVERT(X'{}' USING utf8mb4)".format(value.encode("utf-8").hex())


def emit_upsert(table, columns, key_size, rows):
    replacement = ", ".join("{0} = VALUES({0})".format(col) for col in columns[key_size:])
    for start in range(0, len(rows), BATCH_ROWS):
        batch = rows[start:start + BATCH_ROWS]
        print("INSERT INTO {} ({}) VALUES".format(table, ", ".join(columns)))
        print(",\n".join("(" + ", ".join(sql_literal(value) for value in row) + ")" for row in batch))
        print("ON DUPLICATE KEY UPDATE {};".format(replacement))


def emit_import(customers, funds, trades):
    counts = (len(customers), len(funds), len(trades))
    print("SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION';")
    print("START TRANSACTION;")
    # The version row serializes two loaders and becomes visible with the data.
    emit_upsert(
        "far_dataset", ("data_version", "customer_rows", "asset_rows", "trade_rows", "coverage_end"),
        1, [(RELEASE_SHA256,) + counts + (RELEASE_COVERAGE_END,)],
    )
    emit_upsert(
        "far_customer", ("customer_id", "profile_date", "customer_type", "risk_level", "investment_capacity"),
        2, customers,
    )
    emit_upsert(
        "far_asset", ("isin", "asset_date", "fund_category", "first_price_date", "last_price_date"),
        2, funds,
    )
    emit_upsert(
        "far_trade", ("customer_id", "isin", "trade_date", "trade_type", "transaction_id", "amount"),
        5, trades,
    )
    print("COMMIT;")
    print("SELECT data_version, customer_rows, asset_rows, trade_rows, coverage_end FROM far_dataset;")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("zip_path", nargs="?", help="Optional local copy of the pinned FAR-Trans ZIP.")
    args = parser.parse_args()
    try:
        release = read_release(args.zip_path)
    except (OSError, urllib.error.URLError, zipfile.BadZipFile, KeyError, csv.Error, ValueError, InvalidOperation) as error:
        parser.exit(1, "FAR-Trans import failed: {}\n".format(error))
    emit_import(*release)


if __name__ == "__main__":
    main()
