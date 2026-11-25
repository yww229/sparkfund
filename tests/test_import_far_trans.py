"""Unit tests for the FAR-Trans import validators (no network or MySQL)."""

import io
import unittest
import zipfile
from datetime import date
from decimal import Decimal

from spark_project.fund_ml import import_far_trans as far


def memory_archive(files):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, text in files.items():
            archive.writestr("FAR-Trans/" + name + ".csv", text)
    buffer.seek(0)
    return zipfile.ZipFile(buffer)


class CsvRowsTest(unittest.TestCase):
    def test_reads_requested_fields(self):
        with memory_archive({"sample": "a,b\n1,2\n3,4\n"}) as archive:
            rows = list(far.csv_rows(archive, "sample", ("a", "b")))
        self.assertEqual(rows, [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}])

    def test_rejects_a_missing_column(self):
        with memory_archive({"sample": "a,b\n1,2\n"}) as archive:
            with self.assertRaises(ValueError):
                list(far.csv_rows(archive, "sample", ("a", "c")))

    def test_rejects_an_incomplete_row(self):
        with memory_archive({"sample": "a,b\n1\n"}) as archive:
            with self.assertRaises(ValueError):
                list(far.csv_rows(archive, "sample", ("a", "b")))


class FieldValidatorTest(unittest.TestCase):
    def test_checked_date_round_trip(self):
        self.assertEqual(far.checked_date("2022-11-29"), date(2022, 11, 29))
        for value in ("2022-11-9", "29-11-2022", "2022-02-30"):
            with self.assertRaises(ValueError):
                far.checked_date(value)

    def test_checked_id_uses_the_whole_value(self):
        self.assertEqual(far.checked_id("ABC123", r"[A-Z0-9]{6}"), "ABC123")
        for value in ("abc123", "ABC1234", " ABC123"):
            with self.assertRaises(ValueError):
                far.checked_id(value, r"[A-Z0-9]{6}")

    def test_checked_category_rejects_empty_and_long_values(self):
        self.assertEqual(far.checked_category("MTF"), "MTF")
        with self.assertRaises(ValueError):
            far.checked_category("")
        with self.assertRaises(ValueError):
            far.checked_category("x" * 65)

    def test_checked_amount_keeps_positive_scale(self):
        self.assertEqual(far.checked_amount("100.50"), Decimal("100.50"))
        for value in ("0", "-1", "NaN", "Infinity", "1e15"):
            with self.assertRaises(ValueError):
                far.checked_amount(value)


class SqlLiteralTest(unittest.TestCase):
    def test_decimal_and_int_are_plain_numbers(self):
        self.assertEqual(far.sql_literal(Decimal("12.340")), "12.340")
        self.assertEqual(far.sql_literal(7), "7")

    def test_strings_and_dates_use_hex_utf8(self):
        self.assertEqual(
            far.sql_literal("ISIN1"), "CONVERT(X'4953494e31' USING utf8mb4)"
        )
        self.assertEqual(
            far.sql_literal(date(2025, 1, 6)), "CONVERT(X'323032352d30312d3036' USING utf8mb4)"
        )


class DistinctRowsTest(unittest.TestCase):
    def test_rejects_duplicate_business_keys(self):
        rows = [("C001", "2020-01-01", "X"), ("C001", "2020-01-01", "Y")]
        with self.assertRaises(ValueError):
            far.distinct_rows(rows, 2, "far_customer")

    def test_keeps_unique_rows_in_order(self):
        rows = [("C001", "2020-01-01", "X"), ("C002", "2020-01-01", "Y")]
        self.assertEqual(far.distinct_rows(rows, 2, "far_customer"), rows)


if __name__ == "__main__":
    unittest.main()
