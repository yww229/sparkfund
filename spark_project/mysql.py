"""MysqlStore: one place for JDBC reads, writes and run registration."""

from pyspark.sql import functions as F


class MysqlStore:
    def __init__(self, spark, url, properties):
        self.spark = spark
        self.url = url
        self.properties = properties

    def read(self, table, columns=None, where=None):
        """Read a table, optionally pruning columns and pushing a WHERE clause.

        Spark 2.4 supports a subquery in ``dbtable``; using it keeps the date
        window and the column list in MySQL instead of loading the full table.
        """
        if where:
            selected = ", ".join(columns) if columns else "*"
            dbtable = "(SELECT {0} FROM {1} WHERE {2}) AS source".format(selected, table, where)
            return self.spark.read.jdbc(self.url, dbtable, properties=self.properties)
        frame = self.spark.read.jdbc(self.url, table, properties=self.properties)
        return frame.select(*columns) if columns else frame

    def for_run(self, table, run_id):
        return self.read(table).where(F.col("run_id") == run_id)

    def require_unused_run(self, tables, run_id):
        """Reject a run_id that already exists in any related table."""
        literal = "'" + run_id.replace("'", "''") + "'"
        probes = [
            "(SELECT '{0}' AS table_name FROM {0} WHERE run_id = {1} LIMIT 1)".format(table, literal)
            for table in tables
        ]
        dbtable = "(" + " UNION ALL ".join(probes) + ") AS existing"
        hit = self.spark.read.jdbc(self.url, dbtable, properties=self.properties).take(1)
        if hit:
            raise RuntimeError(
                "Run ID already exists in " + hit[0].table_name + ". Submit with a fresh ID."
            )

    def write(self, table, frame, run_id, partitions=1, batchsize=1000, truncate=False):
        if partitions < 1:
            raise ValueError("partitions must be at least 1.")
        published = frame.withColumn("run_id", F.lit(run_id))
        writer = (
            published.coalesce(partitions)
            .write.option("batchsize", str(batchsize))
            .option("isolationLevel", "READ_COMMITTED")
        )
        if truncate:
            writer = writer.option("truncate", "true")
        writer.jdbc(self.url, table, mode="append", properties=self.properties)

    def count(self, table, run_id):
        return self.for_run(table, run_id).count()

    def append_and_count(self, table, frame, run_id, **options):
        self.write(table, frame, run_id, **options)
        return self.count(table, run_id)

    def register_run(self, table, run_id, frame):
        if self.append_and_count(table, frame, run_id) != 1:
            raise RuntimeError("The run manifest could not be read back.")
