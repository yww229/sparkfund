"""Submit one registered demo job to Spark."""

import argparse

from spark_project.registry import JOBS
from spark_project.settings import DEFAULT_FROM, DEFAULT_TO, jdbc_settings, validate_args


def main():
    parser = argparse.ArgumentParser(description="Submit one Spark fund demo job.")
    parser.add_argument("--job", required=True, choices=sorted(JOBS))
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--date-from", default=DEFAULT_FROM)
    parser.add_argument("--date-to", default=DEFAULT_TO)
    parser.add_argument("--strategy", default="direct", choices=("direct", "preagg"))
    parser.add_argument("--code-version", required=True)
    parser.add_argument("--log-path", default="")
    parser.add_argument("--plan", action="store_true")
    args = parser.parse_args()

    spec = JOBS[args.job]
    try:
        validate_args(args, spec)
        url, properties = jdbc_settings()
    except ValueError as error:
        parser.error(str(error))

    from pyspark.sql import SparkSession

    from spark_project.mysql import MysqlStore

    spark = (
        SparkSession.builder.appName("fund-demo:" + args.job)
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("RUN_ID={0}".format(args.run_id), flush=True)
    print("JOB={0}".format(args.job), flush=True)
    print("CODE_VERSION={0}".format(args.code_version), flush=True)
    print("SPARK_APP_ID={0}".format(spark.sparkContext.applicationId), flush=True)

    try:
        store = MysqlStore(spark, url, properties)
        if spec.kind == "ml":
            stored = spec.run(spark, store, args)
        else:
            from spark_project.runner import run_job

            stored = run_job(spark, store, args, spec)
        print("RESULT_ROWS={0}".format(stored), flush=True)
        print("WRITE_AND_READBACK=PASS", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
