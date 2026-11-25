"""Single registry for ETL and ML jobs.

Adding a job means adding a module plus one entry here; the CLI and the
executor read every flag from the spec instead of special-casing job names.
"""

from dataclasses import dataclass
from typing import Callable, Optional, Tuple

from spark_project import tables
from spark_project.fund_ml import JOB_NAME as ML_JOB
from spark_project.fund_ml.job import run_fund_purchase
from spark_project.jobs import customer_fund, daily_fund_type


@dataclass(frozen=True)
class JobSpec:
    kind: str
    run_tables: Tuple[str, ...]
    build: Optional[Callable] = None
    lookup_input: Optional[Callable] = None
    allows_preagg: bool = False
    run: Optional[Callable] = None


JOBS = {
    "customer_fund": JobSpec(
        kind="etl",
        run_tables=(tables.CUSTOMER_FUND,),
        build=customer_fund.build,
    ),
    "daily_fund_type": JobSpec(
        kind="etl",
        run_tables=(tables.DAILY_FUND_TYPE,),
        build=daily_fund_type.build,
        lookup_input=daily_fund_type.lookup_input,
        allows_preagg=True,
    ),
    ML_JOB: JobSpec(
        kind="ml",
        run_tables=(tables.ML_PREDICTION, tables.ML_METRIC, tables.ML_RUN),
        run=run_fund_purchase,
    ),
}
