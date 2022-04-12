import datetime
import logging
import pathlib
import time
from typing import Any, Dict

import prefect
from prefect import Flow
from prefect.executors import Executor, LocalDaskExecutor, LocalExecutor

from flows import covid19_vaccination_rate_etl_flow


def trigger_flow_run(flow: Flow, executor: Executor, run_parameters: Dict[str, Any]):
    print(
        f"Starting flow run for '{flow.name}' with {executor.__class__.__name__} executor."
    )
    run_start_time = time.monotonic()
    flow.executor = executor
    flow.run(parameters=run_parameters)
    time_elapsed = round(time.monotonic() - run_start_time, 2)
    print(f"Run finished and took {datetime.timedelta(seconds=time_elapsed)}")


if __name__ == "__main__":
    # mute default Prefect's logger on purpose to avoid cluttering standard output
    logger = prefect.context.get("logger")
    logger.setLevel(logging.CRITICAL)

    # define target data directory
    data_dir = pathlib.Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)

    parameters = {"data_dir": data_dir}

    # trigger local Dask executor (multiple threads)
    trigger_flow_run(
        flow=covid19_vaccination_rate_etl_flow,
        executor=LocalDaskExecutor(),
        run_parameters=parameters,
    )

    # trigger local executor (single thread)
    trigger_flow_run(
        flow=covid19_vaccination_rate_etl_flow,
        executor=LocalExecutor(),
        run_parameters=parameters,
    )
