import datetime
import pathlib
import time

import prefect

from flows import covid19_vaccination_rate_etl_flow

if __name__ == "__main__":
    logger = prefect.context.get("logger")

    # define target data directory
    data_dir = pathlib.Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)

    # run flow
    flow_start_time = time.monotonic()
    covid19_vaccination_rate_etl_flow.run(
        parameters={
            "data_dir": data_dir,
        }
    )
    time_elapsed = round(time.monotonic() - flow_start_time, 2)

    logger.info(
        f"{covid19_vaccination_rate_etl_flow.name} finished. Time of execution: "
        f"{datetime.timedelta(seconds=time_elapsed)}"
    )
