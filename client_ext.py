import json
from typing import List, Optional, Union, Dict

import cellengine
import pandas
from cellengine import Compensations, UNCOMPENSATED
from pandas import DataFrame


def get_statistics(
        client: cellengine.APIClient,
        experiment_id: str,
        statistics: List[str],
        channels: List[str],
        q: Optional[float] = None,
        annotations: bool = False,
        compensation_id: Union[Compensations, str] = UNCOMPENSATED,
        fcs_file_ids: Optional[List[str]] = None,
        format: str = "json",
        layout: Optional[str] = None,
        percent_of: Optional[Union[str, List[str]]] = "PARENT",
        population_ids: Optional[List[str]] = None,
        other_params: dict = {}
) -> Union[Dict, str, DataFrame]:
    """
    Request Statistics from CellEngine.

    Args:
        experiment_id: ID of the experiment.
        statistics: Statistics to calculate. Any of "mean", "median",
            "quantile", "mad" (median absolute deviation), "geometricmean",
            "eventcount", "cv", "stddev" or "percent" (case-insensitive).
        q (int): quantile (required for "quantile" statistic)
        channels (List[str]): for "mean", "median", "geometricMean",
            "cv", "stddev", "mad" or "quantile" statistics. Names of channels
            to calculate statistics for.
        annotations: Include file annotations in output
            (defaults to False).
        compensation_id: Compensation to use for gating and statistics
            calculation. Defaults to uncompensated. In addition to a
            compensation ID, three special constants may be used:
                [`UNCOMPENSATED`][cellengine.UNCOMPENSATED],
                [`FILE_INTERNAL`][cellengine.FILE_INTERNAL] or
                [`PER_FILE`][cellengine.PER_FILE].
        fcs_file_ids: FCS files to get statistics for. If
            omitted, statistics for all non-control FCS files will be returned.
        format: str: One of "TSV (with[out] header)",
            "CSV (with[out] header)" or "json" (default), "pandas",
            case-insensitive.
        layout: str: The file (TSV/CSV) or object (JSON) layout.
            One of "tall-skinny", "medium", or "short-wide".
        percent_of: str or List[str]: Population ID or array of
            population IDs.  If omitted or the string "PARENT", will calculate
            percent of parent for each population. If a single ID, will calculate
            percent of that population for all populations specified by
            population_ids. If a list, will calculate percent of each of
            those populations.
        population_ids: List[str]: List of population IDs.
            Defaults to ungated.

    Returns:
        statistics: Dict, String, or pandas.Dataframe
    """

    if "quantile" == statistics and not isinstance(q, float):
        raise ValueError("'q' must be a number for 'quantile' statistic.")

    params = {
        "statistics": statistics,
        "q": q,
        "channels": channels,
        "annotations": annotations,
        "compensationId": compensation_id,
        "fcsFileIds": fcs_file_ids,
        "format": "json" if format == "pandas" else format,
        "layout": layout,
        "percentOf": percent_of,
        "populationIds": population_ids
    }
    params.update(other_params)

    req_params = {key: val for key, val in params.items() if val is not None}

    raw_stats = client._post(
        f"{client.base_url}/api/v1/experiments/{experiment_id}/bulkstatistics",
        json=req_params,
        raw=True,
    )

    format = format.lower()
    if format == "json":
        return json.loads(raw_stats)
    elif "sv" in format:
        try:
            return raw_stats.decode()
        except Exception as e:
            raise ValueError("Invalid output format {}".format(format), e)
    elif format == "pandas":
        try:
            return pandas.DataFrame.from_dict(json.loads(raw_stats))
        except Exception as e:
            raise ValueError("Invalid data format {} for pandas".format(format), e)
    else:
        raise ValueError("Invalid data format selected.")