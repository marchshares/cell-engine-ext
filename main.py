from typing import Optional

import cellengine
import os
import pandas as pd

from cellengine import Experiment, FcsFile

import utils
from client_ext import get_statistics
from params import logger
from s3 import S3FilesLoader


client = cellengine.APIClient()


s3_files_loader = S3FilesLoader(
    bucket="rnd-immune-profiling",
    region_name="us-east-1",
    endpoint_url=None,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
)

def _check_s3_exists(f_path: str) -> bool:
    exists = s3_files_loader.check_exists(
        f_key=f"CellEngine/{f_path}"
    )

    if exists:
        logger.info(f"File exists on s3: {f_path}")
        return True

    return False

def _upload_to_s3(f_path: Optional[str], remove_local: bool = False):
    if f_path is None:
        return

    s3_files_loader.upload_file(
        from_filename=f_path,
        to_f_key=f"CellEngine/{f_path}"
    )

    if remove_local:
        logger.info(f"Remove: {f_path}")
        os.remove(f_path)


def _download_file(exp: Experiment, exp_root: str, fcs_file: FcsFile) -> Optional[str]:
    logger.info(f"download_file")

    fcs_file_path = f"{exp_root}/{fcs_file.name}"
    if _check_s3_exists(f_path=fcs_file_path):
        return None

    binary_file = client.download_fcs_file(exp.id, fcs_file.id)

    with open(fcs_file_path, "wb") as f:
        f.write(binary_file)

    return fcs_file_path


def _download_global_gating_ml(exp: Experiment, exp_root: str) -> str:
    logger.info(f"download_global_gating_ml")

    response = client._get(url=f"{client.base_url}/api/v1/experiments/{exp.id}.gatingml", raw=True)

    gatingml_file_path = f"{exp_root}/{exp.name}_global.gatingml"
    with open(gatingml_file_path, "wb") as f:
        f.write(response)

    return gatingml_file_path


def _download_fcs_gating_ml(exp: Experiment, exp_root: str, fcs_file: FcsFile) -> Optional[str]:
    logger.info(f"download_fcs_gating_ml")

    fcs_name = fcs_file.name.removesuffix(".fcs")
    gatingml_file_path = f"{exp_root}/{fcs_name}.gatingml"
    if _check_s3_exists(f_path=gatingml_file_path):
        return None

    response = client._get(url=f"{client.base_url}/api/v1/experiments/{exp.id}.gatingml?fcsFileId={fcs_file.id}", raw=True)


    with open(gatingml_file_path, "wb") as f:
        f.write(response)

    return gatingml_file_path


def _download_statistics(exp: Experiment, exp_root: str, fcs_file: FcsFile) -> str:
    logger.info(f"download_statistics")

    fcs_name = fcs_file.name.removesuffix(".fcs")
    statistics_file_path = f"{exp_root}/{fcs_name}_statistics.tsv"
    if _check_s3_exists(f_path=statistics_file_path):
        return None

    all_populations = [""]
    all_populations.extend([pop.id for pop in exp.populations])

    statistics_tsv = get_statistics(
        client=client,
        experiment_id=exp.id,
        statistics=["eventcount"],
        channels=[],
        fcs_file_ids=[fcs_file.id],
        format="TSV (with header)",
        layout="medium",
        population_ids=all_populations,
        other_params={
            "ids": True,
            "uniqueNames": True,
            "fullPaths": True
        }
    )

    with open(statistics_file_path, "w", encoding="utf-8", newline='\n') as f:
        f.write(statistics_tsv)

    return statistics_file_path


def _download_experiment_files(exp: Experiment):
    exp_root = f"data/{exp.name}"
    os.makedirs(exp_root, exist_ok=True)

    global_gating_ml_f_path = _download_global_gating_ml(exp, exp_root)

    _upload_to_s3(f_path=global_gating_ml_f_path)

    n = len(exp.fcs_files)
    for idx, fcs_file in enumerate(exp.fcs_files, 1):
        with logger.contextualize(fcs=f"({idx}/{n}) {fcs_file.name} "):
            fcs_f_path = _download_file(exp, exp_root, fcs_file)
            _upload_to_s3(f_path=fcs_f_path, remove_local=True)

            fcs_gating_ml_f_path = _download_fcs_gating_ml(exp, exp_root, fcs_file)
            # utils.compare_files(file1_path=global_gating_ml_f_path, file2_path=fcs_gating_ml_f_path)

            _upload_to_s3(f_path=fcs_gating_ml_f_path, remove_local=True)

            statistics_f_path = _download_statistics(exp, exp_root, fcs_file)
            _upload_to_s3(f_path=statistics_f_path, remove_local=True)




def _extract_experiment_annotations(exp: Experiment) -> pd.DataFrame:
    logger.info(f"extract_experiment_annotations")

    statistics_with_annotations: dict = get_statistics(
        client=client,
        experiment_id=exp.id,
        statistics=[],
        channels=[],
        format="json",
        annotations=True
    )

    exp_annotations = []
    for statistic in statistics_with_annotations:
        fcs_annotations = {
            "experiment": exp.name,
            "filename": statistic["filename"]
        }
        fcs_annotations.update(statistic["annotations"])

        exp_annotations.append(fcs_annotations)

    exp_df = pd.DataFrame(data=exp_annotations)

    return exp_df


def process_experiments(experiments):
    experiment_names = [exp.name for exp in experiments]
    logger.info(f"Process experiments: {experiment_names}")

    annotations_df = pd.DataFrame()

    n = len(experiments)
    for idx, exp in enumerate(experiments, 1):
        with logger.contextualize(exp=f"({idx}/{n}) {exp.name} "):
            _download_experiment_files(exp)

            experiment_annotations_df = _extract_experiment_annotations(exp=exp)
            annotations_df = pd.concat([annotations_df, experiment_annotations_df])

    annotations_f_path = "data/Annotations.xlsx"
    annotations_df.to_excel(annotations_f_path, index=False)
    _upload_to_s3(f_path=annotations_f_path, remove_local=True)


def process_experiments_by_names(experiment_names: list[str]):
    experiments = []
    for exp_name in experiment_names:
        exp = client.get_experiment(name=exp_name)
        experiments.append(exp)

    process_experiments(experiments=experiments)


def main():
    experiments = [
        'PICI0001-MAHLER',
        'Phase2 data subset of the original 2902 (Phase1) dataset',
        'PICI_0002_5_Penn - PFG',
        'PICI0002-X50-Complete',
        '2902 PICI-0002 Ship_4108 (Spitzer Hierarchy) - pfg',
        '2902 PICI-0002 Ship_6216 (Spitzer Hierarchy) - pfg',
        '2902 PICI-0002 Ship_6410 (finalized Hierarchy) - pfg',
        '2902 PICI-0002 Ship_5687 (Spitzer Hierarchy) - pfg',
        '2902 Clinical Samples-Spitzer-complete (original)',
        '2902 (PICI-0002) Re-run Ship ID 6216  - pfg',
        'PICI_0002_6_Penn - pfg',
        'PICI_0002_Penn - pfg',
        'PICI_0002_2_Penn - pfg',
        'PICI_0002_3_Penn - pfg',
        'PICI_0002_4_Penn - pfg',
        '2902 PICI-0002 Ship_6410 (finalized Hierarchy) - pfg (DIANE COPY)',
        '2902 PICI-0002 Ship_5687 (Spitzer Hierarchy) - Diane copy',
        '2902 PICI-0002 Ship_4108 (Spitzer Hierarchy) - Diane copy',
        '2902 PICI-0002 Ship_6216 (Spitzer Hierarchy) - Diane copy'
    ]

    process_experiments_by_names(experiments)


main()
