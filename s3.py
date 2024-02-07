# mypy: ignore-errors

import os
from pathlib import Path
from typing import Optional, Union, BinaryIO

import boto3
from s3transfer.manager import TransferManager
from botocore.config import Config
from s3transfer.subscribers import BaseSubscriber

from params import logger


class S3FilesLoader:

    def __init__(
            self,
            bucket: str,
            region_name: str,
            endpoint_url: Optional[str],
            aws_access_key_id: str,
            aws_secret_access_key: str,
            dry_run: bool = False
    ):
        super().__init__()
        self.s3_resource = boto3.resource(
            service_name="s3",
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            config=Config()
        )

        self.s3_client = self.s3_resource.meta.client

        self.s3_transfer_manager = TransferManager(client=self.s3_client)
        self.bucket_name = bucket
        self.bucket = self.s3_resource.Bucket(self.bucket_name)

        self.__future_queue = []

        self.dry_run = dry_run

    def is_not_dry_run(self) -> bool:
        return not self.dry_run

    def result_all_futures(self) -> int:
        n_futures = 0
        all_n_futures = len(self.__future_queue)

        while len(self.__future_queue) > 0:
            future = self.__future_queue.pop()
            future.result()
            n_futures += 1

            logger.info(f"Resulted future: ({n_futures}/{all_n_futures})")

        return n_futures

    def upload_file(self, from_filename: str, to_f_key: str, info: str = "") -> None:
        logger.info(f"{info}: Upload file to s3: {from_filename} -> {to_f_key}")

        if self.is_not_dry_run():
            self.s3_transfer_manager.upload(
                fileobj=from_filename,
                bucket=self.bucket_name,
                key=to_f_key,
                subscribers=None
            ).result()
        else:
            logger.info(f"{info}: DRY_RUN: skip")

    def add_to_upload_future(self, from_filename: str, to_f_key: str, info: str = "") -> None:
        logger.info(f"{info}: Add to upload file to s3 as future: {from_filename} -> {to_f_key}")

        if self.is_not_dry_run():
            self.__future_queue.append(
                self.s3_transfer_manager.upload(
                    fileobj=from_filename,
                    bucket=self.bucket_name,
                    key=to_f_key,
                    subscribers=None
                )
            )
        else:
            logger.info(f"{info}: DRY_RUN: skip")

    def upload_dir(self, from_dir: str, to_dir_key: str, info: str = "") -> None:
        logger.info(f"{info}: Add to upload dir to s3: {from_dir} -> {to_dir_key}")

        for path, _subdirs, files in os.walk(from_dir):
            for f_name in files:
                # data/F003024_RS000922_PBMC/25102022/LDT Plate Plus CLEAN/CLEAN_A1_A01-1666703655.fcs
                loca_path = (Path(path) / f_name).as_posix()

                # /25102022/LDT Plate Plus CLEAN/CLEAN_A1_A01-1666703655.fcs
                relative_loca_path = loca_path.removeprefix(from_dir)

                # RS000922/F003024/lab/flow-cytometry/BPV8/raw-data/F003024_RS000922_PBMC + /25102022/LDT Plate Plus CLEAN/CLEAN_A1_A01-1666703655.fcs  # noqa: E501
                to_f_key = to_dir_key + relative_loca_path

                self.add_to_upload_future(
                    from_filename=loca_path,
                    to_f_key=to_f_key
                )

        self.result_all_futures()

    def download_file(self, from_f_key: str, to_filename: Union[str, BinaryIO], info: str = "") -> None:
        if isinstance(to_filename, str):
            logger.info(f"{info}: Download file from s3: {from_f_key} -> {to_filename}")
        else:
            logger.info(f"{info}: Download file from s3: {from_f_key}")

        self.s3_transfer_manager.download(
            bucket=self.bucket_name,
            key=from_f_key,
            fileobj=to_filename,
            subscribers=None
        ).result()

    def add_to_download_future(
            self,
            from_f_key: str,
            to_filename: str,
            info: str = "",
            subscribers: list[BaseSubscriber] = None
    ) -> None:
        logger.info(f"{info}: Add to download file from s3 as future: {from_f_key} -> {to_filename}")

        self.__future_queue.append(
            self.s3_transfer_manager.download(
                bucket=self.bucket_name,
                key=from_f_key,
                fileobj=to_filename,
                subscribers=subscribers
            )
        )

    def download_dir(self, from_dir_key: str, to_dir: str = None, info: str = "") -> None:
        if to_dir is None:
            to_dir = os.path.basename(from_dir_key)

        logger.info(f"{info}: Add to download dir from s3: {from_dir_key} -> {to_dir}")

        objs = list(self.bucket.objects.filter(Prefix=from_dir_key))
        if objs:
            for obj in objs:
                to_filename = str(obj.key).replace(from_dir_key, to_dir)
                os.makedirs(os.path.dirname(to_filename), exist_ok=True)

                self.add_to_download_future(
                    from_f_key=obj.key,
                    to_filename=to_filename
                )

        self.result_all_futures()

    def move_file(self, from_f_key: str, to_f_key: str, to_bucket: str = None) -> None:
        if not to_bucket:
            to_bucket = self.bucket_name

        original_path = f"{self.bucket_name}/{from_f_key}"
        destination_path = f"{to_bucket}/{to_f_key}"

        if original_path == destination_path:
            logger.info(f"Cannot move files! from_f_key=to_f_key={to_f_key}")
            return

        logger.info(f"Move file: s3://{self.bucket_name}/{from_f_key} -> s3://{to_bucket}/{to_f_key}")
        if self.is_not_dry_run():
            response = self.s3_client.copy_object(
                Bucket=to_bucket,
                CopySource={'Bucket': self.bucket_name, 'Key': from_f_key},
                Key=to_f_key
            )

            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                self.s3_client.delete_object(
                    Bucket=self.bucket_name,
                    Key=from_f_key
                )
            else:
                raise Exception(f"Unable to move file {from_f_key=}.")
        else:
            logger.info("DRY_RUN: skip")

    def copy_file(self, from_f_key: str, to_f_key: str, to_bucket: str = None, info: str = "") -> None:
        if not to_bucket:
            to_bucket = self.bucket_name

        logger.info(f"{info}: Copy file: s3://{self.bucket_name}/{from_f_key} -> s3://{to_bucket}/{to_f_key}")

        if self.is_not_dry_run():
            self.s3_client.copy_object(
                Bucket=to_bucket,
                CopySource={'Bucket': self.bucket_name, 'Key': from_f_key},
                Key=to_f_key
            )
        else:
            logger.info(f"{info}: DRY_RUN: skip")

    def copy_dir(self, from_dir_key: str, to_dir_key: str, to_bucket: str = None, info: str = "") -> None:
        if not to_bucket:
            to_bucket = self.bucket_name

        logger.info(f"{info}: Copy dir: s3://{self.bucket_name}/{from_dir_key} -> s3://{to_bucket}/{to_dir_key}")

        objs = list(self.bucket.objects.filter(Prefix=from_dir_key))
        num_objs = len(objs)
        for i, obj in enumerate(objs, 1):
            new_key = obj.key.replace(from_dir_key, to_dir_key, 1)
            self.copy_file(from_f_key=obj.key, to_f_key=new_key, to_bucket=to_bucket, info=f"{info} ({i}/{num_objs})")

    def move_dir(
            self,
            from_dir_key: str,
            to_dir_key: str,
            to_bucket: str = None,
            max_obj_count: int = 100,
            info: str = ""
    ) -> None:
        if not to_bucket:
            to_bucket = self.bucket_name

        logger.info(f"{info}: Move dir: s3://{self.bucket_name}/{from_dir_key} -> s3://{to_bucket}/{to_dir_key}")

        objs = list(self.bucket.objects.filter(Prefix=from_dir_key))
        num_objs = len(objs)

        logger.info(f"{info}: Found {num_objs} files to move")
        if num_objs > max_obj_count:
            raise Exception(
                f"{info}: num_objs > max_obj_count: {num_objs} > {max_obj_count}. Too much objects to move. "
                f"You can increase max_obj_count"
            )

        for _i, obj in enumerate(objs, 1):
            new_key = obj.key.replace(from_dir_key, to_dir_key, 1)
            self.move_file(from_f_key=obj.key, to_f_key=new_key, to_bucket=to_bucket)

    def delete_dir(self, dir_key: str, max_obj_count: int = 30, info: str = "") -> None:
        logger.info(f"{info}: Delete dir on s3: {dir_key}. Max Count: {max_obj_count}")
        if dir_key == "/":
            raise Exception(f"{info}: Deleting by root key='/' is prohibited. Use explicit keys to directories")

        objs = list(self.bucket.objects.filter(Prefix=dir_key))
        num_objs = len(objs)

        logger.info(f"{info}: Found {num_objs} files to delete")
        if num_objs > max_obj_count:
            raise Exception(
                f"{info}: num_objs > max_obj_count: {num_objs} > {max_obj_count}. Too much objects to delete. "
                f"You can increase max_obj_count. !!! Be carefully - you can delete objects without recovery"
            )

        for i, obj in enumerate(objs, 1):
            self.delete_file(f_key=obj.key, info=f"{info} ({i}/{num_objs})")

    def delete_file(self, f_key: str, info: str = "") -> None:
        logger.info(f"{info} Delete file on s3: {f_key}")

        if self.is_not_dry_run():
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=f_key)
        else:
            logger.info(f"{info}: DRY_RUN: skip")

    def check_exists(self, f_key: str) -> bool:
        result = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=f_key, MaxKeys=1)
        contents = result.get('Contents')

        return contents is not None

    def list_objects(self, f_key: str) -> list[dict]:
        result = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=f_key)
        contents = result.get('Contents')

        if contents:
            return contents
        else:
            return []

    def list_f_keys(self, f_key: str) -> list[str]:
        objects = self.list_objects(f_key=f_key)

        return [obj['Key'] for obj in objects]

    def print_f_keys(self, f_key: str) -> None:
        f_keys = self.list_f_keys(f_key=f_key)

        for i, full_f_key in enumerate(f_keys, 1):
            print(f"{i}: {full_f_key}")

        print("")
