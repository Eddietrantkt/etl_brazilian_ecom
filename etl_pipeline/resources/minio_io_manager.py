import os
import tempfile
from contextlib import contextmanager
from datetime import datetime
from typing import Union
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from minio import Minio

@contextmanager
def connect_minio(config):
    client = Minio(
        endpoint=config.get("endpoint_url"),
        access_key=config.get("aws_access_key_id"),
        secret_key=config.get("aws_secret_access_key"),
        secure=False,
    )
    try:
        yield client
    except Exception:
        raise

class MinioIOManager(IOManager):
    def __init__(self, config):
        self.config = config
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        """Tự động tạo bucket nếu chưa tồn tại."""
        with connect_minio(self.config) as minio_client:
            bucket_name = self.config.get("bucket_name")
            if not minio_client.bucket_exists(bucket_name):
                minio_client.make_bucket(bucket_name)

    def _get_path(self, context: Union[OutputContext, InputContext]):
        layer, schema, table = context.asset_key.path
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        tmp_file_path = os.path.join(
            tempfile.gettempdir(),
            "file-{}-{}.parquet".format(
                datetime.today().strftime("%Y%m%d%H%M%S"),
                "_".join(context.asset_key.path),
            ),
        )
        return f"{key}.pq", tmp_file_path

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        key_name, tmp_file_name = self._get_path(context)
        try:
            obj.to_parquet(tmp_file_name)
            with connect_minio(self.config) as minio_client:
                minio_client.fput_object(
                    bucket_name=self.config.get("bucket_name"),
                    object_name=key_name,
                    file_path=tmp_file_name,
                )
            os.remove(tmp_file_name)
        except Exception:
            raise

    def load_input(self, context: InputContext) -> pd.DataFrame:
        key_name, tmp_file_name = self._get_path(context)
        try:
            with connect_minio(self.config) as minio_client:
                minio_client.fget_object(
                    bucket_name=self.config.get("bucket_name"),
                    object_name=key_name,
                    file_path=tmp_file_name,
                )
            pd_data = pd.read_parquet(tmp_file_name)
            os.remove(tmp_file_name)
            return pd_data
        except Exception:
            raise