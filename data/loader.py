import os
import io
import pickle

from data.query import get_query
from utils import DBClient, S3Client, AthenaClient

import pandas as pd

__all__ = ["loading"]


def load_onpremise(db_type: str, **kwargs) -> pd.DataFrame:
    conn = DBClient.get_conn(db_type)
    return pd.read_sql(get_query(**kwargs), conn)


def load_s3(file_key: str):
    data = S3Client.get_object(file_key)
    data = io.BytesIO(data.read())

    data_type = file_key.split(".")[-1]

    if data_type == "parquet":
        return pd.read_parquet(data, engine="pyarrow")
    elif data_type == "csv":
        return pd.read_csv(data)
    elif data_type == "pickle":
        return pickle.load(data)
    else:
        raise TypeError("data type must be the one of csv, parquet and pickle")


def load_athena(**kwargs):
    return AthenaClient.get_query(get_query(**kwargs))


def loading(atype: str, **kwargs):
    if atype == "csv":
        return pd.read_csv(kwargs["file_path"])
    elif atype in ("mysql", "mssql"):
        return load_onpremise(atype)
    elif atype == "s3":
        return load_s3(kwargs["file_path"])
    elif atype == "athena":
        return load_athena(**kwargs)
    else:
        raise TypeError("loading type must be the one of csv, mysql, mssql, s3 and athena")
