import time
import pandas as pd
import pymssql
import pymysql
from sqlalchemy import create_engine

from utils import DBClient
import settings

# Create Engine의 경우 추후 다시 코드 작성 예정
# class Create_table:
#     db_config = settings.db_config

#     def uploading(df, table_name):
#         path = f"mysql+pymysql://{Create_table.db_config["user"]}:{Create_table.db_config["password"]}@{Create_table.db_config["host"]}:{Create_table.db_config["port"]}/{Create_table.db_config["DB"]}"
#         engine = create_engine(path)
#         df.to_sql(index=False, name=table_name, con=engine, if_exists="replace")
#         print("UPLOADING DONE")

__all__ = ["uploading"]


class Upload:
    def __init__(self, db_type):
        self.db_type = db_type
        self.conn = None

    def _connect_conn(self):
        self.conn = DBClient.get_conn(self.db_type)

    def _close_conn(self):
        self.conn.close()

    def _make_query(self, df, DB: str, table_name: str):
        insert_query = f"insert into {DB}.{table_name} ("
        for col in df.columns:
            insert_query += col + ", "
        insert_query = insert_query[:-1] + ") values"

        element_query = "("
        for _ in range(df.shape[1]):
            element_query += "%s,"
        element_query = element_query[:-1] + "),"

        return insert_query, element_query

    def _uploading(self, df, table_name, DB):
        cur = self.conn.cursor()

        insert_query, element_query = self._make_query(df, DB, table_name)

        data = []

        for idx, i in enumerate(df.values):
            query = insert_query + element_query
            for j in i:
                data.append(j)
            if idx % 100 == 0:
                query = query.rstrip(",")
                cur.execute(query, tuple(data))
                self.conn.commit()
                time.sleep(0.3)
                query = insert_query
                data = []
        query_ = query.rstrip(",")
        cur.execute(query_, tuple(data))
        self.conn.commit()

    def upload_with_conn(self, df, table_name, DB, delete: bool = True):
        self._connect_conn()
        if delete:
            self._delete(table_name)
        self._uploading(df, table_name, DB)
        self._close_conn()


def uploading(df, table_name, DB, delete: bool = True):
    return Upload().upload_with_conn(df=df, table_name=table_name, DB=DB, delete=delete)
