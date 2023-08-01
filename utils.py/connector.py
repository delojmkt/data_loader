import settings

import pymysql
import pymssql

__all__ = ["DBClient"]


class DBClient(object):
    db_conn = settings.db_config

    def get_conn(db_type: str):
        if db_type == "mysql":
            return pymysql.connect(**DBClient.db_conn)
        elif db_type == "mssql":
            return pymssql.connect(**DBClient.db_conn)
        else:
            raise ValueError("DB type must be the one of mysql and mssql")
