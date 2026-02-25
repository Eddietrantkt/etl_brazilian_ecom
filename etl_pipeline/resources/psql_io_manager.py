from contextlib import contextmanager
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from sqlalchemy import create_engine

@contextmanager
def connect_psql(config):
    conn_info = (
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        +f"@{config['host']}:{config['port']}"
        +f"/{config['database']}"
    )

    db_conn = create_engine(conn_info)
    try:
        yield db_conn
    except Exception:
        raise


class PostgresqlIOManager(IOManager):
    def __init__(self, config):
        self.config = config

    def load_input(self, context: InputContext) -> pd.DataFrame:
        schema, table = context.asset_key.path[-2], context.asset_key.path[-1]
        try:
            with connect_psql(self.config) as db_conn:
                pd_data = pd.read_sql(f"SELECT * FROM {schema}.{table}", db_conn)
                return pd_data
        except Exception:
            raise

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        schema, table = context.asset_key.path[-2], context.asset_key.path[-1]
        try:
            with connect_psql(self.config) as conn:
                obj.to_sql(table, conn, schema=schema, if_exists="replace", index=False)
        except Exception:
            raise
