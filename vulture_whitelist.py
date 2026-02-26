# vulture whitelist - these are used implicitly by DuckDB SQL (FROM df)
from src.db.db import DuckDBConnector

DuckDBConnector.log_stop_orders.df
DuckDBConnector.log_executions.df
DuckDBConnector.log_account_info.df
DuckDBConnector.log_account_snapshots.df
