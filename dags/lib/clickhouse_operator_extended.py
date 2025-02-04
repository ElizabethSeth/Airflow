import typing as t
from airflow_clickhouse_plugin.operators.clickhouse import BaseClickHouseOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ExecuteReturnT
class ClickHouseOperatorExtended(BaseClickHouseOperator):
    def execute(self, context: t.Dict[str, t.Any]) -> ExecuteReturnT:
        self._sql = [s.strip() for s in self._sql.split(';') if s.strip()]
        return self._hook_execute()