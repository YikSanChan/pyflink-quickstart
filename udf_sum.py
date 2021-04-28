from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.udf import udf

# https://flink.apache.org/2020/04/09/pyflink-udf-support-flink.html
# https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/python/table-api-users-guide/udfs/python_udfs.html

@udf(input_types=[DataTypes.INT(), DataTypes.INT()], result_type=DataTypes.BIGINT(), func_type="pandas")
def add(i, j):
  return i + j

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

t_env.create_temporary_function("add", add)

my_source_ddl = """
create table mySource (
    a INT,
    b INT
) with (
    'connector' = 'datagen',
    'rows-per-second' = '5'
)
"""

my_sink_ddl = """
create table mySink (
    c BIGINT
) with (
    'connector' = 'print'
)
"""

my_transform_dml = """
insert into mySink
select add(a, b)
from mySource
"""

t_env.execute_sql(my_source_ddl)
t_env.execute_sql(my_sink_ddl)
t_env.execute_sql(my_transform_dml)
