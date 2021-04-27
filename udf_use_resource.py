from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.udf import udf

@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
def decrypt(s):
    import pandas as pd
    d = pd.read_csv('resources/crypt.csv', header=None, index_col=0, squeeze=True).to_dict()
    return d.get(s, "unknown")

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

t_env.create_temporary_function("decrypt", decrypt)

my_source_ddl = """
create table mySource (
    a STRING
) with (
    'connector' = 'filesystem',
    'format' = 'csv',
    'path' = '/tmp/input'
)
"""

my_sink_ddl = """
create table mySink (
    a STRING
) with (
    'connector' = 'print'
)
"""

my_transform_dml = """
insert into mySink
select DECRYPT(a)
from mySource
"""

t_env.execute_sql(my_source_ddl)
t_env.execute_sql(my_sink_ddl)
t_env.execute_sql(my_transform_dml)
