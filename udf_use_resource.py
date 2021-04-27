from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from decrypt_fun import decrypt

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
