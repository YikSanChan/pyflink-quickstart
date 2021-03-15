from pyflink.table import BatchTableEnvironment, EnvironmentSettings
from pyflink.table.descriptors import Schema, OldCsv, FileSystem
from pyflink.table.expressions import lit

# https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/python/table_api_tutorial.html

table_env = BatchTableEnvironment.create(environment_settings=EnvironmentSettings.new_instance()
    .in_batch_mode().use_blink_planner().build())
table_env._j_tenv.getPlanner().getExecEnv().setParallelism(1)

my_source_ddl = """
    create table mySource (
        word VARCHAR
    ) with (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = '/tmp/input'
    )
"""

my_sink_ddl = """
    create table mySink (
        word VARCHAR,
        `count` BIGINT
    ) with (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = '/tmp/output'
    )
"""

table_env.sql_update(my_source_ddl)
table_env.sql_update(my_sink_ddl)

tab = table_env.from_path('mySource')
tab.group_by(tab.word) \
   .select(tab.word, lit(1).count) \
   .execute_insert('mySink').wait()
