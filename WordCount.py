from pyflink.table import EnvironmentSettings, BatchTableEnvironment

# https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/python/table_api_tutorial.html

env_settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
table_env = BatchTableEnvironment.create(environment_settings=env_settings)
table_env.get_config().get_configuration().set_string("parallelism.default", "1")

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

transform_dml = """
INSERT INTO mySink
SELECT word, COUNT(1) FROM mySource GROUP BY word
"""

table_env.execute_sql(my_source_ddl)
table_env.execute_sql(my_sink_ddl)
table_env.execute_sql(transform_dml).wait()

# before run: echo -e  "flink\npyflink\nflink" > /tmp/input
# after run: cat /tmp/output

