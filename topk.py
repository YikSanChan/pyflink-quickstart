from pyflink.table import BatchTableEnvironment, EnvironmentSettings

env_settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
t_env = BatchTableEnvironment.create(environment_settings=env_settings)

source_ddl = """
create table mySource (
    user_id bigint,
    item_id bigint,
    score bigint
) with (
    'connector' = 'filesystem',
    'format' = 'csv',
    'path' = 'resources/user_behavior.csv'
)
"""

sink_ddl = """
create table mySink (
    user_id bigint,
    last_3_views string
) with (
    'connector' = 'print'
)
"""

transform_dml = """
insert into mySink
with top3 as (
    SELECT user_id, item_id
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY score DESC) as row_num
        FROM mySource
    )
    WHERE row_num <= 3
)
select user_id, LISTAGG(CAST(item_id AS string)) last_3_views from top3
group by user_id
"""

t_env.execute_sql(source_ddl)
t_env.execute_sql(sink_ddl)
t_env.execute_sql(transform_dml).wait()
