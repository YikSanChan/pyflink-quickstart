from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

source_ddl = """
create table mySource (
    user_id bigint,
    item_id bigint,
    event_time bigint
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
        SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_time DESC) as row_num
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
