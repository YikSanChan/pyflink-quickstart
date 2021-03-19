from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.udf import udf

# https://flink.apache.org/2020/04/09/pyflink-udf-support-flink.html
# https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/python/table-api-users-guide/udfs/python_udfs.html

@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
def parse(s):
  import json
  # a dummy parser
  res = {}
  content = json.loads(s)
  if 'item_id' in content:
      res['item_id'] = str(content['item_id']) # REMEMBER to match the result_type
  if 'tag' in content:
      res['tag'] = content['tag']
  return res

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

t_env.register_function("parse", parse)

my_source_ddl = """
create table mySource (
    id BIGINT,
    contentstr STRING
) with (
    'connector' = 'filesystem',
    'format' = 'json',
    'path' = '/tmp/input'
)
"""

my_sink_ddl = """
create table mySink (
    id BIGINT
) with (
    'connector' = 'print'
)
"""

my_transform_dml = """
insert into mySink
with t1 as (
    select id, parse(contentstr) as content
    from mySource
)
select id
from t1
where content['item_id'] is not null
and content['tag'] = 'a'
"""

t_env.execute_sql(my_source_ddl)
t_env.execute_sql(my_sink_ddl)
t_env.execute_sql(my_transform_dml).wait()

# /tmp/input
# {"id":1,"contentstr":"{\"item_id\":123,\"tag\":\"a\"}"}
# {"id":2,"contentstr":"{\"item_id\":123,\"tag\":\"b\"}"}
