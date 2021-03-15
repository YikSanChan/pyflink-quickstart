from pyflink.table import EnvironmentSettings, StreamTableEnvironment

# https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/connectors/datagen.html

env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
table_env = StreamTableEnvironment.create(environment_settings=env_settings)

orders_source_ddl = """
CREATE TABLE Orders (
    order_number BIGINT,
    price        DECIMAL(32,2),
    buyer        ROW<first_name STRING, last_name STRING>,
    order_time   TIMESTAMP(3)
) WITH (
  'connector' = 'datagen',
  'number-of-rows' = '10'
)
"""

printorders_sink_ddl = """
CREATE TABLE PrintOrders WITH ('connector' = 'print')
LIKE Orders (EXCLUDING ALL)
"""

table_env.execute_sql(orders_source_ddl)
table_env.execute_sql(printorders_sink_ddl)

table_env.from_path("Orders").insert_into("PrintOrders")

table_env.execute("Datagen Mock")
