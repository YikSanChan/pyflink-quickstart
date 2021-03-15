from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# https://ci.apache.org/projects/flink/flink-docs-stable/dev/python/table-api-users-guide/python_table_api_connectors.html

FAT_JAR_PATH = "/Users/chenyisheng/source/yiksanchan/pyflink-quickstart/flink-sql-connector-kafka_2.12-1.12.0.jar"

def log_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    env_settings = EnvironmentSettings.Builder().use_blink_planner().build()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env, environment_settings=env_settings)
    # specify connector and format jars
    t_env.get_config().get_configuration().set_string("pipeline.jars", "file://" + FAT_JAR_PATH)
    
    source_ddl = """
            CREATE TABLE source_table(
                a VARCHAR,
                b INT
            ) WITH (
              'connector' = 'kafka',
              'topic' = 'source_topic',
              'properties.bootstrap.servers' = 'localhost:9092',
              'properties.group.id' = 'test_group',
              'scan.startup.mode' = 'earliest-offset',
              'format' = 'json'
            )
            """

    sink_ddl = """
            CREATE TABLE sink_table(
                a VARCHAR
            ) WITH (
              'connector' = 'kafka',
              'topic' = 'sink_topic',
              'properties.bootstrap.servers' = 'localhost:9092',
              'format' = 'json'
            )
            """

    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)

    t_env.from_path("source_table").select("a").execute_insert("sink_table").wait()


if __name__ == '__main__':
    log_processing()
