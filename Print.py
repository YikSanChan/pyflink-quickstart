from pyflink.table import EnvironmentSettings, StreamTableEnvironment

# https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/python/table-api-users-guide/intro_to_table_api.html

env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
table_env = StreamTableEnvironment.create(environment_settings=env_settings)

table_env.execute_sql("""
    CREATE TABLE datagen (
        id INT,
        data STRING
    ) WITH (
        'connector' = 'datagen',
        'rows-per-second' = '1'
    )
""")

table_env.execute_sql("""
    CREATE TABLE print (
        id INT,
        data STRING
    ) WITH (
        'connector' = 'print'
    )
""")

source_table = table_env.from_path("datagen") # same as table_env.sql_query("SELECT * FROM datagen")

result_table = source_table.select(source_table.id, source_table.data)

# Uncomment the below command to print a pandas DataFrame
# print(result_table.to_pandas())

result_table.execute_insert("print").wait() # same as table_env.execute_sql("INSERT INTO print SELECT * FROM datagen").wait()
