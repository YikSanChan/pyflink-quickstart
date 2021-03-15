from pyflink.table import EnvironmentSettings, StreamTableEnvironment

# https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/python/table-api-users-guide/intro_to_table_api.html

# 1. create a TableEnvironment
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
table_env = StreamTableEnvironment.create(environment_settings=env_settings)

# 2. create source Table
table_env.execute_sql("""
    CREATE TABLE datagen (
        id INT,
        data STRING
    ) WITH (
        'connector' = 'datagen',
        'fields.id.kind' = 'sequence',
        'fields.id.start' = '1',
        'fields.id.end' = '10'
    )
""")

# 3. create sink Table
table_env.execute_sql("""
    CREATE TABLE print (
        id INT,
        data STRING
    ) WITH (
        'connector' = 'print'
    )
""")

# 4. query from source table and perform caculations
source_table = table_env.from_path("datagen") # same as table_env.sql_query("SELECT * FROM datagen")

result_table = source_table.select(source_table.id + 1, source_table.data)

# Uncomment the below command to print a pandas DataFrame
# print(result_table.to_pandas())

# 5. emit query result to sink table
result_table.execute_insert("print").wait() # same as table_env.execute_sql("INSERT INTO print SELECT * FROM datagen").wait()
