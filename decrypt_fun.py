from pyflink.table.udf import udf, ScalarFunction
from pyflink.table import DataTypes

@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
def decrypt(s):
    import pandas as pd
    d = pd.read_csv('resources.zip/resources/crypt.csv', header=None, index_col=0, squeeze=True).to_dict()
    return d.get(s, "unknown")
