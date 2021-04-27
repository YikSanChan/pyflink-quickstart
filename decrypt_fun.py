from pyflink.table.udf import udf, ScalarFunction
from pyflink.table import DataTypes

class Decrypt(ScalarFunction):
    def open(self, function_context):
        import pandas as pd
        self.d = pd.read_csv('resources.zip/resources/crypt.csv', header=None, index_col=0, squeeze=True).to_dict()

    def eval(self, s):
        return self.d.get(s, "unknown")

decrypt = udf(Decrypt(), input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
