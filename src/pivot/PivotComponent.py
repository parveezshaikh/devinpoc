from src.etl.Component import ComponentInfo
from src.etl.SparkAbstract import SparkAbstract


class PivotComponent(ComponentInfo):
    def __init__(self, component, spark):
        self.fileobj = component
        self.spark = spark

    def execute(self):
        try:
            print(f"Running for..{self.fileobj.id}")
            reference_nodes = self.fileobj.node_reference or []
            if not reference_nodes:
                raise Exception("node_reference must be provided for Pivot component")

            source_key = reference_nodes[0]
            if source_key not in SparkAbstract.mapDf:
                raise Exception(f"{source_key} doesnt exist in MAPDF to run PIVOT")

            pivot_column = self.fileobj.pivot_column
            value_column = self.fileobj.pivot_value_column
            index_columns = self.fileobj.pivot_index_columns or []
            if isinstance(index_columns, str):
                index_columns = [index_columns]

            aggregation = self.fileobj.pivot_aggregation or "sum"
            if isinstance(aggregation, str):
                aggregation = aggregation.lower()
            else:
                raise Exception("pivot_aggregation must be provided as a string")
            fill_value = self.fileobj.pivot_fill_value

            if not pivot_column or not value_column:
                raise Exception("pivot_column and pivot_value_column must be configured for Pivot component")

            if not index_columns:
                raise Exception("pivot_index_columns must contain at least one column for Pivot component")

            pivot_source_df = SparkAbstract.mapDf[source_key]

            pivoted_df = pivot_source_df.groupBy(*index_columns).pivot(str(pivot_column)).agg({str(value_column): aggregation})

            if fill_value is not None:
                pivoted_df = pivoted_df.fillna(fill_value)

            SparkAbstract.addToDict(self.fileobj.id, pivoted_df)
            print(f"====> Writing {self.fileobj.id} Successfully to MapDF")

        except Exception as e:
            print("Error in Pivot Component")
            print(e)
            raise Exception(f"Error in PivotComponent-->{e}")
