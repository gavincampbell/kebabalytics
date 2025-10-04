
import sys
from kebabalytics.scores import calculate_meaningless_kpis
from pyspark.sql import SparkSession

def main():
    catalog_name = sys.argv[1]
    spark = SparkSession.getActiveSession()
    sales_df = spark.read.table(f"{catalog_name}.default.kebab_orders")
    calculate_meaningless_kpis(sales_df).write.mode("overwrite").saveAsTable(f"{catalog_name}.default.kpis")



