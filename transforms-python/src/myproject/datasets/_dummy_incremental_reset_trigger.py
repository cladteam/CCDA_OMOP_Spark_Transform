from transforms.api import transform, Output
from pyspark.sql import SparkSession
import datetime


@transform(
    output=Output("ri.foundry.main.dataset.1cc0185b-10b1-4a44-a565-09d88b1899a2")
)
def compute(ctx, output):
    spark: SparkSession = ctx.spark_session
    # Get the current system time as an ISO string
    now = datetime.datetime.now().isoformat()
    # Create a simple DataFrame with a single integer value
    df = spark.createDataFrame([(now,)], ["col1"])
    output.write_dataframe(df)
