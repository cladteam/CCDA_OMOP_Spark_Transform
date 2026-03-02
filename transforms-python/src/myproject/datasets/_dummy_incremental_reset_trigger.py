from transforms.api import transform, Output
from pyspark.sql import SparkSession
import datetime
from . import DUMMY_TRIGGER_PATH

@transform(
    output=Output(DUMMY_TRIGGER_PATH)
)
def compute(ctx, output):
    spark: SparkSession = ctx.spark_session
    # Get the current system time as an ISO string
    now = datetime.datetime.now().isoformat()
    # Create a simple DataFrame with a single integer value
    df = spark.createDataFrame([(now,)], ["col1"])
    output.write_dataframe(df)
