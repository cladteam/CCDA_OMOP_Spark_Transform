from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, StringType
from transforms.api import transform_df, Input, Output

def choose_most_data(df, primary_key_fields, preference_fields):
    """
    Deduplicate rows by primary key.
    Prefer rows with preference columns that have  NOT null, NOT empty (for strings), and NOT -1 values, but take what you can get.
     
    Input:
     - df
     - primary_key_field, string, name of PK
     - preference_fields, list of strings, names of fields to ......
    Returns - the modified DF
    """

    # Build ordering expression for preference ranking
    # This basically sorts on those columns, but treats NULL and -1 the same way.
    # When more than one column is used, the first column takes precedence.
    good_ct = 0 
    for field in preference_fields:
        dtype = df.schema[field].dataType
        if isinstance(dtype, TimestampType):
            #good_ct += F.when( F.col(field).isNull(), 0).otherwise(1)
            good_ct += F.when( F.col(field).isNull(), 0).otherwise(-1)
        elif isinstance(dtype, StringType):
            #good_ct += F.when( F.col(field).isNull() | (F.col(field) == ''), 0).otherwise(1)
            good_ct += F.when( F.col(field).isNull() | (F.col(field) == ''), 0).otherwise(-1)
        else:
            #good_ct += F.when(F.col(field).isNull() | (F.col(field) == -1), 0).otherwise(1)
            good_ct += F.when(F.col(field).isNull() | (F.col(field) == -1), 0).otherwise(-1)

    df = df.withColumn("good_ct", good_ct)
    
    #window_spec = Window.partitionBy(primary_key_fields).orderBy(["good_ct"], ascending=False)
    window_spec = Window.partitionBy(primary_key_fields).orderBy(["good_ct"])
    return (
        df.withColumn("row_rank", F.row_number().over(window_spec))
          .filter(F.col("row_rank") == 1)
          .drop("row_rank")
    )

