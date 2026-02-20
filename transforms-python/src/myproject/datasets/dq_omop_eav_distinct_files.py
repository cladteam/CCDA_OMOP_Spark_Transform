from pyspark.sql import functions as F
from pyspark.sql import types as T
from transforms.api import transform_df, Input, Output, incremental
from . import OMOP_EAV_DICT_FULL_PATH


@incremental(semantic_version=1, snapshot_inputs=["source_df"])
@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/datasets/uniquify_stage_1/dq_omop_eav_distinct_files"),
    source_df=Input(OMOP_EAV_DICT_FULL_PATH),
    dummy_incremental_reset_trigger=Input("ri.foundry.main.dataset.1cc0185b-10b1-4a44-a565-09d88b1899a2"),
)
def compute(source_df, dummy_incremental_reset_trigger):
    # Count all rows before filtering
    total_rows = source_df.count()

    df = source_df.filter(F.col("domain_name") == "Person"
    ).filter(F.col("field_name") == "filename"
    ).agg(
        F.countDistinct(F.col("field_value")).alias("distinct_file_count")
    ).withColumn("rows", F.lit(total_rows).cast(T.LongType()),
    ).withColumn("rows_per_file", F.col("rows") / F.col("distinct_file_count")
    ).withColumn("df_name", F.lit(OMOP_EAV_DICT_FULL_PATH)
    ).withColumn("measured_at", F.current_timestamp()
    ).select(
        "measured_at", "df_name", "distinct_file_count", "rows", "rows_per_file"
    )

    return df
