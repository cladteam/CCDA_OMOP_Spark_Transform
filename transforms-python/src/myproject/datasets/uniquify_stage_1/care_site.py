# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/datasets/uniquify_stage_1/care_site"),
    source_df = Input("ri.foundry.main.dataset.001d3357-81c1-4d8c-a44b-e2a63a9b7a4c")
)
def compute(source_df):
    return source_df.dropDuplicates()
