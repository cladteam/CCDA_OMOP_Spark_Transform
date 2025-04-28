# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/datasets/uniquify_stage_1/condition_occurrence"),
    source_df = Input("ri.foundry.main.dataset.e34c8928-d1c1-4b4e-8026-e6024e6afdbb")
)
def compute(source_df):
    return source_df.dropDuplicates()
