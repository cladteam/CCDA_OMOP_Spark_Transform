# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from . import OMOP_EAV_DICT_FULL_PATH
from . import OMOP_EAV_DICT_BACKUP_FULL_PATH

# Always pull from this branch
source_branch = "master"


@transform_df(
    Output(OMOP_EAV_DICT_BACKUP_FULL_PATH),
    source_df=Input(OMOP_EAV_DICT_FULL_PATH, branch=source_branch),
)
def compute(source_df):
    """
    Create a copy of the big OMOP_EAV_DICT dataframe for reference, run as needed.
    Always pull from the same source branch.
    """
    return source_df
