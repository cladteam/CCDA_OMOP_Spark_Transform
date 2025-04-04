from transforms.api import transform, Input, Output, configure
from pyspark.sql import types as T
from pyspark.sql import functions as F
import math
import pandas as pd
from datetime import date
from datetime import datetime
import numpy as np

# do it from Pandas, not tuples like list_dataset_files.py does

## @configure(profile=['EXECUTOR_MEMORY_LARGE', 'EXECUTOR_MEMORY_MEDIUM', 'DRIVER_MEMORY_LARGE'] )
@transform(
    single_test_table = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/single_test_table"),

    xml_files=Input("ri.foundry.main.dataset.8c8ff8f9-d429-4396-baed-a3de9c945f49"),
    metadata = Input("ri.foundry.main.dataset.672dd7ae-bbd4-43e8-9b8b-b5c7e8711e79"),
    visit_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/visit_concept_xwalk_mapping_dataset"),
    codemap_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/codemap_xwalk"),
    valueset_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/ccda_value_set_mapping_table_dataset")
)
def compute(
    ctx,
    # outputs
        single_test_table,
    # inputs
        xml_files, 
        metadata, visit_xwalk_ds,
        codemap_xwalk_ds, valueset_xwalk_ds ):
        
    global codemap_xwalk
    global ccda_value_set_mapping_table_dataset
    global visit_concept_xwalk_mapping_dataset

    FILE_LIMIT=10 
    EXPORT_DATASETS=False

    # Link concept maps
    # set package variables to these datasets in an obvious way here
    codemap_xwalk = codemap_xwalk_ds
    ccda_value_set_mapping_table_dataset = valueset_xwalk_ds
    visit_concept_xwalk_mapping_dataset = visit_xwalk_ds

# Q: what generates this error?  `StructType` can not accept object `3720983147285715` in type `int64`.
#             3,720,983,147,285,715
#             q   t   b   m   k
#            15  12   9   6   3
# 3.7 * 10^15
# 2^64 == 18,446,744,073,709,551,616
#         18  15  12   9   6   3 

    data = [ 
        {   'path': 'a', 
            'size': 1, 
            'time_int': 0.003,
            #####'a_long': "3720983147285715",
            'a_long': int("3720983147285715"),
            'string_length':1, 
            'contents': "foo",
            'date': date.fromisoformat('2025-03-30'), 
            'datetime': datetime.fromisoformat('2025-03-30 12:34:01')  
        },
        {   'path': 'b', 
            'size': 1, 
            'time_int': 0.003,
            'a_long': 3720983147285715,
            'string_length':1, 
            'contents': "foo",
            'date': date.fromisoformat('2025-03-30'), 
            'datetime': datetime.fromisoformat('2025-03-30 12:34:01')  
        },
        {   'path': 'c', 
            'size': 1, 
            'time_int': 0.003,
            'a_long': np.int64(3720983147285715),
            'string_length':1, 
            'contents': "foo",
            'date': date.fromisoformat('2025-03-30'), 
            'datetime': datetime.fromisoformat('2025-03-30 12:34:01')  
        }
    ]
    df = pd.DataFrame(data)
    schema = T.StructType([
        T.StructField("path",     T.StringType(), True),
        T.StructField("size",     T.IntegerType(), True),
        T.StructField("time_int", T.FloatType(), True),
        T.StructField("a_long",   T.LongType(), True),
        T.StructField("string_length", T.IntegerType(), True),
        T.StructField("contents", T.StringType(), True),
        T.StructField("date",     T.DateType(), True),
        T.StructField("datetime", T.TimestampType(), True)
        ])
    spark_dff = ctx.spark_session.createDataFrame(df, schema)
    single_test_table.write_dataframe(spark_dff)



    
