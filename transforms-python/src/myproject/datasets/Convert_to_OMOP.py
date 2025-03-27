# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/datasets/Convert_to_OMOP"),
    source_df=Input("SOURCE_DATASET_PATH"),
)
def compute(source_df):
    return source_df
from transforms.api import transform_df, Input, Output, transform
from pyspark.sql import types as T
from pyspark.sql import Row
from pyspark.sql import functions as F
import io
import time

from prototype_2 import layer_datasets


    
def create_empty_df(ctx):
    schema = T.StructType([T.StructField("key", T.StringType(), True)])
    df = ctx.spark_session.createDataFrame([("dummy_key",)], schema)
    df = df.withColumn('when', F.current_timestamp())
    return df

@transform(
    output_df = Output("/All of Us-cdb223/HIN - HIE/CCDA/datasets/list_dataset_files"),
    # the big production set
    xml_files=Input("ri.foundry.main.dataset.8c8ff8f9-d429-4396-baed-a3de9c945f49")
    # the test set
    #xml_files=Input("ri.foundry.main.dataset.877bc6a8-2ec1-4b21-9794-4ad02cc27e30")
)
def compute(ctx, output_df, xml_files):
    filestatus_list = list(xml_files.filesystem().ls())

    file_limit=60
    file_count=0
    tuple_list = []
    fs = xml_files.filesystem()
    for status in filestatus_list:
        start_time = time.time()
        with fs.open(status.path, 'rb') as f:
            br = io.BufferedReader(f)
            tw = io.TextIOWrapper(br) 
            contents = tw.readline(1000)
            contents = tw.readline()
            for line in tw:
                contents += line
            layer_datasets.process_string(contents, status.path, False )

            end_time = time.time()
            time_int  = end_time - start_time
            string_length  =  len(contents)
            tuple_list.append([status.path, status.size, time_int, string_length, contents])

        # TODO combine datasets, write and export

        file_count += 1
        if file_count > file_limit:
            break
    schema = T.StructType([
        T.StructField("path", T.StringType(), True),
        T.StructField("size", T.IntegerType(), True),
        T.StructField("time_int", T.FloatType(), True),
        T.StructField("string_length", T.IntegerType(), True),
        T.StructField("contents", T.StringType(), True)
        ])
    df = ctx.spark_session.createDataFrame(tuple_list, schema)
    output_df.write_dataframe(df)
