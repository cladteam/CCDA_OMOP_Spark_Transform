from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F
from pyspark.sql import types as T
from ..util import ds_schema


@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/location"),
    omop_eav_dict = Input("ri.foundry.main.dataset.ce6307e8-388a-4c71-b407-4744bee5ec7f"),
)
def compute(ctx, omop_eav_dict):

    df = omop_eav_dict.select('key_value', 'field_name', 'field_value') \
                .where(F.column('domain_name') == 'Location') \
                .distinct() \
                .groupBy('key_value') \
                .pivot("field_name") \
                .agg(F.first('field_value')) \
                .drop('key_value')

                # works
                #.agg(F.collect_list('field_value') ) \

    df = df.withColumn('location_id', df['location_id'].cast(T.LongType())) 

    df = df.select(['location_id', 'address_1', 'address_2', 'city', 'state', 
                    'zip', 'county', 'location_source_value',
                    'filename'])
    df =ctx.spark_session.createDataFrame(df.rdd, ds_schema.domain_dataset_schema['Location'])
    return(df)

   