from pyspark.sql import functions as F
from pyspark.sql import types as T
from transforms.api import transform_df, Input, Output
from ..util import ds_schema

@transform_df(
    Output("ri.foundry.main.dataset.001d3357-81c1-4d8c-a44b-e2a63a9b7a4c"),
    omop_eav_dict = Input("ri.foundry.main.dataset.7510d9f2-9597-477c-8f03-e290d81d8d23"),
)
def compute(ctx, omop_eav_dict):
   # OMOP_EAV_DICT is domain_name, key_type, key_value, field_name, field_value
   # care_site is: care_site_id, care_site_name, place_of_service_concept_id, 
   #               location_id, care_site_source_value, place_of_service_source_value,
   #               filename

   # SQL would require a 7-way self-join to pull them all on the same row?

   # I'm not sure how to code this in PySpark yet, but we want to group by domain_name and key_value.
   # Then build a dictionary from the field_name and field_values you find,
   # making sure you have all the fields with None values, in case some are missing.
   # Since this is a file for just care_site, you'd filter on domain_name Care_Site *first*.
   #
   # That makes for simple, 1 file in, 1 file out transforms.
   #
   # I talked to Shawn about possibly partitioning these jobs by domain_name, but if I understood
   # that risks forcing a partition that creates different sizes of input data. There are almost
   # 40m rows for the measurement table, but only 128k person rows, and 1m condition rows.
   # Not experts, maybe there's a way to use the domain_name field to our advantage and not take
   # the hit of unequal sizes...
   #
   # Don't let the difference between a config_name and an omop domain_name get in your way.
   # Each domain (Observation, Measurement, Person, Visit, etc.) may source rows from multiple
   # places in the CCDA document, and so multiple configuration files, each with their own name.
   # For example Drug comes from immunization_immunization_activity, 
   # medication_medication_activity and medication_medication_dispense.
   # It has bitten me more than once. There are a few places where this mapping exists. ddl.py
   # in the old repo, and ds_schema.py and correct_types.py
   #
   # PART OF this process will also involve converting from the strings in the EAV table to the 
   # types needed in Spark and ultimately in union tables for AoU, DQ Dashboard, or the Data 
   # Counts project. See also the correct_types.py and ds_schema.py files mentioned above, here
   # in this project.
   #
   # NB! 
   # When you apply a schema, despite everything in RDBMSes being about column names,
   # the ORDER MATTERS. That's why there are select statements here. They re-order the
   # columns into the order of the schema data.
   # And that schema, which has an oder different from the OMOP DDL, is what is expected
   # by the ingest code for AoU and Data Counts.

    fields = [ 'care_site_id', 'care_site_name', 'place_of_service_concept_id',
           'location_id', 'care_site_source_value', 'place_of_service_source_value',
           'filename'
    ]

    df = omop_eav_dict.select('key_value', 'field_name', 'field_value') \
                .where(F.column('domain_name') == 'Care_Site') \
                .distinct() \
                .groupBy('key_value') \
                .pivot("field_name", fields) \
                .agg(F.first('field_value')) \
                .drop('key_value')

    df = df \
        .withColumn('care_site_id', df['care_site_id'].cast(T.LongType())) \
        .withColumn('location_id', df['location_id'].cast(T.LongType())) \
        .withColumn('place_of_service_concept_id', df['place_of_service_concept_id'].cast(T.IntegerType())) 

    df = df.select(['location_id', 'care_site_id', 'care_site_name', 'place_of_service_concept_id',
                    'care_site_source_value', 'place_of_service_source_value',
                    'filename'])


    new_df = ctx.spark_session.createDataFrame(df.rdd, ds_schema.domain_dataset_schema['Care_Site'])

    return(new_df)
   