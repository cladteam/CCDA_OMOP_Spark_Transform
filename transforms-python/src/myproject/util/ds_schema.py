# from pyspark.sql import functions as F

domain_dataset_schema = {
    'Care_Site': T.StructType([
        T.StructField( 'care_site_id', T.LongType(), True),
        T.StructField( 'place_of_service_concept_id', T.IntegerType(), True),
        T.StructField( 'care_site_source_value', T.StringType(), True),
        T.StructField( 'place_of_service_source_value', T.StringType(), True)
    ]),
    #'location':{
    #}
    'Provider':  T.StructType([
        T.StructField( 'gender_concept_id', T.IntegerType(), True),
        T.StructField( 'specialty_concept_id', T.IntegerType(), True),
        T.StructField( 'specialty_source_concept_id', T.IntegerType(), True),
        T.StructField( 'gender_source_concept_id', T.IntegerType(), True),
        T.StructField( 'dea', T.StringType(), True),
        T.StructField( 'npi', T.StringType(), True)
    ]),
    'Person': T.StructType([
        T.StructField( 'race_source_concept_id', T.IntegerType(), True),
        T.StructField( 'race_source_value', T.StringType(), True),
        T.StructField( 'ethnicity_source_concept_id', T.IntegerType(), True),
        T.StructField( 'ethnicity_source_value', T.StringType(), True),
        T.StructField( 'gender_source_concept_id', T.IntegerType(), True),
        T.StructField( 'gender_source_value', T.StringType(), True),
        T.StructField( 'provider_id', T.LongType(), True),
        T.StructField( 'gender_concept_id', T.IntegerType(), True),
        T.StructField( 'ethnicity_concept_id', T.IntegerType(), True),
        T.StructField( 'race_concept_id', T.IntegerType(), True)
    ]),
    'Visit': T.StructType([
        T.StructField( 'admitting_source_concept_id', T.IntegerType(), True),
        T.StructField( 'care_site_id', T.LongType(), True),
        T.StructField( 'discharge_to_concept_id', T.IntegerType(), True),
        T.StructField( 'visit_source_concept_id',  T.IntegerType(), True),
        T.StructField( 'preceding_visit_occurrence_id', T.LongType(), True),
        T.StructField( 'visit_type_concept_id', T.IntegerType(), True),
        T.StructField( 'visit_concept_id', T.IntegerType(), True)
    ]),
    'Measurement': T.StructType([
        T.StructField( 'operator_concept_id', T.IntegerType(), True),
        T.StructField( 'value_as_concept_id', T.IntegerType(), True),
        T.StructField( 'unit_concept_id', T.IntegerType(), True),
        T.StructField( 'measurement_source_concept_id', T.IntegerType(), True),
        T.StructField( 'measurement_datetime', T.TimestampType(), True),
        T.StructField( 'measurement_date', T.DateType(), True),
        T.StructField( 'measurement_time', T.StringType(), True),
        T.StructField( 'range_low', T.FloatType(), True),
        T.StructField( 'range_high', T.FloatType(), True),
        T.StructField( 'provider_id', T.LongType(), True),
        T.StructField( 'visit_occurrence_id', T.LongType(), True),
        T.StructField( 'visit_detail_id', T.LongType(), True),
        T.StructField( 'measurement_type_concept_id',  T.IntegerType(), True),
        T.StructField( 'value_as_number', T.DoubleType(), True)
    ]),
    'Observation': T.StructType([
        T.StructField( 'value_as_number', T.FloatType(), True),
        T.StructField( 'qualifier_concept_id', T.IntegerType(), True),
        T.StructField( 'unit_concept_id', T.IntegerType(), True),
        T.StructField( 'observation_source_concept_id', T.IntegerType(), True),
        T.StructField( 'observation_datetime', T.TimestampType(), True),
        T.StructField( 'observation_date', T.DateType(), True),
        T.StructField( 'provider_id', T.LongType(), True),
        T.StructField( 'visit_occurrence_id', T.LongType(), True),
        T.StructField( 'visit_detail_id', T.LongType(), True),
        T.StructField( 'observation_type_concept_id', T.IntegerType(), True),
        T.StructField( 'value_as_concept_id', T.IntegerType(), True),
        T.StructField( 'value_as_number', T.DoubleType(), True)
    ]),
    'Condition': T.StructType([
        T.StructField( 'visit_occurrence_id', T.LongType(), True),
        T.StructField( 'condition_status_concept_id', T.IntegerType(), True),
        T.StructField( 'condition_source_concept_id', T.IntegerType(), True),
        T.StructField( 'condition_end_datetime', T.TimestampType(), True),
        T.StructField( 'condition_end_date', T.DateType(), True),
        T.StructField( 'condition_status_source_value', T.StringType(), True),
        T.StructField( 'provider_id', T.LongType(), True),
        T.StructField( 'visit_occurrence_id', T.LongType(), True),
        T.StructField( 'visit_detail_id', T.LongType(), True),
        T.StructField(  'condition_type_concept_id', T.IntegerType(), True)
    ]),
    'Procedure': T.StructType([
        T.StructField( 'visit_occurrence_id', T.LongType(), True),
        T.StructField( 'modifier_concept_id', T.IntegerType(), True),
        T.StructField( 'modifier_source_value', T.StringType(), True),
        T.StructField( 'procedure_source_concept_id', T.IntegerType(), True),
        T.StructField( 'provider_id', T.LongType(), True),
        
        T.StructField( 'visit_detail_id', T.LongType(), True),
        T.StructField( 'procedure_type_concept_id', T.IntegerType(), True),
        T.StructField( 'procedure_concept_id', T.IntegerType(), True),
        T.StructField( 'procedure_source_concept_id', T.IntegerType(), True)
    ]),
    'Drug': T.StructType([
        T.StructField( 'quantity', T.FloatType(), True),
        T.StructField( 'sig', T.StringType(), True),
        T.StructField( 'route_concept_id', T.IntegerType(), True),
        T.StructField( 'route_source_value', T.StringType(), True),
        T.StructField( 'lot_number', T.StringType(), True),
        T.StructField( 'visit_occurrence_id', T.LongType(), True),
        T.StructField( 'provider_id', T.LongType(), True),
        T.StructField( 'visit_occurrence_id', T.LongType(), True),
        T.StructField( 'visit_detail_id', T.LongType(), True)
    ])
}
