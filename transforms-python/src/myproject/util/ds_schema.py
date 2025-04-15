
# from pyspark.sql import functions as F
# #
# #
#### this is woefully incomplete
# #
# #
from pyspark.sql import types as T

# ISSUES
# - dates in condition

domain_key_fields = {
    'Person': 'person_id',
    'Condition':  'condition_occurrence_id',
    'Device': 'device_exposure_id',
    'Drug':  'drug_exposure_id',
    'Measurement':  'measurement_id',
    'Observation': 'observation_id',
    'Procedure': 'procedure_id',
    'Visit':   'visit_occurrence_id',
    'Care_Site': 'care_site_id',
    'Location': 'location_id',
    'Provider': 'procedure_id'
}

domain_dataset_schema = {

    'Person': T.StructType([
#   T.StructField('payload', T.StringType(), True),
#    T.StructField('data_partner_id', T.IntegerType(), True),
#   T.StructField('global_person_id', T.StringType(), True),
    T.StructField('person_id', T.LongType(), True),                     # 1 !
    T.StructField('gender_concept_id', T.IntegerType(), True),          # 2 !
    T.StructField('year_of_birth', T.IntegerType(), True),              # 3 !
    T.StructField('month_of_birth', T.IntegerType(), True),             # 4 !
    T.StructField('day_of_birth', T.IntegerType(), True),               # 5 !
    T.StructField('birth_datetime', T.TimestampType(), True),           # 6 !
    T.StructField('race_concept_id', T.IntegerType(), True),            # 7 !
    T.StructField('ethnicity_concept_id', T.IntegerType(), True),       # 8 !
    T.StructField('location_id', T.LongType(), True),                   # 9 !
    T.StructField('provider_id', T.LongType(), True),                   #10 !
    T.StructField('care_site_id', T.LongType(), True),                  #11 !
    T.StructField('person_source_value', T.StringType(), True),         #12 !
    T.StructField('gender_source_value', T.StringType(), True),         #13 !
    T.StructField('gender_source_concept_id', T.IntegerType(), True),   #14 !
    T.StructField('race_source_value', T.StringType(), True),           #15 !
    T.StructField('race_source_concept_id', T.IntegerType(), True),     #16 !
    T.StructField('ethnicity_source_value', T.StringType(), True),      #17 !
    T.StructField('ethnicity_source_concept_id', T.IntegerType(), True) #18 !
#   T.StructField('gender_concept_name', T.StringType(), True),
#   T.StructField('ethnicity_concept_name', T.StringType(), True),
#   T.StructField('race_concept_name', T.StringType(), True),
#   T.StructField('gender_source_concept_name', T.StringType(), True),
#   T.StructField('ethnicity_source_concept_name', T.StringType(), True),  
#   T.StructField('race_source_concept_name', T.StringType(), True)
    ]),

    'Condition': T.StructType([ 
#   T.StructField('payload', T.StringType(), True),
#   T.StructField('data_partner_id', T.IntegerType(), True),

    T.StructField('condition_occurrence_id',      T.LongType(), True),         # 1
    T.StructField('person_id',                    T.LongType(), True),                       # 2
    T.StructField('condition_concept_id',         T.IntegerType(), True),         # 3

    T.StructField('condition_start_date',         T.DateType(), True),            # 4
    #T.StructField('condition_start_date', T.StringType(), True),         # 4

    T.StructField('condition_start_datetime',     T.TimestampType(), True),   # 5
    #T.StructField('condition_start_datetime', T.StringType(), True),     # 5

    T.StructField('condition_end_date',           T.DateType(), True),              # 6
    #T.StructField('condition_end_date', T.StringType(), True),           # 6

    T.StructField('condition_end_datetime',       T.TimestampType(), True),     # 7
    #T.StructField('condition_end_datetime', T.StringType(), True),       # 7

    T.StructField('condition_type_concept_id',     T.IntegerType(), True),    # 8
    T.StructField('condition_status_concept_id',   T.IntegerType(), True),  # 9
    T.StructField('stop_reason',                   T.StringType(), True),                   #10
    T.StructField('provider_id',                   T.LongType(), True),                     #11
    T.StructField('visit_occurrence_id',           T.LongType(), True),             #12
    T.StructField('visit_detail_id',               T.LongType(), True),                 #13
    T.StructField('condition_source_value',        T.StringType(), True),        #14
    T.StructField('condition_source_concept_id',   T.IntegerType(), True),  #15
    T.StructField('condition_status_source_value', T.StringType(), True)  #16

#   T.StructField('condition_concept_name', T.StringType(), True),
#   T.StructField('condition_type_concept_name', T.StringType(), True),
#   T.StructField('condition_source_concept_name', T.StringType(), True),
#   T.StructField('condition_status_concept_name', T.StringType(), True)
     ]), 

    'Death' : T.StructType([
    T.StructField('payload', T.StringType(), True),
    T.StructField('data_partner_id', T.IntegerType(), True),
    T.StructField('death_datetime', T.TimestampType(), True),
    T.StructField('cause_source_concept_id', T.IntegerType(), True),
    T.StructField('cause_concept_id', T.IntegerType(), True),
    T.StructField('person_id', T.LongType(), True),
    T.StructField('death_date', T.DateType(), True),
    T.StructField('death_type_concept_id', T.IntegerType(), True),
    T.StructField('cause_source_value', T.StringType(), True),
    T.StructField('cause_concept_name', T.StringType(), True),
    T.StructField('death_type_concept_name', T.StringType(), True),
    T.StructField('cause_source_concept_name', T.StringType(), True)
    ]),

    'Device': T.StructType([
    T.StructField('visit_detail_id', T.LongType(), True),
#    T.StructField('payload', T.StringType(), True),
#    T.StructField('data_partner_id', T.IntegerType(), True),
    T.StructField('visit_occurrence_id', T.LongType(), True),
    T.StructField('provider_id', T.LongType(), True),
    T.StructField('person_id', T.LongType(), True),
    T.StructField('device_exposure_id', T.LongType(), True),
    T.StructField('device_concept_id', T.IntegerType(), True),
    T.StructField('device_exposure_start_date', T.DateType(), True),
    T.StructField('device_exposure_start_datetime', T.TimestampType(), True),
    T.StructField('device_exposure_end_date', T.DateType(), True),
    T.StructField('device_exposure_end_datetime', T.TimestampType(), True),
    T.StructField('device_type_concept_id', T.IntegerType(), True),
    T.StructField('unique_device_id', T.StringType(), True),
    T.StructField('quantity', T.IntegerType(), True),
    T.StructField('device_source_value', T.StringType(), True),
    T.StructField('device_source_concept_id', T.IntegerType(), True),
#    T.StructField('device_concept_name', T.StringType(), True),
#    T.StructField('device_type_concept_name', T.StringType(), True),
#    T.StructField('device_source_concept_name', T.StringType(), True)
     ]), 

    'Drug': T.StructType([
    T.StructField('visit_detail_id', T.LongType(), True),                   #19
#   T.StructField('payload', T.StringType(), True),
#   T.StructField('data_partner_id', T.IntegerType(), True),
    T.StructField('sig', T.StringType(), True),                             #14
    T.StructField('verbatim_end_date', T.DateType(), True),                 # 8
    T.StructField('visit_occurrence_id', T.LongType(), True),               #18
    T.StructField('provider_id', T.LongType(), True),                       #17
    T.StructField('days_supply', T.IntegerType(), True),                    #13
    T.StructField('quantity', T.FloatType(), True),                         #12
    T.StructField('person_id', T.LongType(), True),                         # 2
    T.StructField('drug_exposure_id', T.LongType(), True),                  # 1
    T.StructField('drug_concept_id', T.IntegerType(), True),                # 3
    T.StructField('drug_exposure_start_date', T.DateType(), True),          # 4
    T.StructField('drug_exposure_start_datetime', T.TimestampType(), True), # 5
    T.StructField('drug_exposure_end_date', T.DateType(), True),            # 6
    T.StructField('drug_exposure_end_datetime', T.TimestampType(), True),   # 7
    T.StructField('drug_type_concept_id', T.IntegerType(), True),           # 9
    T.StructField('stop_reason', T.StringType(), True),                     #10
    T.StructField('refills', T.IntegerType(), True),                        #11
    T.StructField('route_concept_id', T.IntegerType(), True),               #15
    T.StructField('lot_number', T.StringType(), True),                      #16
    T.StructField('drug_source_value', T.StringType(), True),               #20
    T.StructField('drug_source_concept_id', T.IntegerType(), True),         #21
    T.StructField('route_source_value', T.StringType(), True),              #22
    T.StructField('dose_unit_source_value', T.StringType(), True),          #23
#    T.StructField('drug_concept_name', T.StringType(), True),
#    T.StructField('drug_type_concept_name', T.StringType(), True),
#    T.StructField('route_concept_name', T.StringType(), True),
#    T.StructField('drug_source_concept_name', T.StringType(), True)
     ]), 

    'Measurement': T.StructType([
#   T.StructField('payload', T.StringType(), True),
#   T.StructField('data_partner_id', T.IntegerType(), True),
    T.StructField('measurement_time', T.StringType(), True),
    T.StructField('visit_detail_id', T.LongType(), True),
    T.StructField('visit_occurrence_id', T.LongType(), True),
    T.StructField('provider_id', T.LongType(), True),
    T.StructField('range_high', T.FloatType(), True),
    T.StructField('range_low', T.FloatType(), True),
    T.StructField('unit_concept_id', T.IntegerType(), True),
    T.StructField('person_id', T.LongType(), True),
    T.StructField('measurement_id', T.LongType(), True),
    T.StructField('measurement_concept_id', T.IntegerType(), True),
    T.StructField('measurement_date', T.DateType(), True),
    T.StructField('measurement_datetime', T.TimestampType(), True),
    T.StructField('measurement_type_concept_id', T.IntegerType(), True),
    T.StructField('operator_concept_id', T.IntegerType(), True),
    T.StructField('value_as_number', T.DoubleType(), True),
    T.StructField('value_as_concept_id', T.IntegerType(), True),
    T.StructField('measurement_source_value', T.StringType(), True),
    T.StructField('measurement_source_concept_id', T.IntegerType(), True),
    T.StructField('unit_source_value', T.StringType(), True),
    T.StructField('value_source_value', T.StringType(), True),
#   T.StructField('measurement_concept_name', T.StringType(), True),
#   T.StructField('measurement_type_concept_name', T.StringType(), True),
#   T.StructField('operator_concept_name', T.StringType(), True),
#   T.StructField('value_as_concept_name', T.StringType(), True),
#   T.StructField('unit_concept_name', T.StringType(), True),
#   T.StructField('measurement_source_concept_name', T.StringType(), True)
     ]),

    'Observation': T.StructType([
 #   T.StructField('payload', T.StringType(), True),
 #   T.StructField('data_partner_id', T.IntegerType(), True),
    T.StructField('visit_detail_id', T.LongType(), True),
    T.StructField('visit_occurrence_id', T.LongType(), True),
    T.StructField('provider_id', T.LongType(), True),
    T.StructField('value_as_number', T.DoubleType(), True),
    T.StructField('person_id', T.LongType(), True),
    T.StructField('observation_id', T.LongType(), True),
    T.StructField('observation_concept_id', T.IntegerType(), True),
    T.StructField('observation_source_value', T.StringType(), True),
    T.StructField('observation_source_concept_id', T.IntegerType(), True),
    T.StructField('observation_date', T.DateType(), True),
    T.StructField('observation_datetime', T.TimestampType(), True),
    T.StructField('observation_type_concept_id', T.IntegerType(), True),
    T.StructField('value_as_string', T.StringType(), True),
    T.StructField('value_as_concept_id', T.IntegerType(), True),
    T.StructField('qualifier_concept_id', T.IntegerType(), True),
    T.StructField('qualifier_source_value', T.StringType(), True),
    T.StructField('unit_concept_id', T.IntegerType(), True),
    T.StructField('unit_source_value', T.StringType(), True),
 #   T.StructField('observation_source_concept_name', T.StringType(), True),
 #   T.StructField('observation_concept_name', T.StringType(), True),
 #   T.StructField('observation_type_concept_name', T.StringType(), True),
 #   T.StructField('qualifier_concept_name', T.StringType(), True),
 #   T.StructField('unit_concept_name', T.StringType(), True),
 #   T.StructField('value_as_concept_name', T.StringType(), True)
     ]), 

    'Procedure': T.StructType([
    T.StructField('visit_detail_id', T.LongType(), True),
 #   T.StructField('payload', T.StringType(), True),
 #   T.StructField('data_partner_id', T.IntegerType(), True),
    T.StructField('procedure_source_concept_id', T.IntegerType(), True),
    T.StructField('procedure_concept_id', T.IntegerType(), True),
    T.StructField('visit_occurrence_id', T.LongType(), True),
    T.StructField('person_id', T.LongType(), True),
    T.StructField('procedure_occurrence_id', T.LongType(), True),
    T.StructField('provider_id', T.LongType(), True),
    T.StructField('modifier_concept_id', T.IntegerType(), True),
    T.StructField('procedure_date', T.DateType(), True),
    T.StructField('procedure_datetime', T.TimestampType(), True),
    T.StructField('procedure_type_concept_id', T.IntegerType(), True),
    T.StructField('quantity', T.IntegerType(), True),
    T.StructField('procedure_source_value', T.StringType(), True),
    T.StructField('modifier_source_value', T.StringType(), True),
 #   T.StructField('procedure_concept_name', T.StringType(), True),
 #   T.StructField('procedure_type_concept_name', T.StringType(), True),
 #   T.StructField('procedure_source_concept_name', T.StringType(), True),
 #   T.StructField('modifier_concept_name', T.StringType(), True)
     ]), 

    'Visit_Detail': T.StructType([
    T.StructField('payload', T.StringType(), True),
    T.StructField('data_partner_id', T.IntegerType(), True),
    T.StructField('visit_occurrence_id', T.LongType(), True),
    T.StructField('visit_detail_parent_id', T.LongType(), True),
    T.StructField('preceding_visit_detail_id', T.LongType(), True),
    T.StructField('care_site_id', T.LongType(), True),
    T.StructField('provider_id', T.LongType(), True),
    T.StructField('person_id', T.LongType(), True),
    T.StructField('visit_detail_id', T.LongType(), True),
    T.StructField('visit_detail_concept_id', T.IntegerType(), True),
    T.StructField('visit_detail_start_date', T.DateType(), True),
    T.StructField('visit_detail_start_datetime', T.TimestampType(), True),
    T.StructField('visit_detail_end_date', T.DateType(), True),
    T.StructField('visit_detail_end_datetime', T.TimestampType(), True),
    T.StructField('visit_detail_type_concept_id', T.IntegerType(), True),
    T.StructField('visit_detail_source_value', T.StringType(), True),
    T.StructField('visit_detail_source_concept_id', T.IntegerType(), True),
    T.StructField('admitting_source_value', T.StringType(), True),
    T.StructField('admitting_source_concept_id', T.IntegerType(), True),
    T.StructField('discharge_to_source_value', T.StringType(), True),
    T.StructField('discharge_to_concept_id', T.IntegerType(), True),
    T.StructField('discharge_to_concept_name', T.StringType(), True),
    T.StructField('visit_detail_concept_name', T.StringType(), True),
    T.StructField('visit_detail_type_concept_name', T.StringType(), True),
    T.StructField('visit_detail_source_concept_name', T.StringType(), True),
    T.StructField('admitting_source_concept_name', T.StringType(), True)
     ]), 

    'Visit': T.StructType([
#    T.StructField('data_partner_id', T.IntegerType(), True),
#    T.StructField('payload', T.StringType(), True),
    T.StructField('visit_source_value', T.StringType(), True),
    T.StructField('person_id', T.LongType(), True),
    T.StructField('visit_occurrence_id', T.LongType(), True),
    T.StructField('visit_source_concept_id', T.IntegerType(), True),
    T.StructField('preceding_visit_occurrence_id', T.LongType(), True),
    T.StructField('discharge_to_concept_id', T.IntegerType(), True),
    T.StructField('admitting_source_concept_id', T.IntegerType(), True),
    T.StructField('care_site_id', T.LongType(), True),
    T.StructField('provider_id', T.LongType(), True),
    T.StructField('visit_concept_id', T.IntegerType(), True),
    T.StructField('visit_start_date', T.DateType(), True),
    T.StructField('visit_start_datetime', T.TimestampType(), True),
    T.StructField('visit_end_date', T.DateType(), True),
    T.StructField('visit_end_datetime', T.TimestampType(), True),
    T.StructField('visit_type_concept_id', T.IntegerType(), True),
    T.StructField('admitting_source_value', T.StringType(), True),
    T.StructField('discharge_to_source_value', T.StringType(), True),
#    T.StructField('visit_concept_name', T.StringType(), True),
#    T.StructField('visit_type_concept_name', T.StringType(), True),
#    T.StructField('visit_source_concept_name', T.StringType(), True),
#    T.StructField('admitting_source_concept_name', T.StringType(), True),
#    T.StructField('discharge_to_concept_name', T.StringType(), True)
     ]), 

    'Care_Site': T.StructType([ 
#    T.StructField('payload', T.StringType(), True),
#    T.StructField('data_partner_id', T.IntegerType(), True),
    T.StructField('location_id', T.LongType(), True),
    T.StructField('care_site_id', T.LongType(), True),
    T.StructField('care_site_name', T.StringType(), True),
    T.StructField('place_of_service_concept_id', T.IntegerType(), True),
    T.StructField('care_site_source_value', T.StringType(), True),
    T.StructField('place_of_service_source_value', T.StringType(), True),
 #   T.StructField('place_of_service_concept_name', T.StringType(), True)
     ]), 

    'Location': T.StructType([
 #   T.StructField('payload', T.StringType(), True),
 #   T.StructField('data_partner_id', T.IntegerType(), True),
    T.StructField('location_id', T.LongType(), True),
    T.StructField('address_1', T.StringType(), True),
    T.StructField('address_2', T.StringType(), True),
    T.StructField('city', T.StringType(), True),
    T.StructField('state', T.StringType(), True),
    T.StructField('zip', T.StringType(), True),
    T.StructField('county', T.StringType(), True),
    T.StructField('location_source_value', T.StringType(), True)
     ]), 

    'Observation_Period': T.StructType([
    T.StructField('payload', T.StringType(), True),
    T.StructField('data_partner_id', T.IntegerType(), True),
    T.StructField('observation_period_start_date', T.DateType(), True),
    T.StructField('observation_period_end_date', T.DateType(), True),
    T.StructField('person_id', T.LongType(), True),
    T.StructField('observation_period_id', T.LongType(), True),
    T.StructField('period_type_concept_id', T.IntegerType(), True),
    T.StructField('period_type_concept_name', T.StringType(), True)
     ]), 

    'Payer_Plan_Period': T.StructType([ 
    T.StructField('payer_source_value', T.StringType(), True),
    T.StructField('data_partner_id', T.IntegerType(), True),
    T.StructField('payer_plan_period_end_date', T.DateType(), True),
    T.StructField('stop_reason_concept_id', T.IntegerType(), True),
    T.StructField('sponsor_source_concept_id', T.IntegerType(), True),
    T.StructField('plan_concept_name', T.StringType(), True),
    T.StructField('sponsor_source_value', T.StringType(), True),
    T.StructField('family_source_value', T.StringType(), True),
    T.StructField('payer_concept_id', T.IntegerType(), True),
    T.StructField('stop_reason_source_concept_id', T.IntegerType(), True),
    T.StructField('payer_plan_period_start_date', T.DateType(), True),
    T.StructField('stop_reason_source_value', T.StringType(), True),
    T.StructField('sponsor_concept_name', T.StringType(), True),
    T.StructField('plan_source_concept_id', T.IntegerType(), True),
    T.StructField('plan_concept_id', T.IntegerType(), True),
    T.StructField('sponsor_concept_id', T.IntegerType(), True),
    T.StructField('payload', T.StringType(), True),
    T.StructField('plan_source_concept_name', T.StringType(), True),
    T.StructField('payer_source_concept_name', T.StringType(), True),
    T.StructField('payer_concept_name', T.StringType(), True),
    T.StructField('sponsor_source_concept_name', T.StringType(), True),
    T.StructField('payer_plan_period_id', T.LongType(), True),
    T.StructField('plan_source_value', T.StringType(), True),
    T.StructField('stop_reason_concept_name', T.StringType(), True),
    T.StructField('payer_source_concept_id', T.IntegerType(), True),
    T.StructField('stop_reason_source_concept_name', T.StringType(), True),
    T.StructField('person_id', T.LongType(), True)
     ]), 

    'Provider': T.StructType([
#   T.StructField('payload', T.StringType(), True),
#   T.StructField('data_partner_id', T.IntegerType(), True),
    T.StructField('year_of_birth', T.IntegerType(), True),
    T.StructField('care_site_id', T.LongType(), True),
    T.StructField('provider_id', T.LongType(), True),
    T.StructField('provider_name', T.StringType(), True),
    T.StructField('npi', T.StringType(), True),
    T.StructField('dea', T.StringType(), True),
    T.StructField('specialty_concept_id', T.IntegerType(), True),
    T.StructField('gender_concept_id', T.IntegerType(), True),
    T.StructField('provider_source_value', T.StringType(), True),
    T.StructField('specialty_source_value', T.StringType(), True),
    T.StructField('specialty_source_concept_id', T.IntegerType(), True),
    T.StructField('gender_source_value', T.StringType(), True),
    T.StructField('gender_source_concept_id', T.IntegerType(), True),
#   T.StructField('specialty_concept_name', T.StringType(), True),
#   T.StructField('gender_concept_name', T.StringType(), True),
#   T.StructField('specialty_source_concept_name', T.StringType(), True),
#   T.StructField('gender_source_concept_name', T.StringType(), True)
    ])
}
