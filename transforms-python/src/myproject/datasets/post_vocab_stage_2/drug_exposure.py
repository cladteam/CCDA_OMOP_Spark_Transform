from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post_vocab_stage_2/drug_exposure"),
    drugs = Input("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/drug_exposure"),
    codemap = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/codemap_xwalk")
)
def compute(drugs, codemap):
    split_source_value = F.split(drugs.drug_source_value, '\\|') # splits on a regex, escape the 'or'
    df = drugs.withColumn('drug_concept_source_system', split_source_value.getItem(1)) \
                  .withColumn('drug_concept_source_code', split_source_value.getItem(0))

    df = df.join(codemap, (df.drug_concept_source_system == codemap.src_vocab_code_system) & \
                          (df.drug_concept_source_code == codemap.src_code),
                          "left outer"  ) 

    df = df.withColumn('drug_concept_id', df.source_concept_id)

    df = df.select([ 
        'visit_detail_id', 'sig', 'verbatim_end_date',
        'visit_occurrence_id', 'provider_id', 'days_supply', 'quantity',
        'person_id', 'drug_exposure_id', 'drug_concept_id',
        'drug_exposure_start_date', 'drug_exposure_start_datetime',
        'drug_exposure_end_date', 'drug_exposure_end_datetime',
        'drug_type_concept_id', 'stop_reason', 'refills', 'route_concept_id', 'lot_number',
        'drug_source_value', 'drug_source_concept_id', 'route_source_value',
        'dose_unit_source_value'
    ])

    
    return df