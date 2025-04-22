from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post_vocab_stage_2/procedure_occurrence"),
    procedures = Input("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/procedure_occurrence"),
    codemap = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/codemap_xwalk")
)
def compute(procedures, codemap):
    split_source_value = F.split(procedures.procedure_source_value, '\\|') # splits on a regex, escape the 'or'
    df = procedures.withColumn('procedure_concept_source_system', split_source_value.getItem(1)) \
                  .withColumn('procedure_concept_source_code', split_source_value.getItem(0))

    df = df.join(codemap, (df.procedure_concept_source_system == codemap.src_vocab_code_system) & \
                          (df.procedure_concept_source_code == codemap.src_code),
                          "leftouter")

    df = df.withColumn('procedure_concept_id', df.target_concept_id)
    df = df.withColumn('procedure_source_concept_id', df.source_concept_id)

    df = df.select([
        'visit_detail_id', 'procedure_source_concept_id', 'procedure_concept_id', 'visit_occurrence_id',
        'person_id', 'procedure_occurrence_id', 'provider_id', 'modifier_concept_id',
        'procedure_date', 'procedure_datetime', 'procedure_type_concept_id',
        'quantity', 'procedure_source_value', 'modifier_source_value'
    ]) 

    return df