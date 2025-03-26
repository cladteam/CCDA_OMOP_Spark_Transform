
from transforms.api import transform, Input, Output
from pyspark.sql import types as T
from pyspark.sql import functions as F


import os


def process_dataset_of_files_initial(dataset):
    ## A TransformInput object does not have an attribute files. Please check the spelling and/or the datatype of the object.
    ccda_documents_generator = dataset.files()  ## <---- no attribute files!
    for filegen in ccda_documents_generator:
        filepath = filegen.download()
        print(f"PROCESSING {os.path.basename(filepath)}")

## def process_dataset_of_files(dataset):


    
def create_empty_df(ctx):
    schema = T.StructType([T.StructField("key", T.StringType(), True)])
    df = ctx.spark_session.createDataFrame([("dummy_key",)], schema)
    df = df.withColumn('when', F.current_timestamp())
    return df
 

@transform(
    condition_occurrence = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP/condition_occurrence"),
    drug_exposure = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP/drug_exposure"),
    location = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP/location"),
    measurement = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP/measurement"),
    observation = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP/observation"),
    person = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP/person"),
    procedure_occurrence = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP/procedure_occurrence"),
    provider = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP/provider"),
    visit_occurrence = Output("ri.foundry.main.dataset.1fc47371-d39c-4985-aacd-c0aacf5484b3"),

    xml_files=Input("ri.foundry.main.dataset.8c8ff8f9-d429-4396-baed-a3de9c945f49"),
    # xml_files = Input("/All of Us-cdb223/HIN - HIE/sharedResources/FullyIdentified/ccda/ccda_response_files"),
    metadata = Input("ri.foundry.main.dataset.672dd7ae-bbd4-43e8-9b8b-b5c7e8711e79"),
    # metadata = Input("/All of Us-cdb223/HIN - HIE/sharedResources/FullyIdentified/ccda/ccda_response_metadata"),
    visit_xwalk = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/visit_concept_xwalk_mapping_dataset"),
    codemap_xwalk = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/codemap_xwalk"),
    valueset_xwalk = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/ccda_value_set_mapping_table_dataset")
)
def compute(
    ctx,
    # outputs
        condition_occurrence,  drug_exposure, location, 
        measurement, observation, person,
        procedure_occurrence, provider, visit_occurrence,
    # inputs
        xml_files, 
        metadata, visit_xwalk,
        codemap_xwalk, valueset_xwalk ):

    #process_dataset_of_files(xml_files)

    fdf = create_empty_df(ctx)
    condition_occurrence.write_dataframe(fdf)
    drug_exposure.write_dataframe(fdf)
    location.write_dataframe(fdf)
    measurement.write_dataframe(fdf)
    observation.write_dataframe(fdf)
    person.write_dataframe(fdf)
    procedure_occurrence.write_dataframe(fdf)
    provider.write_dataframe(fdf)
    visit_occurrence.write_dataframe(fdf)



    
