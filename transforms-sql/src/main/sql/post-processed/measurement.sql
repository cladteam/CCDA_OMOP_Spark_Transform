CREATE TABLE `/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post-processed/measurement` AS
    SELECT distinct
      m.measurement_id,
      cast(rm.mspi as LONG) as person_id,
      m.measurement_concept_id,
      to_date(m.measurement_date) as measurement_date,
      m.measurement_datetime,
      m.measurement_time,
      m.measurement_type_concept_id,
      m.operator_concept_id,
      m.value_as_number,
      m.value_as_concept_id,
      m.unit_concept_id,
      m.range_high,
      m.range_low,
      m.provider_id,
      m.visit_occurrence_id,
      m.visit_detail_id,
      m.measurement_source_value,
      m.measurement_source_concept_id,
      m.unit_source_value,
      m.value_source_value,
      map.data_partner_id
 --     mcn.concept_name as measurement_concept_name,
 --     mtcn.concept_name as measurement_type_concept_name,
 --     ocn.concept_name as operator_concept_name,
 --     vcn.concept_name as value_as_concept_name,
 --     ucn.concept_name as unit_concept_name,
 --     mscn.concept_name as measurement_source_concept_name 
    FROM `ri.foundry.main.dataset.1956665b-3f6d-4efb-a6b0-3e87c19d9bd8` m
    JOIN   `/All of Us-cdb223/HIN - HIE/sharedResources/FullyIdentiifed/ccda/ccda_response_metadata` rm
      ON m.filename = rm.response_file_path
    JOIN  `/All of Us-cdb223/HIN - HIE/sharedResources/health_care_site_to_data_partner_id` map
      ON rm.healthcare_site = map.healthcare_site
--     JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` mcn
--       ON mcn.concept_id = m.measurement_concept_id
--     JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` mtcn
--       on  mtcn.concept_id = m.measurement_type_concept_id
--     JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` ocn
--       on ocn.concept_id = m.operator_concept_id
--     JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` vcn
--       on vcn.concept_id = m.value_as_concept_id
--     JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` ucn 
--       on ucn.concept_id = m.unit_concept_id
--     JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` mscn
--       on mscn.concept_id = m.measurement_source_concept_i`