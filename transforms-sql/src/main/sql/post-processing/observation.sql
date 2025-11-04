CREATE TABLE `/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post-processed/observation` AS
    SELECT distinct 
      o.observation_id,
      cast(rm.mspi as LONG) as person_id,
      o.observation_concept_id,
      to_date(o.observation_date) as observation_date,
      o.observation_datetime,
      o.observation_type_concept_id,
      o.value_as_string,
      o.value_as_number,
      o.value_as_concept_id,
      o.qualifier_concept_id,
      o.unit_concept_id,
      o.provider_id,
      o.visit_occurrence_id,
      o.visit_detail_id,
      o.observation_source_value,
      o.observation_source_concept_id,
      o.unit_source_value,
      o.qualifier_source_value,
--      rm.healthcare_site,
      map.data_partner_id

 --     oscn.concept_name as observation_source_concept_name,
 --     ocn.concept_name as observation_concept_name,
 --     otcn.concept_name as observation_type_concept_name,
 --     qcn.concept_name as qualifier_concept_name,
 --     ucn.concept_name as unit_concept_name,
 --     vcn.concept_name as value_as_concept_name
    -- FROM `ri.foundry.main.dataset.724fd1c6-8a5b-43f4-87d0-9c881b3a5892` o
-------    FROM `/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/uniquify_stage_1/observation` o
    FROM `ri.foundry.main.dataset.f8da6eeb-2179-464b-b928-f517e00710b2` o
    JOIN   `/All of Us-cdb223/HIN - HIE/sharedResources/FullyIdentiifed/ccda/ccda_response_metadata` rm
      ON o.filename = rm.response_file_path
    JOIN `/All of Us-cdb223/HIN - HIE/sharedResources/health_care_site_to_data_partner_id` map
      ON rm.healthcare_site = map.healthcare_site
--       JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` oscn
--         on oscn.concept_id = o.observation_source_concept_id 
--       JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` ocn
--         on ocn.concept_id = o.observation_concept_id
--       JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` otcn
--         on otcn.concept_id = o.observation_type_concept_id
--       JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` qcn
--         on qcn.concept_id = o.qualifier_concept_id
--       JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` ucn
--         on ucn.concept_id = o.unit_concept_id
--       JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` vcn
--         on vcn.concept_id = o.value_as_concept_id