CREATE TABLE `ri.foundry.main.dataset.310b521c-c56e-453c-a135-fdf6c50efc6b` AS
    SELECT distinct 
      po.procedure_occurrence_id,
      cast(rm.mspi as LONG) as person_id,
      po.procedure_concept_id,
      to_date(po.procedure_date) as procedure_date,
      po.procedure_datetime,
      po.procedure_type_concept_id,
      po.modifier_concept_id,
      po.quantity,
      po.provider_id,
      po.visit_occurrence_id,
      po.visit_detail_id,
      po.procedure_source_value,
      po.procedure_source_concept_id,
      po.modifier_source_value,
--       rm.healthcare_site,
      map.data_partner_id

 --     pcn.concept_name as procedure_concept_name,
 --     ptcn.concept_name as procedure_type_concept_name,
 --     pscn.concept_name as procedure_source_concept_name,
 --     mcn.concept_name as modifier_concept_name
    --FROM `ri.foundry.main.dataset.503e0eda-6b31-46cc-94a5-b4f16c20be65` po
    FROM `/All of Us-cdb223/HIN - HIE/CCDA/datasets/uniquify_stage_1/procedure_occurrence` po
    JOIN   `/All of Us-cdb223/HIN - HIE/sharedResources/FullyIdentiifed/ccda/ccda_response_metadata` rm
      ON po.filename = rm.response_file_path
    JOIN  `/All of Us-cdb223/HIN - HIE/sharedResources/health_care_site_to_data_partner_id` map
      ON rm.healthcare_site = map.healthcare_site
--    JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` pcn
--      ON pcn.concept_id = po.procedure_concept_id
--    JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` ptcn
--      ON ptcn.concept_id = po.procedure_type_concept_id
--    JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` pscn
--      ON pscn.concept_id = po.procedure_source_concept_id
--    JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` mcn
--      ON mcn.concept_id =  po.modifier_concept_id