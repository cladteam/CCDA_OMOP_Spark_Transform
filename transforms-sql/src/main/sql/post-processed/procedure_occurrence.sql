CREATE TABLE `/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post-processed/procedure_occurrence` AS
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
      map.data_partner_id
 --     pcn.concept_name as procedure_concept_name,
 --     ptcn.concept_name as procedure_type_concept_name,
 --     pscn.concept_name as procedure_source_concept_name,
 --     mcn.concept_name as modifier_concept_name
    FROM `ri.foundry.main.dataset.e328942e-bd13-43f6-9ab0-21a6ebac16cd` po
    JOIN   `ri.foundry.main.dataset.672dd7ae-bbd4-43e8-9b8b-b5c7e8711e79` rm
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
--      ON mcn.concept_id =  po.modifier_concept_i`