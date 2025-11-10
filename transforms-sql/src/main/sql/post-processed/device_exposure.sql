CREATE TABLE `/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post-processed/device_exposure` AS
    SELECT  distinct
      de.device_exposure_id, --
      cast(rm.mspi as LONG) as person_id, --
      de.device_concept_id, --
      to_date(de.device_exposure_start_date) as device_exposure_start_date, --
      de.device_exposure_start_datetime,--
      to_date(de.device_exposure_end_date) as device_exposure_end_date, --
      de.device_exposure_end_datetime, --
      de.unique_device_id, --
      de.device_type_concept_id, --
      de.quantity, --
      de.provider_id, --
      de.visit_occurrence_id, --
      de.visit_detail_id,  --
      de.device_source_value, --
      de.device_source_concept_id, --
      map.data_partner_id, --
      dcn.concept_name as device_concept_name,  --target_concept_id and target_concept_name
      dtcn.concept_name as device_type_concept_name,
      dscn.concept_name as device_source_concept_name
    FROM `ri.foundry.main.dataset.b1aa8bc7-106d-4234-b93b-061bf473cf80` de
    JOIN  `ri.foundry.main.dataset.672dd7ae-bbd4-43e8-9b8b-b5c7e8711e79` rm
      ON de.filename = rm.response_file_path
    JOIN `/All of Us-cdb223/HIN - HIE/sharedResources/health_care_site_to_data_partner_id` map
      ON rm.healthcare_site = map.healthcare_site
    LEFT JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` dcn
      ON dcn.concept_id = de.device_concept_id
    LEFT JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` dtcn
      ON dtcn.concept_id = de.device_type_concept_id
    LEFT JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` dscn
      ON dscn.concept_id = de.device_source_concept_id
     