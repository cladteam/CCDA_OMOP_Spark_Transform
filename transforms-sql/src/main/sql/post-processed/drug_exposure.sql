CREATE TABLE `/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post-processed/drug_exposure` AS
    SELECT  distinct
      de.drug_exposure_id, --
      cast(rm.mspi as LONG) as person_id, --
      de.drug_concept_id, --
      to_date(de.drug_exposure_start_date) as drug_exposure_start_date, --
      de.drug_exposure_start_datetime,--
      to_date(de.drug_exposure_end_date) as drug_exposure_end_date, --
      de.drug_exposure_end_datetime, --
      de.verbatim_end_date, --
      de.drug_type_concept_id, --
      de.stop_reason, --
      de.refills, --
      de.quantity, --
      de.days_supply, --
      de.sig, --
      de.route_concept_id, --
      de.lot_number, --
      de.provider_id, --
      de.visit_occurrence_id, --
      de.visit_detail_id,  --
      de.drug_source_value, --
      de.drug_source_concept_id, --
      de.route_source_value, --
      de.dose_unit_source_value, --
      map.data_partner_id, --
     dcn.concept_name as drug_concept_name,  --target_concept_id and target_concept_name
     dtcn.concept_name as drug_type_concept_name,
     rcn.concept_name as route_concept_name,
     dscn.concept_name as drug_source_concept_name
    FROM  `ri.foundry.main.dataset.efbc1e75-a650-469e-91e8-ece4ba38a376` de
    JOIN   `ri.foundry.main.dataset.672dd7ae-bbd4-43e8-9b8b-b5c7e8711e79` rm
      ON de.filename = rm.response_file_path
    JOIN  `/All of Us-cdb223/HIN - HIE/sharedResources/health_care_site_to_data_partner_id` map
      ON rm.healthcare_site = map.healthcare_site
LEFT    JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` dcn
      ON dcn.concept_id = de.drug_concept_id
LEFT    JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` dtcn
      ON dtcn.concept_id = de.drug_type_concept_id
LEFT    JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` rcn
      ON rcn.concept_id = de.route_concept_id
LEFT    JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` dscn
      ON dscn.concept_id = de.drug_source_concept_id
     