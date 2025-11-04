CREATE TABLE `/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post-processed/provider` AS
    SELECT distinct 
      p.provider_id,
      p.provider_name,
      p.npi,
      p.dea,
      p.specialty_concept_id,
      p.care_site_id,
      p.year_of_birth,
      p.gender_concept_id,
      p.provider_source_value,
      p.specialty_source_value,
      p.specialty_source_concept_id,
      p.gender_source_value,
      p.gender_source_concept_id,
--      rm.healthcare_site,
      map.data_partner_id,

      sscn.concept_name as specialty_source_concept_name,
      gscn.concept_name as gender_source_concept_name,
      gcn.concept_name as gender_concept_name,
      scn.concept_name as specialty_concept_name
    -- FROM `ri.foundry.main.dataset.f9048d29-0f7f-4f43-8287-2d25e080ccf8` p
    FROM `ri.foundry.main.dataset.66681671-e913-44b5-bd1f-1d6067165972` p
    JOIN   `/All of Us-cdb223/HIN - HIE/sharedResources/FullyIdentiifed/ccda/ccda_response_metadata` rm
      ON p.filename = rm.response_file_path
    JOIN  `/All of Us-cdb223/HIN - HIE/sharedResources/health_care_site_to_data_partner_id` map
      ON rm.healthcare_site = map.healthcare_site
LEFT    JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` sscn
      ON sscn.concept_id = p.specialty_source_concept_id
LEFT    JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` gscn
      ON gscn.concept_id = p.gender_source_concept_id
LEFT    JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` gcn
      ON gcn.concept_id = p.gender_concept_id
LEFT    JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` scn
      ON scn.concept_id = p.specialty_concept_id