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
      map.data_partner_id,
      sscn.concept_name as specialty_source_concept_name,
      gscn.concept_name as gender_source_concept_name,
      gcn.concept_name as gender_concept_name,
      scn.concept_name as specialty_concept_name
    FROM `ri.foundry.main.dataset.673b0f9d-76b9-4c03-8382-bb566451cd8e` p
    JOIN   `ri.foundry.main.dataset.672dd7ae-bbd4-43e8-9b8b-b5c7e8711e79` rm
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