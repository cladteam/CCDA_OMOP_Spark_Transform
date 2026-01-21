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
      p.data_partner_id,
      sscn.concept_name as specialty_source_concept_name,
      gscn.concept_name as gender_source_concept_name,
      gcn.concept_name as gender_concept_name,
      scn.concept_name as specialty_concept_name
    FROM `ri.foundry.main.dataset.673b0f9d-76b9-4c03-8382-bb566451cd8e` p
LEFT    JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` sscn
      ON sscn.concept_id = p.specialty_source_concept_id
LEFT    JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` gscn
      ON gscn.concept_id = p.gender_source_concept_id
LEFT    JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` gcn
      ON gcn.concept_id = p.gender_concept_id
LEFT    JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` scn
      ON scn.concept_id = p.specialty_concept_id
    WHERE p.provider_id IS NOT NULL;