CREATE TABLE `/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post-processed/person` AS
SELECT distinct 
        cast(rm.mspi as LONG) as person_id,
        rm.mspi as global_person_id,
        gender_concept_id,
        year_of_birth,
        month_of_birth,
        day_of_birth,
        birth_datetime, 
        race_concept_id, 
        ethnicity_concept_id, 
        location_id, 
        provider_id,
        care_site_id, 
        person_source_value,
        gender_source_value, 
        gender_source_concept_id,
        race_source_value,
        race_source_concept_id,
        ethnicity_source_value,
        ethnicity_source_concept_id,
        map.data_partner_id -- 21
 --       gcn.concept_name as gender_concept_name,
 --       gscn.concept_name as gender_source_concept_name,
 --       ecn.concept_name as ethnicity_concept_name,
 --       escn.concept_name as ethnicity_source_concept_name,
 --       rcn.concept_name as race_concept_name,
 --       rscn.concept_name as race_source_concept_name
    FROM `ri.foundry.main.dataset.fa4869f2-c211-4437-8af8-6b284bea126c` p
    JOIN   `ri.foundry.main.dataset.672dd7ae-bbd4-43e8-9b8b-b5c7e8711e79` rm
      ON p.filename = rm.response_file_path
    JOIN  `/All of Us-cdb223/HIN - HIE/sharedResources/health_care_site_to_data_partner_id` map
      ON map.healthcare_site = rm.healthcare_site
--    JOIN `/All of Us-cdb223/Data Source: Athena Vocabularies/datasets/concept`  gcn
--      on p.gender_concept_id = gcn.concept_id
--    JOIN `/All of Us-cdb223/Data Source: Athena Vocabularies/datasets/concept`  gscn
--      on p.gender_source_concept_id = gscn.concept_id 
--    JOIN `/All of Us-cdb223/Data Source: Athena Vocabularies/datasets/concept`  ecn
--      on p.ethnicity_concept_id = ecn.concept_id 
--    JOIN `/All of Us-cdb223/Data Source: Athena Vocabularies/datasets/concept`  escn
--      on p.ethnicity_source_concept_id = escn.concept_id 
--    JOIN `/All of Us-cdb223/Data Source: Athena Vocabularies/datasets/concept`  rcn
--      on p.race_concept_id = rcn.concept_id
--    JOIN `/All of Us-cdb223/Data Source: Athena Vocabularies/datasets/concept`  rscn
--      on p.race_source_concept_id = rscn.concept_id 
     