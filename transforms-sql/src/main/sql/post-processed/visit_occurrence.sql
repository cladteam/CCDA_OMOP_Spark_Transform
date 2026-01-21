CREATE TABLE `/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post-processed/visit_occurrence` AS
    SELECT distinct
        vo.visit_occurrence_id,
        vo.person_id,
        vo.visit_concept_id,
        to_date(vo.visit_start_date) as visit_start_date,
        vo.visit_start_datetime,
        to_date(vo.visit_end_date) as visit_end_date,
        vo.visit_end_datetime,
        vo.visit_type_concept_id,
        vo.provider_id,
        vo.care_site_id,
        vo.visit_source_value,
        vo.visit_source_concept_id,
        vo.admitting_source_value,
        vo.admitting_source_concept_id,
        vo.discharge_to_source_value,
        vo.discharge_to_concept_id,
        vo.preceding_visit_occurrence_id,
        vo.data_partner_id

    FROM `ri.foundry.main.dataset.f3d88333-9315-4a11-963a-6703a72cfd8a` vo
    WHERE vo.visit_occurrence_id IS NOT NULL
      AND vo.person_id IS NOT NULL
      AND vo.visit_concept_id IS NOT NULL
      AND vo.visit_start_date IS NOT NULL
      AND vo.visit_end_date IS NOT NULL
      AND vo.visit_type_concept_id IS NOT NULL;

