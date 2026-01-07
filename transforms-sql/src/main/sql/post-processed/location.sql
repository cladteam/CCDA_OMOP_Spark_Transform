CREATE TABLE `/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post-processed/location` AS
    SELECT distinct
        l.location_id,
        upper(l.address_1) as address_1,
        upper(l.address_2) as address_2,
        upper(l.city) as city,
        upper(l.state) as state,
        l.zip,
        upper(l.county) as county,
        l.location_source_value,
        l.data_partner_id
    FROM `ri.foundry.main.dataset.1ed07a69-4970-4b8d-b67c-e0557606ff33` l
