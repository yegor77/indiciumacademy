 with
    fonte_estados AS (
        select
            cast(STATEPROVINCEID AS int)                AS STATEPROVINCEID
            , cast(TERRITORYID AS int)                  AS TERRITORYID
            , cast(STATEPROVINCECODE AS string)         AS cd_estado
            , cast(COUNTRYREGIONCODE AS string)         AS COUNTRYREGIONCODE
            , cast(ISONLYSTATEPROVINCEFLAG AS string)   AS sn_pais_sem_estado
            , cast(NAME AS string)                      AS nm_estado
FROM {{ source('erp', 'STATEPROVINCE') }}
    )
    
select * from fonte_estados
