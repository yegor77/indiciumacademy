with
    fonte_paises AS (
        select
            cast(COUNTRYREGIONCODE AS string)   AS COUNTRYREGIONCODE
            , cast(NAME AS string)              AS nm_pais
FROM {{ source('erp', 'COUNTRYREGION') }}
)
select *
from fonte_paises
;