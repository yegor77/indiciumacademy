    with
    fonte_enderecos AS (
        select
            cast(ADDRESSID AS int)                AS ADDRESSID
            , cast(CITY AS string)                AS desc_cidade
            , cast(STATEPROVINCEID AS int)        AS STATEPROVINCEID
            , cast(POSTALCODE AS string)          AS cod_postal
FROM {{ source('erp', 'ADDRESS') }}
    )
select *
from fonte_enderecos
