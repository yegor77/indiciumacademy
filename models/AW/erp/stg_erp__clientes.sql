with
    fonte_clientes as (
        select
            cast(CUSTOMERID as int)     AS CUSTOMERID
            , cast(PERSONID as int)     AS PERSONID
            , cast(STOREID as int)      AS STOREID
            , cast(TERRITORYID as int)  AS TERRITORYID
FROM {{ source('erp', 'CUSTOMER') }}
)

select * from fonte_clientes