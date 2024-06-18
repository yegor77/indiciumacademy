    with
    fonte_vendedores AS (
        select
            cast(BUSINESSENTITYID AS int)           AS BUSINESSENTITYID
            , cast(TERRITORYID AS int)              AS TERRITORYID
            , cast(SALESQUOTA AS numeric(18,2))     AS meta_venda
            , cast(BONUS AS numeric(18,2))          AS bonus_meta
            , cast(COMMISSIONPCT AS numeric(18,2))  AS comicao_perc
            , cast(SALESYTD AS numeric(18,2))       AS vendas_YTD
            , cast(SALESLASTYEAR AS numeric(18,2))  AS vendas_LY
FROM {{ source('erp', 'SALESPERSON') }}
)
select *
from fonte_vendedores
;
