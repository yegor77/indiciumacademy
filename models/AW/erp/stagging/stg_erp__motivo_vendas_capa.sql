with
    fonte_motivo_vendas_capa AS (
        select
            cast(SALESREASONID AS int)      AS SALESREASONID
            , cast(SALESORDERID AS int)     AS SALESORDERID
FROM {{ source('erp', 'SALESORDERHEADERSALESREASON') }}
    )
    
select * from fonte_motivo_vendas_capa