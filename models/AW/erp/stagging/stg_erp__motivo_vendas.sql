with
    fonte_motivo_vendas AS (
        select
            cast(SALESREASONID AS int)      AS SALESREASONID
            , cast(NAME AS string)          AS desc_motivo_venda
            , cast(REASONTYPE AS string)    AS tp_motivo_venda
FROM {{ source('erp', 'SALESREASON') }}
    )
    
select * from fonte_motivo_vendas