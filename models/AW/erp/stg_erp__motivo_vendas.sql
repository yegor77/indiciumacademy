    with
    fonte_motivo_vendas as (
        select
            cast(SALESREASONID as int)      AS SALESREASONID
            , cast(NAME as string)          AS desc_motivo_venda
            , cast(REASONTYPE as string)    AS tp_motivo_venda
FROM {{ source('erp', 'SALESREASON') }}
    )
select *
from fonte_motivo_vendas
;