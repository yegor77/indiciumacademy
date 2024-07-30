with
    fonte_cartoes AS (
        select
                cast(CREDITCARDID as int)   AS CREDITCARDID
            , cast(CARDTYPE as string)      AS nm_cartao
FROM {{ source('erp', 'CREDITCARD') }}
)
select * from fonte_cartoes