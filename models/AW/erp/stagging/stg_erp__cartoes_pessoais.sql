with
    fonte_cartoes_pessoais AS (
        select
            cast(BUSINESSENTITYID AS int)   AS BUSINESSENTITYID
            , cast(CREDITCARDID AS int)     AS CREDITCARDID
FROM {{ source('erp', 'PERSONCREDITCARD') }}
)

select * from fonte_cartoes_pessoais