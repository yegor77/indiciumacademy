    with
    fonte_pessoas as (
        select
            cast(BUSINESSENTITYID as int)   AS BUSINESSENTITYID
            ,cast(PERSONTYPE as varchar)   AS ind_tipo_pessoa
            , case
                when PERSONTYPE = 'SC' then 'Contato da loja'
                when PERSONTYPE = 'IN' then 'Cliente individual (varejo)'
                when PERSONTYPE = 'SP' then 'Vendedor'
                when PERSONTYPE = 'EM' then 'Funcionário (não vendedor)'
                when PERSONTYPE = 'VC' then 'Contato do fornecedor'
                when PERSONTYPE = 'GC' then 'Contato geral'
            end as tipo_pessoa
            , case 
                when MIDDLENAME is not null then 
                    cast(FIRSTNAME as string)||' '||cast(MIDDLENAME as string)||' '||cast(LASTNAME as string)
                else
                    cast(FIRSTNAME as string)||' '||cast(LASTNAME as string)
            end as nm_pessoa
FROM {{ source('erp', 'PERSON') }}
)
select *
from fonte_pessoas
;