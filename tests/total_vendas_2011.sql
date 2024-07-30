--with
--    vendas_em_2011 as (
--    SELECT SUM(VALOR_NEGOCIADO) AS vendas_brutas
--    FROM {{ ref('int_pedido_vendas') }}
--        where DT_PEDIDO between '2011-01-01' and '2011-12-31'
--    )
--select *
--from vendas_em_2011
--where vendas_brutas not between 12646112.1 and 12646112.2

with
    vendas_em_2011 as (
    SELECT SUM("Vlr. negociado") AS vendas_brutas
    FROM {{ ref('fact_vendas') }}
        where "Dt. pedido" between '2011-01-01' and '2011-12-31'
    )
select *
from vendas_em_2011
where vendas_brutas not between 12646112.1 and 12646112.2