with
    fonte_pedidos_itens AS (
        select
            cast(SALESORDERID AS int)                   AS SALESORDERID
            , cast(SALESORDERDETAILID AS int)           AS SALESORDERDETAILID
            , cast(ORDERQTY AS numeric(18,2))           AS qtd_produto
            , cast(PRODUCTID AS int)                    AS PRODUCTID
            , cast(SPECIALOFFERID AS int)               AS SPECIALOFFERID
            , cast(UNITPRICE AS numeric(18,4))          AS preco_unitario
            , cast(UNITPRICEDISCOUNT AS numeric(18,2))  AS desconto_perc
FROM {{ source('erp', 'SALESORDERDETAIL') }}
)
select *
from fonte_pedidos_itens
;