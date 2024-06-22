WITH

pedidos_capa AS (
    SELECT *
    FROM {{ ref('stg_erp__pedidos') }}
),

pedidos_item AS (
    SELECT *
    FROM {{ ref('stg_erp__pedidos_item') }}
)

, uniao_tabelas as (
        select
pedidos_capa.SALESORDERID
, pedidos_capa.REVISIONNUMBER
, pedidos_capa.DT_PEDIDO
, pedidos_capa.STATUS_PEDIDO
, pedidos_capa.COD_STATUS_PEDIDO
, pedidos_capa.TP_PEDIDO
, pedidos_capa.COD_TP_PEDIDO
, pedidos_capa.CUSTOMERID
, pedidos_capa.SALESPERSONID
, pedidos_capa.TERRITORYID
, pedidos_capa.SHIPMETHODID
, pedidos_capa.CREDITCARDID
, pedidos_capa.ADDRESSID
, pedidos_capa.CHECK_SUBTOTAL
, pedidos_capa.TAXAS
, pedidos_capa.FRETE
, pedidos_capa.CHECK_TOTAL_PEDIDO
, pedidos_item.SALESORDERDETAILID
, pedidos_item.QTD_PRODUTO
, pedidos_item.PRODUCTID
, pedidos_item.SPECIALOFFERID
, pedidos_item.PRECO_UNITARIO
, pedidos_item.DESCONTO_PERC

        from pedidos_item
        left join pedidos_capa
        on pedidos_item.SALESORDERID = pedidos_capa.SALESORDERID
    )

, tabela_full AS (
    SELECT
        hash(SALESORDERID)            AS fk_pedido
        , hash(CUSTOMERID)            AS fk_cliente
        , hash(SALESPERSONID)         AS fk_vendedor
        , hash(CREDITCARDID)          AS fk_cartao
        , hash(ADDRESSID)             AS fk_endereco
        , hash(SALESORDERDETAILID)    AS pk_item_pedido
        , hash(PRODUCTID)             AS fk_produto
        , TERRITORYID
        , SHIPMETHODID
        , SPECIALOFFERID
        , REVISIONNUMBER
        , DT_PEDIDO
        , STATUS_PEDIDO
        , COD_STATUS_PEDIDO
        , TP_PEDIDO
        , COD_TP_PEDIDO
        , QTD_PRODUTO
        , PRECO_UNITARIO
        , DESCONTO_PERC
        , CHECK_SUBTOTAL
        , TAXAS
        , FRETE
        , CHECK_TOTAL_PEDIDO
        , PRECO_UNITARIO * QTD_PRODUTO                         AS valor_negociado
        , (PRECO_UNITARIO * QTD_PRODUTO) * (1 - DESCONTO_PERC) AS valor_negociado_liquido
    FROM uniao_tabelas
)

select *
from tabela_full