WITH

dim_pedido_vendas AS (
    SELECT *
    FROM {{ ref('int_pedido_vendas') }}
),

cz_pedido_vendas as (
    SELECT
	FK_PEDIDO
	, FK_CLIENTE
	, FK_VENDEDOR
	, FK_CARTAO
	, FK_ENDERECO
	, FK_ITEM_PEDIDO
	, FK_PRODUTO
    , SALESORDERID              AS "Cód. pedido"
	, TERRITORYID               AS "Cód. territorio"
	, SHIPMETHODID              AS "Cód. metodo de envio"
	, SPECIALOFFERID            AS "Cód. oferta especial"
	, REVISIONNUMBER            AS "Núm. revisao"
	, DT_PEDIDO                 AS "Dt. pedido"
	, STATUS_PEDIDO             AS "Status do pedido"
	, COD_STATUS_PEDIDO         AS "Cód. status do pedido"
	, TP_PEDIDO                 AS "Tp de pedido"
	, COD_TP_PEDIDO             AS "Cód. tp de pedido"
	, QTD_PRODUTO               AS "Qtde produto"
	, PRECO_UNITARIO            AS "Vlr. unitario"
	, DESCONTO_PERC             AS "Vlr. desc. percentual"
	, CHECK_SUBTOTAL            AS "Vlr. subtotal"
	, TAXAS                     AS "Vlr. taxas"
	, FRETE                     AS "Vlr. frete"
	, CHECK_TOTAL_PEDIDO        AS "Vlr. total pedido"
	, VALOR_NEGOCIADO           AS "Vlr. negociado"
	, VALOR_NEGOCIADO_LIQUIDO   AS "Vlr. negociado liq."
    from dim_pedido_vendas
)

SELECT * FROM cz_pedido_vendas