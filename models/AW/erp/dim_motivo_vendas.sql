WITH

dim_motivo_vendas AS (
    SELECT *
    FROM {{ ref('int_motivo_vendas') }}
),

cz_motivo_vendas as (
    SELECT
    FK_PEDIDO
    , CD_PEDIDO             AS "Cód. pedido"
    , CD_MOTIVO_VENDA       AS "Cód. motivo venda"
    , DESC_MOTIVO_VENDA     AS "Desc. motivo venda"
    , TP_MOTIVO_VENDA       AS "Desc. Tipo de venda"
    from dim_motivo_vendas
)

SELECT * FROM cz_motivo_vendas