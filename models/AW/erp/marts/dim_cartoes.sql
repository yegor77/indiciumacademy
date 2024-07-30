WITH

dim_cartoes AS (
    SELECT *
    FROM {{ ref('int_cartoes') }}
),

cz_cartoes as (
    SELECT
     FK_CARTAO
    , CD_CARTAO                 AS "Cód. do cartao"
    , NM_CARTAO                 AS "Desc. cartao"
    , CD_PESSOA_CARTAO          AS "Cód. cartao pessoa"
    , TP_PESSOA_CARTAO          AS "Tp pessoa cartao"
    , COD_TIPO_PESSOA_CARTAO    AS "Cód. tp. pessoa cartao"
    , NM_PESSOA_CARTAO          AS "Nome da pessoa cartao"
    from dim_cartoes
)

SELECT * FROM cz_cartoes