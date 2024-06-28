WITH

dim_clientes AS (
    SELECT *
    FROM {{ ref('int_clientes') }}
),

cz_clientes as (
    SELECT
    FK_CLIENTE
    , STOREID           AS "Cód. da loja"
    , TERRITORYID       AS "Cód. territorio"
    , PERSONID          AS "Cód. pessoa"
    , TP_CLIENTE        AS "Tp de cliente"
    , NM_LOJA           AS "Nome da loja"
    from dim_clientes
)

SELECT * FROM cz_clientes