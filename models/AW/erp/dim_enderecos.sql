WITH

dim_enderecos AS (
    SELECT *
    FROM {{ ref('int_enderecos') }}
),

cz_endereco as (
    SELECT
    FK_ENDERECO
    , DESC_CIDADE           AS "Desc. cidade"
    , CD_ESTADO             AS "Cód. estado"
    , COUNTRYREGIONCODE     AS "Cód. regiao pais"
    , SN_PAIS_SEM_ESTADO    AS "Flag pais sem estado"
    , NM_ESTADO             AS "Desc. estado"
    , TERRITORYID           AS "Cód. territorio"
    , NM_PAIS               AS "Desc. pais"
    from dim_enderecos
)

SELECT * FROM cz_endereco