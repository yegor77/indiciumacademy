   with
    estados as (
        select *
    FROM {{ ref('stg_erp__estados') }}
    )

    , cidades as (
        select *
    FROM {{ ref('stg_erp__enderecos') }}
    )

    , paises as (
        select *
    FROM {{ ref('stg_erp__paises') }}
    )

    , uniao_tabelas as (
        select
            cidades.ADDRESSID
            , cidades.DESC_CIDADE
            , estados.CD_ESTADO
            , estados.COUNTRYREGIONCODE
            , estados.SN_PAIS_SEM_ESTADO
            , estados.NM_ESTADO
            , estados.TERRITORYID
            , paises.NM_PAIS
        from cidades
        left join estados
        on cidades.STATEPROVINCEID = estados.STATEPROVINCEID
        left join paises
        on estados.COUNTRYREGIONCODE = paises.COUNTRYREGIONCODE
    )

, chaves as (
    select
            hash(ADDRESSID)     AS FK_endereco
            ,DESC_CIDADE
            ,CD_ESTADO
            ,COUNTRYREGIONCODE
            ,SN_PAIS_SEM_ESTADO
            ,NM_ESTADO
            ,TERRITORYID
            ,NM_PAIS
FROM uniao_tabelas
),

dimensao_endereco as (
    SELECT
    FK_ENDERECO
    , DESC_CIDADE           AS "Desc. cidade"
    , CD_ESTADO             AS "Cód. estado"
    , COUNTRYREGIONCODE     AS "Cód. regiao pais"
    , SN_PAIS_SEM_ESTADO    AS "Flag pais sem estado"
    , NM_ESTADO             AS "Desc. estado"
    , TERRITORYID           AS "Cód. territorio"
    , NM_PAIS               AS "Desc. pais"
    from chaves
)

SELECT * FROM dimensao_endereco