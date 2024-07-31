WITH

cartoes AS (
    SELECT *
    FROM {{ ref('stg_erp__cartoes') }}
),

cartoes_pessoais AS (
    SELECT *
    FROM {{ ref('stg_erp__cartoes_pessoais') }}    
),

pessoas AS (
    SELECT *
    FROM {{ ref('stg_erp__pessoas') }}    
),

joined AS (
    SELECT
        cartoes.CREDITCARDID
        ,cartoes.NM_CARTAO
        ,cartoes_pessoais.BUSINESSENTITYID
        ,pessoas.TIPO_PESSOA
        ,pessoas.IND_TIPO_PESSOA
        ,pessoas.NM_PESSOA
    FROM cartoes
    LEFT JOIN cartoes_pessoais
        ON cartoes.CREDITCARDID = cartoes_pessoais.CREDITCARDID
    LEFT JOIN pessoas
        ON cartoes_pessoais.BUSINESSENTITYID = pessoas.BUSINESSENTITYID
),

chaves AS (
    SELECT
        hash(CREDITCARDID)  AS FK_cartao,
        CREDITCARDID        AS cd_cartao,
        NM_CARTAO,
        BUSINESSENTITYID    AS cd_pessoa_cartao,
        TIPO_PESSOA         AS tp_pessoa_cartao,
        IND_TIPO_PESSOA     AS cod_tipo_pessoa_cartao,
        NM_PESSOA           AS nm_pessoa_cartao
    FROM joined

    UNION ALL

    SELECT
        hash(0)             AS FK_cartao,
        0                   AS cd_cartao,
        'Não cadastrado'    AS NM_CARTAO,
        NULL                AS cd_pessoa_cartao,
        NULL                AS tp_pessoa_cartao,
        NULL                AS cod_tipo_pessoa_cartao,
        'Não cadastrado'    AS nm_pessoa_cartao
),

dimensao_cartoes as (
    SELECT
     FK_CARTAO
    , CD_CARTAO                 AS "Cód. do cartao"
    , NM_CARTAO                 AS "Desc. cartao"
    , CD_PESSOA_CARTAO          AS "Cód. cartao pessoa"
    , TP_PESSOA_CARTAO          AS "Tp pessoa cartao"
    , COD_TIPO_PESSOA_CARTAO    AS "Cód. tp. pessoa cartao"
    , NM_PESSOA_CARTAO          AS "Nome da pessoa cartao"
    from chaves
)

SELECT * FROM dimensao_cartoes