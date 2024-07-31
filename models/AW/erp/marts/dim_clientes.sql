    with
    cliente as (
        select *
    FROM {{ ref('stg_erp__clientes') }}
    )

    , pessoas as (
        select *
    FROM {{ ref('stg_erp__pessoas') }}
    )

    , cliente_pessoa AS (
        SELECT 
    cliente.CUSTOMERID
    ,cliente.STOREID
    ,cliente.TERRITORYID
    ,cliente.PERSONID
    ,pessoas.TIPO_PESSOA               AS TP_CLIENTE
    ,pessoas.NM_PESSOA                 AS NM_LOJA
FROM 
    cliente
LEFT JOIN 
    pessoas
ON 
    cliente.PERSONID = pessoas.BUSINESSENTITYID
    )
    
, uniao_tabelas as(
SELECT
    hash(CUSTOMERID)  AS FK_cliente
    , STOREID
    , TERRITORYID
    , PERSONID
    , TP_CLIENTE
    , NM_LOJA   
FROM cliente_pessoa
),

dimensao_cliente as (
    SELECT
    FK_CLIENTE
    , STOREID           AS "Cód. da loja"
    , TERRITORYID       AS "Cód. territorio"
    , PERSONID          AS "Cód. pessoa"
    , TP_CLIENTE        AS "Tp de cliente"
    , NM_LOJA           AS "Nome da loja"
    from uniao_tabelas
)

SELECT * FROM dimensao_cliente