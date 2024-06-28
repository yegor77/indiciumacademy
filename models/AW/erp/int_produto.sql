WITH

produtos AS (
    SELECT *
    FROM {{ ref('stg_erp__produtos') }}
),

categorias_produtos AS (
    SELECT *
    FROM {{ ref('stg_erp__categoria_produto') }}    
),

subcategoria_produtos AS (
    SELECT *
    FROM {{ ref('stg_erp__subcategoria_produto') }}    
),

    joined AS (
         select
             produtos.PRODUCTID
            , produtos.NM_PRODUTO
            , produtos.COD_PRODUTO
            , produtos.SN_FABRICADO_PELA_EMPRESA
            , produtos.SN_PODE_SER_VENDIDO
            , produtos.PRODUTO_COR
            , produtos.UNID_MEDIDA
            , produtos.UNID_PESO
            , produtos.PROD_DIAS_FABRICACAO
            , produtos.LINHA_DO_PRODUTO
            , produtos.PROD_TAMANHO_CLASSE
            , produtos.PROD_ESTILO
            , produtos.PRODUCTSUBCATEGORYID
            , produtos.PRODUCTMODELID
            , subcategoria_produtos.PRODUCTCATEGORYID
            , subcategoria_produtos.nm_subcategoria_produto
            , categorias_produtos.NM_CATEGORIA_PRODUTO
         from produtos
        left join subcategoria_produtos
        on produtos.PRODUCTSUBCATEGORYID = subcategoria_produtos.PRODUCTSUBCATEGORYID
        left join categorias_produtos
        on subcategoria_produtos.PRODUCTCATEGORYID = categorias_produtos.PRODUCTCATEGORYID
    )

    , chaves as (
SELECT
    hash(PRODUCTID)                  AS FK_produto
    , NM_PRODUTO
    , COD_PRODUTO
    , SN_FABRICADO_PELA_EMPRESA
    , SN_PODE_SER_VENDIDO
    , PRODUTO_COR
    , UNID_MEDIDA
    , UNID_PESO
    , PROD_DIAS_FABRICACAO
    , LINHA_DO_PRODUTO
    , PROD_TAMANHO_CLASSE
    , PROD_ESTILO
    , PRODUCTSUBCATEGORYID
    , PRODUCTMODELID
    , PRODUCTCATEGORYID
    , nm_subcategoria_produto
    , NM_CATEGORIA_PRODUTO
FROM joined
    )

select *
from chaves