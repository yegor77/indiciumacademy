WITH

dim_produto AS (
    SELECT *
    FROM {{ ref('int_produto') }}
),

cz_produto as (
    SELECT
	FK_PRODUTO
	, NM_PRODUTO                        AS "Desc. produto"
	, COD_PRODUTO                       AS "Cód. produto"
	, SN_FABRICADO_PELA_EMPRESA         AS "Flag fab. pela empresa"
	, SN_PODE_SER_VENDIDO               AS "Flag pode ser vendido"
	, PRODUTO_COR                       AS "Desc. cor produto"
	, UNID_MEDIDA                       AS "Medidas do produto"
	, UNID_PESO                         AS "Peso do produto"
	, PROD_DIAS_FABRICACAO              AS "Qtde dias fabricacao"
	, LINHA_DO_PRODUTO                  AS "Linha do produto"
	, PROD_TAMANHO_CLASSE               AS "Desc. prod. tamanha classe"
	, PROD_ESTILO                       AS "Desc. prod. estilo"
	, PRODUCTSUBCATEGORYID              AS "Cód. subcateg. produto"
	, PRODUCTMODELID                    AS "Cód. modelo produto"
	, PRODUCTCATEGORYID                 AS "Cód. categoria produto"
	, NM_SUBCATEGORIA_PRODUTO           AS "Desc. subcategoria prod."
	, NM_CATEGORIA_PRODUTO              AS "Desc. categoria prod."
    from dim_produto
)

SELECT * FROM cz_produto