WITH 
    fonte_categoria_produto AS (
    SELECT
        CAST(PRODUCTCATEGORYID AS INT) AS PRODUCTCATEGORYID,
        CASE
            WHEN NAME = 'Bikes' THEN 'Bicicletas'
            WHEN NAME = 'Components' THEN 'Componentes'
            WHEN NAME = 'Clothing' THEN 'Roupas'
            WHEN NAME = 'Accessories' THEN 'Acessórios'
        END AS nm_categoria_produto
    FROM {{ source('erp', 'PRODUCTCATEGORY') }}
)

select *
from fonte_categoria_produto
