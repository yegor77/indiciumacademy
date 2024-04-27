with
    produtos as (
        select *
        from {{ ref('stg_erp__produtos') }}
    )
    , categorias as (
        select *
        from {{ ref('stg_erp__categorias') }}
    )
    , fornecedores as (
        select *
        from {{ ref('stg_erp__fornecedores') }}
    )
    , joined as (
        select
            produtos.PK_PRODUTO
            , produtos.NM_PRODUTO
            , produtos.QUANTIDADE_POR_UNIDADE
            , produtos.PRECO_POR_UNIDADE
            , produtos.UNIDADE_EM_ESTOQUE
            , produtos.UNIDADE_POR_PEDIDO
            , produtos.NIVEL_DE_PEDIDO
            , produtos.IS_DISCONTINUADO
            , categorias.NM_CATEGORIA
            , categorias.DESCRICAO_CATEGORIA
            , fornecedores.NM_FORNECEDOR
            , fornecedores.CIDADE_FORNECEDOR
            , fornecedores.PAIS_FORNECEDOR
        from produtos
        left join categorias on produtos.fk_categoria = categorias.pk_categoria
        left join fornecedores on produtos.fk_fornecedor = fornecedores.pk_fornecedor
    )
select *
from joined