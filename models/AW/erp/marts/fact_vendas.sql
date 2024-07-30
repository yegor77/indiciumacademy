 WITH

    cartoes as (
        select *
    FROM {{ ref('dim_cartoes') }}
    ),

        clientes as (
        select *
    FROM {{ ref('dim_clientes') }}
    ),

        enderecos as (
        select *
    FROM {{ ref('dim_enderecos') }}
    ),

        motivo_vendas as (
        select *
    FROM {{ ref('dim_motivo_vendas') }}
    ),

        produto as (
        select *
    FROM {{ ref('dim_produto') }}
    ),

        pedido_vendas as (
        select *
    FROM {{ ref('dim_pedido_vendas') }}
    ),
 
    joined as( 
    SELECT
     pedido_vendas."Dt. pedido"
    , pedido_vendas."Cód. pedido"
    , pedido_vendas."Status do pedido"
    , pedido_vendas."Cód. status do pedido"
    , pedido_vendas."Tp de pedido"
    , pedido_vendas."Cód. tp de pedido"
    , pedido_vendas."Qtde produto"
    , pedido_vendas."Vlr. unitario"
    , pedido_vendas."Vlr. total pedido"
    , pedido_vendas."Vlr. negociado"
    , pedido_vendas."Vlr. negociado liq."
    , produto."Desc. produto"
    , produto."Cód. produto"
    , produto."Flag fab. pela empresa"
    , produto."Flag pode ser vendido"
    , produto."Linha do produto"
    , produto."Desc. subcategoria prod."
    , produto."Desc. categoria prod."
    , cartoes."Desc. cartao"
    , cartoes."Nome da pessoa cartao"
    , cartoes."Cód. tp. pessoa cartao"
    , clientes."Nome da loja"
    , enderecos."Desc. cidade"
    , enderecos."Cód. estado"
    , enderecos."Cód. regiao pais"
    , enderecos."Flag pais sem estado"
    , enderecos."Desc. estado"
    , enderecos."Desc. pais"
from pedido_vendas
    LEFT JOIN produto on produto.FK_PRODUTO = pedido_vendas.FK_PRODUTO
    LEFT JOIN cartoes on cartoes.FK_CARTAO = pedido_vendas.FK_CARTAO
    LEFT JOIN clientes on clientes.FK_CLIENTE = pedido_vendas.FK_CLIENTE
    LEFT JOIN enderecos on enderecos.FK_ENDERECO = pedido_vendas.FK_ENDERECO
    )

    SELECT 
   *
    FROM joined