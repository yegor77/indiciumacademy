with
    motivo_vendas_capa as (
        select *
    FROM {{ ref('stg_erp__motivo_vendas_capa') }}    
    )

    , motivo_vendas as (
        select *
    FROM {{ ref('stg_erp__motivo_vendas') }}    
    )

    , uniao_tabelas as (
        select
            motivo_vendas_capa.SALESORDERID
            , motivo_vendas_capa.SALESREASONID
            , motivo_vendas.DESC_MOTIVO_VENDA
            , motivo_vendas.TP_MOTIVO_VENDA
        from motivo_vendas_capa
        left join motivo_vendas
        on motivo_vendas_capa.SALESREASONID = motivo_vendas.SALESREASONID
    )

    , chaves as (
        select
            hash(SALESORDERID) as fk_pedido
            , SALESORDERID as cd_pedido
            , SALESREASONID as cd_motivo_venda
            , DESC_MOTIVO_VENDA
            , TP_MOTIVO_VENDA
        from uniao_tabelas
    )

select *
from chaves