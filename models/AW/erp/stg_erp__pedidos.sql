with
    fonte_pedidos AS (
        select
            cast(SALESORDERID AS int)           AS SALESORDERID
            , cast(REVISIONNUMBER AS int)       AS REVISIONNUMBER
            , cast(ORDERDATE AS date)           AS dt_pedido
            , case
                when STATUS = 1 then 'Em processamento'
                when STATUS = 2 then 'Aprovado'
                when STATUS = 3 then 'Em espera'
                when STATUS = 4 then 'Rejeitado'
                when STATUS = 5 then 'Enviado'
                when STATUS = 6 then 'Cancelado'
            end                                 AS status_pedido
            , STATUS                            AS cod_status_pedido
            , case
                when ONLINEORDERFLAG = True then 'Pedido feito pelo cliente'
                when ONLINEORDERFLAG = False then 'Pedido feito por vendas'
            end                                 AS tp_pedido
            , case
                when ONLINEORDERFLAG = True then 1
                when ONLINEORDERFLAG = False then 0
            end                                 AS cod_tp_pedido 
            , cast(CUSTOMERID AS int)           AS CUSTOMERID
            , cast(SALESPERSONID as int)        AS SALESPERSONID
            , cast(TERRITORYID as int)          AS TERRITORYID
            , cast(SHIPMETHODID as int)         AS SHIPMETHODID
            , cast(CREDITCARDID as int)         AS CREDITCARDID
            , cast(SHIPTOADDRESSID as int)      AS ADDRESSID
            , cast(SUBTOTAL as numeric(18,2))   AS check_subtotal
            , cast(TAXAMT as numeric(18,2))     AS taxas
            , cast(FREIGHT as numeric(18,2))    AS frete
            , cast(TOTALDUE as numeric(18,2))   AS check_total_pedido
FROM {{ source('erp', 'SALESORDERHEADER') }}
)
select * from fonte_pedidos