    with
    fonte_produtos as (
        select
            cast(PRODUCTID as int)              AS PRODUCTID
            , cast(NAME as string)              AS nm_produto
            , cast(PRODUCTNUMBER as string)     AS cod_produto
            , MAKEFLAG                          AS sn_fabricado_pela_empresa
            , FINISHEDGOODSFLAG                 AS sn_pode_ser_vendido
            , case
                when COLOR = 'Black'        then 'Preto'
                when COLOR = 'Red'          then 'Vermelho'
                when COLOR = 'White'        then 'Branco'
                when COLOR = 'Blue'         then 'Azul'
                when COLOR = 'Multi'        then 'Colorido'
                when COLOR = 'Yellow'       then 'Amarelo'
                when COLOR = 'Grey'         then 'Cinza'
                when COLOR = 'Silver/Black' then 'Prata/ Preto'
                when COLOR = 'Silver'       then 'Prata'
                else 'N/A'
            end                                 AS produto_cor
            , case
                when SIZEUNITMEASURECODE is null and SIZE is null then 'Não se aplica'
                when SIZEUNITMEASURECODE is null then cast(SIZE as string)
                else SIZE||' '||SIZEUNITMEASURECODE
            end                                 AS unid_medida
            , case
                when WEIGHT is not null then WEIGHT||' '||WEIGHTUNITMEASURECODE
                else 'Não se aplica'
            end                                 AS unid_peso
            , cast(DAYSTOMANUFACTURE as int)    AS prod_dias_fabricacao
            , case
                when PRODUCTLINE = 'R' then 'Estrada'
                when PRODUCTLINE = 'M' then 'Montanha'
                when PRODUCTLINE = 'T' then 'Passeio'
                when PRODUCTLINE = 'S' then 'Padrão'
                else 'Não se aplica'
            end                                 AS linha_do_produto
            , case
                when CLASS = 'H' then 'Grande'
                when CLASS = 'M' then 'Médio'
                when CLASS = 'L' then 'Pequeno'
                else 'Não se aplica'
            end                                 AS prod_tamanho_classe
            , case
                when STYLE = 'W' then 'Feminino'
                when STYLE = 'M' then 'Masculino'
                when STYLE = 'U' then 'Unisex'
                else 'Não se aplica'
            end                                 AS prod_estilo
            , cast(PRODUCTSUBCATEGORYID as int) AS PRODUCTSUBCATEGORYID
            , cast(PRODUCTMODELID as int)       AS PRODUCTMODELID
FROM {{ source('erp', 'PRODUCT') }}
)
select *
from fonte_produtos
;