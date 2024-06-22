WITH 
    fonte_subcategoria_produto AS (
    SELECT
            cast(PRODUCTSUBCATEGORYID as int)
                as PRODUCTSUBCATEGORYID
            , cast(PRODUCTCATEGORYID as int)
                as PRODUCTCATEGORYID
            ,CASE
        WHEN Name = 'Mountain Bikes' THEN 'Bicicletas de Montanha'
        WHEN Name = 'Road Bikes' THEN 'Bicicletas de Estrada'
        WHEN Name = 'Touring Bikes' THEN 'Bicicletas de Turismo'
        WHEN Name = 'Handlebars' THEN 'Guidões'
        WHEN Name = 'Bottom Brackets' THEN 'Movimentos Centrais'
        WHEN Name = 'Brakes' THEN 'Freios'
        WHEN Name = 'Chains' THEN 'Correntes'
        WHEN Name = 'Cranksets' THEN 'Pedivelas'
        WHEN Name = 'Derailleurs' THEN 'Câmbios Dianteiros e Traseiros'
        WHEN Name = 'Forks' THEN 'Garfos'
        WHEN Name = 'Headsets' THEN 'Caixas de Direção'
        WHEN Name = 'Mountain Frames' THEN 'Quadros para Mountain Bike'
        WHEN Name = 'Pedals' THEN 'Pedais'
        WHEN Name = 'Road Frames' THEN 'Quadros para Bicicleta de Estrada'
        WHEN Name = 'Saddles' THEN 'Selins'
        WHEN Name = 'Touring Frames' THEN 'Quadros para Bicicleta de Turismo'
        WHEN Name = 'Wheels' THEN 'Rodas'
        WHEN Name = 'Bib-Shorts' THEN 'Calções com Alças'
        WHEN Name = 'Caps' THEN 'Bonés'
        WHEN Name = 'Gloves' THEN 'Luvas'
        WHEN Name = 'Jerseys' THEN 'Camisas de Ciclismo'
        WHEN Name = 'Shorts' THEN 'Calções'
        WHEN Name = 'Socks' THEN 'Meias'
        WHEN Name = 'Tights' THEN 'Calças Justas'
        WHEN Name = 'Vests' THEN 'Colete'
        WHEN Name = 'Bike Racks' THEN 'Suportes para Bicicletas'
        WHEN Name = 'Bike Stands' THEN 'Suportes para Bicicleta'
        WHEN Name = 'Bottles and Cages' THEN 'Garrafas e Suportes'
        WHEN Name = 'Cleaners' THEN 'Produtos de Limpeza'
        WHEN Name = 'Fenders' THEN 'Para-lamas'
        WHEN Name = 'Helmets' THEN 'Capacetes'
        WHEN Name = 'Hydration Packs' THEN 'Mochilas de Hidratação'
        WHEN Name = 'Lights' THEN 'Faróis'
        WHEN Name = 'Locks' THEN 'Travas'
        WHEN Name = 'Panniers' THEN 'Bolsas de Bicicleta'
        WHEN Name = 'Pumps' THEN 'Bombas de Ar'
        WHEN Name = 'Tires and Tubes' THEN 'Pneus e Câmaras de Ar'
        ELSE Name end as nm_subcategoria_produto
    FROM {{ source('erp', 'PRODUCTSUBCATEGORY') }}
)

select *
from fonte_subcategoria_produto
