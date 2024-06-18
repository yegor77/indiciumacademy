    with
    fonte_funcionarios as (
        select
            cast(BUSINESSENTITYID AS int)           AS BUSINESSENTITYID
            , cast(JOBTITLE AS string)              AS funcionario_cargo
            , cast(BIRTHDATE AS date)               AS dt_nasc_funcionario
            , case
                when MARITALSTATUS = 'S' then 'Solteiro(a)'
                when MARITALSTATUS = 'M' then 'Casado(a)'
            end                                     AS desc_estado_civil
            , case
                when GENDER = 'M' then 'Masculino'
                when GENDER = 'F' then 'Feminino'
            end                                     AS desc_genero
            , cast(HIREDATE AS date)                AS dt_contratacao
            , SALARIEDFLAG                          AS sn_assalariado
            , CURRENTFLAG                           AS sn_ativo
            , ORGANIZATIONNODE                      AS nivel_organizacional
            , split_part(ORGANIZATIONNODE,'/',2)    AS nivel_2
            , split_part(ORGANIZATIONNODE,'/',3)    AS nivel_3
            , split_part(ORGANIZATIONNODE,'/',4)    AS nivel_4
            , split_part(ORGANIZATIONNODE,'/',5)    AS nivel_5
            FROM {{ source('erp', 'EMPLOYEE') }}
)
select *
from fonte_funcionarios
;