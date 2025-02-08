with
    source_data as (
        select
            cast(cod_colaborador as int) as cod_colaborador
            , cast(cod_agencia as int) as cod_agencia
        from {{ source('raw_data', 'colaborador_agencia')}}
    )

select *
from source_data
