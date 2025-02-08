with
    stg_data as (
        select
            cod_agencia
            , txt_nome
            , txt_endereco
            , txt_cidade
            , txt_uf
            , dt_data_abertura
            , txt_tipo_agencia
        from {{ ref('stg_agencias') }}
    )

select *
from stg_data
