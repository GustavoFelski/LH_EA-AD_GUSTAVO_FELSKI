with
    source_data as (
        select
            cast(cod_agencia as int) as cod_agencia
            , cast(nome as string) as txt_nome
            , cast(endereco as string) as txt_endereco
            , cast(cidade as string) as txt_cidade
            , cast(uf as string) as txt_uf
            , date(data_abertura) as dt_data_abertura
            , cast(tipo_agencia as string) as txt_tipo_agencia
        from {{ source('raw_data', 'agencias') }}
    )

select *
from source_data
