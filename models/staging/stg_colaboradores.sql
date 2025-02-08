with
    source_data as (
        select
            cast(cod_colaborador as int) as cod_colaborador
            , cast(primeiro_nome as string) as txt_primeiro_nome
            , cast(ultimo_nome as string) as txt_ultimo_nome
            , cast(email as string) as txt_email
            , replace(replace(cpf, '.', ''), '-', '') as cod_cpf
            , date(data_nascimento) as dt_data_nascimento
            , cast(endereco as string) as txt_endereco
            , replace(cep, '-', '') as cod_cep
        from {{ source('raw_data', 'colaboradores')}}
    )

select *
from source_data
