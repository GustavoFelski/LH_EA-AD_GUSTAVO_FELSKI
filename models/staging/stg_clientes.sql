with
    source_data as (
        select
            cast(cod_cliente as int) as cod_cliente
            , cast(primeiro_nome as string) as txt_primeiro_nome
            , cast(ultimo_nome as string) as txt_ultimo_nome
            , cast(email as string) as txt_email
            , cast(tipo_cliente as string) as txt_tipo_cliente
            , date(data_inclusao) as dt_data_inclusao
            , replace(replace(cpfcnpj, '.', ''), '-', '') as cod_cpf_ou_cnpj
            , date(data_nascimento) as dt_data_nascimento
            , cast(endereco as string) as txt_endereco
            , replace(cep, '-', '') as cod_cep
        from {{ source('raw_data', 'clientes') }}
    )

select *
from source_data
