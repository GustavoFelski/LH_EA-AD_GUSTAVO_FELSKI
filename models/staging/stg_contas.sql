with
    source_data as (
        select
            cast(num_conta as int) as num_conta
            , cast(cod_cliente as int) as cod_cliente
            , cast(cod_agencia as int) as cod_agencia
            , cast(cod_colaborador as int) as cod_colaborador
            , cast(tipo_conta as string) as txt_tipo_conta
            , date(data_abertura) as dt_data_abertura
            , saldo_total as vlr_saldo_total
            , saldo_disponivel as vlr_saldo_disponivel
            , date(data_ultimo_lancamento) as dt_data_ultimo_lancamento
        from {{ source('raw_data', 'contas') }}
    )

select *
from source_data
