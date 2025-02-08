with
    source_data as (
        select
            cast(cod_transacao as int) as cod_transacao
            , cast(num_conta as int) as num_conta
            , date(data_transacao) as dt_data_transacao
            , cast(nome_transacao as string) as txt_nome_transacao
            , valor_transacao as vlr_valor_transacao
        from{{ source('raw_data', 'transacoes') }}
    )

select *
from source_data
