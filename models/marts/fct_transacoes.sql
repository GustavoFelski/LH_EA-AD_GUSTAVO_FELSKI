with
    stgs_data as (
        select
            transacoes.cod_transacao
            , transacoes.num_conta
            , contas.txt_tipo_conta
            , contas.cod_cliente
            , contas.cod_agencia
            , contas.txt_tipo_agencia
            , transacoes.dt_data_transacao
            , transacoes.txt_nome_transacao
            , transacoes.vlr_valor_transacao
            , case
                when cast(substr(cast(transacoes.dt_data_transacao as string), 9, 2) as int) < 16 then 'início do mês'
                else 'fim do mês'
            end as flg_periodo_mes
            , case 
                when vlr_valor_transacao > 0 then 'vlr_valor_recebido'
                when vlr_valor_transacao < 0 then 'vlr_valor_retirado'
                else 'neutro'
            end as flg_tipo_transacao
            , coalesce(case when vlr_valor_transacao > 0 then vlr_valor_transacao end, 0) as vlr_valor_recebido
            , coalesce(case when vlr_valor_transacao < 0 then abs(vlr_valor_transacao) end, 0) as vlr_valor_retirado
        from {{ ref('stg_transacoes') }} as transacoes
        left join {{ ref('dim_contas') }} as contas
            on transacoes.num_conta = contas.num_conta
    )

select *
from stgs_data
