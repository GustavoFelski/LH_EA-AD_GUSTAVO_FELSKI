with
    stgs_data as (
        select
            contas.num_conta
            , contas.cod_cliente
            , contas.cod_agencia
            , agencias.txt_tipo_agencia
            , contas.cod_colaborador
            , contas.txt_tipo_conta
            , contas.dt_data_abertura
            , contas.vlr_saldo_total
            , contas.vlr_saldo_disponivel
            , contas.dt_data_ultimo_lancamento
            , DATE_DIFF(CURRENT_DATE(), COALESCE(contas.dt_data_ultimo_lancamento, CURRENT_DATE()), DAY) AS num_dias_sem_movimentacao
            , contas.vlr_saldo_total - contas.vlr_saldo_disponivel as vlr_saldo_bloqueado
            , case
                when contas.vlr_saldo_total - contas.vlr_saldo_disponivel < 0 then 'Sim'
                else 'Não'
            end as flg_conta_saldo_negativo
        from {{ ref('stg_contas') }} as contas
        left join {{ ref('stg_agencias') }} as agencias
            on contas.cod_agencia = agencias.cod_agencia
    )

select *
from stgs_data
