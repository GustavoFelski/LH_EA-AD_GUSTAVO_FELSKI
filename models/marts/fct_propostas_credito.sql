with
    stgs_data as (
        select
            propostas.cod_proposta
            , propostas.cod_cliente
            , propostas.cod_colaborador
            , colaboradores.cod_agencia as cod_agencia_colaborador
            , contas.cod_agencia as cod_agencia_cliente
            , propostas.dt_data_entrada_proposta
            , propostas.num_taxa_juros_mensal
            , propostas.vlr_valor_proposta
            , propostas.vlr_valor_entrada
            , propostas.vlr_valor_prestacao
            , propostas.num_quantidade_parcelas
            , propostas.num_carencia
            , propostas.flg_status_proposta
        from {{ ref('stg_propostas_credito') }} as propostas
        left join {{ ref('dim_colaboradores') }} as colaboradores
            on propostas.cod_colaborador = colaboradores.cod_colaborador
        left join {{ ref('dim_contas') }} as contas
            on propostas.cod_cliente = contas.cod_cliente
    )

select *
from stgs_data
