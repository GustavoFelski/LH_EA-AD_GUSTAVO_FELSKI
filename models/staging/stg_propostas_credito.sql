with
    source_data as (
        select
            cast(cod_proposta as int) as cod_proposta
            , cast(cod_cliente as int) as cod_cliente
            , cast(cod_colaborador as int) as cod_colaborador
            , date(data_entrada_proposta) as dt_data_entrada_proposta
            , cast(taxa_juros_mensal as int) as num_taxa_juros_mensal
            , valor_proposta as vlr_valor_proposta
            , valor_entrada as vlr_valor_entrada
            , valor_prestacao as vlr_valor_prestacao
            , cast(quantidade_parcelas as int) as num_quantidade_parcelas
            , cast(carencia as int) as num_carencia
            , cast(status_proposta as string) as flg_status_proposta
        from {{ source('raw_data', 'propostas_credito') }}
    )

select *
from source_data
