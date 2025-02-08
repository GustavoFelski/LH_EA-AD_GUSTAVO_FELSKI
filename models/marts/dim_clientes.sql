with
    stg_data as (
        select
            cod_cliente
            , txt_primeiro_nome
            , txt_ultimo_nome
            , txt_email
            , txt_tipo_cliente
            , dt_data_inclusao
            , cod_cpf_ou_cnpj
            , dt_data_nascimento
            , txt_endereco
            , cod_cep
            , date_diff(current_date(), dt_data_nascimento, year) as num_idade_atual
            , right(txt_endereco, 2) as cod_uf
            , case lower(right(txt_endereco, 2))
                when 'ac' then 'Acre'
                when 'al' then 'Alagoas'
                when 'ap' then 'Amapá'
                when 'am' then 'Amazonas'
                when 'ba' then 'Bahia'
                when 'ce' then 'Ceará'
                when 'df' then 'Distrito Federal'
                when 'es' then 'Espírito Santo'
                when 'go' then 'Goiás'
                when 'ma' then 'Maranhão'
                when 'mt' then 'Mato Grosso'
                when 'ms' then 'Mato Grosso do Sul'
                when 'mg' then 'Minas Gerais'
                when 'pa' then 'Pará'
                when 'pb' then 'Paraíba'
                when 'pr' then 'Paraná'
                when 'pe' then 'Pernambuco'
                when 'pi' then 'Piauí'
                when 'rj' then 'Rio de Janeiro'
                when 'rn' then 'Rio Grande do Norte'
                when 'rs' then 'Rio Grande do Sul'
                when 'ro' then 'Rondônia'
                when 'rr' then 'Roraima'
                when 'sc' then 'Santa Catarina'
                when 'sp' then 'São Paulo'
                when 'se' then 'Sergipe'
                when 'to' then 'Tocantins'
                else 'Desconhecido'
            end as txt_nome_estado
            , case
                when date_diff(current_date(), dt_data_nascimento, year) <= 18 then '0-18'
                when date_diff(current_date(), dt_data_nascimento, year) <= 30 then '18-30'
                when date_diff(current_date(), dt_data_nascimento, year) <= 50 then '30-50'
                when date_diff(current_date(), dt_data_nascimento, year) <= 70 then '50-70'
                else '70+'
            end as txt_faixa_etaria
            , case
                when date_diff(current_date(), dt_data_nascimento, year) <= 18 then 1
                when date_diff(current_date(), dt_data_nascimento, year) <= 30 then 2
                when date_diff(current_date(), dt_data_nascimento, year) <= 50 then 3
                when date_diff(current_date(), dt_data_nascimento, year) <= 70 then 4
                else 5
            end as num_faixa_etaria_ordenacao          
        from {{ ref('stg_clientes') }}
    )

    , ultima_transacao_geral as (
        select
            max(dt_data_transacao) as dt_ultima_transacao_geral
        from {{ ref('stg_transacoes') }}
    )

    , ultima_transacao_conta as (
        select 
            num_conta 
            , max(dt_data_transacao) as dt_ultima_transacao_conta
        from {{ ref('stg_transacoes') }}
        group by num_conta
    )

    , contas_ativas_inativas AS (
        select 
            utc.num_conta
            , date_diff(utg.dt_ultima_transacao_geral, utc.dt_ultima_transacao_conta, day) as num_dias_ultima_movimentacao
            , case 
                when date_diff(utg.dt_ultima_transacao_geral, utc.dt_ultima_transacao_conta, day) > 180 then 'inativo'
                else 'ativo'
            end as flg_status_conta
        from ultima_transacao_conta utc
        cross join ultima_transacao_geral utg
    )

    , joined_tables as (
        select
            contas.cod_cliente
            , contas_ativas_inativas.num_dias_ultima_movimentacao
            , contas_ativas_inativas.flg_status_conta            
        from {{ ref('stg_contas') }} contas
        left join contas_ativas_inativas
            on contas.num_conta = contas_ativas_inativas.num_conta
    )

    , final as (
        select
            stg_data.*
            , joined_tables.num_dias_ultima_movimentacao
            , joined_tables.flg_status_conta            
        from stg_data
        left join joined_tables
            on stg_data.cod_cliente = joined_tables.cod_cliente
    )

select *
from final
