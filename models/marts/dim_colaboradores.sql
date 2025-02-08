with
    stgs_data as (
        select
            colaboradores.cod_colaborador
            , colaborador_agencia.cod_agencia
            , colaboradores.txt_primeiro_nome
            , colaboradores.txt_ultimo_nome
            , colaboradores.txt_email
            , colaboradores.cod_cpf
            , colaboradores.dt_data_nascimento
            , colaboradores.txt_endereco
            , colaboradores.cod_cep
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
        from {{ ref('stg_colaboradores') }} as colaboradores
        left join {{ ref ('stg_colaborador_agencia') }} as colaborador_agencia
            on colaboradores.cod_colaborador = colaborador_agencia.cod_colaborador
    )

select *
from stgs_data
