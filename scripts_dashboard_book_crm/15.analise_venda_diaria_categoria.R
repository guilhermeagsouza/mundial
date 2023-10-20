# ANÁLISE VENDA DIÁRIA DAS CATEGORIAS

# Início do código
start.time <- Sys.time()

# 0.0 Carrega os pacotes
pacman::p_load(
  odbc, DBI, tidyverse, hms, DataExplorer, writexl, magrittr, feather
  )

# 0.1 Conecta ao banco de dados
source(
  'scripts_dashboard_book_crm/0.conecta_banco.R', 
  echo=TRUE, 
  max.deparse.length=60
)

df_venda_hierarquia_produtos_dia <- DBI::dbGetQuery(
  conn = con,
  statement = "-- O filtro está atualmente na DESCRICAO_COMPLETA do produto 
SELECT 
	DATA,
	s.secao AS NOME_SECAO,
	s.nomgrup as NOME_GRUPO,
	s.nom_sub as NOME_SUBGRUPO,
	SUM(QUANTIDADE) AS QUANTIDADE_TOTAL,
	SUM(PRECO_TOTAL-ISNULL(DESCONTO,0)) AS PRECO_TOTAL,
	SUM(PRECO_TOTAL-ISNULL(DESCONTO,0))/SUM(QUANTIDADE) AS PRECO_MEDIO
FROM VM_INTEGRACAO.dbo.vw_propz v
LEFT JOIN VM_DATABSP.dbo.PRODUTOS p ON v.COD_INTERNO = p.PRODUTO_ID
LEFT JOIN [sgm].[dbo].[secao] s ON p.MERCADOLOGICO2=s.codsec AND p.MERCADOLOGICO3=s.grupo AND p.MERCADOLOGICO4=s.subgrupo
WHERE DATA > '2023-01-01'
GROUP BY DATA, s.secao,s.nomgrup,s.nom_sub
ORDER BY SUM(PRECO_TOTAL) DESC
;"
)

end.time <- Sys.time()
(time.taken <- round(end.time - start.time,2))

# Remove os espaços das variáveis de formato character
df_venda_hierarquia_produtos_dia %<>% mutate_if(is.character, str_trim)

df_venda_hierarquia_produtos_dia %<>% 
  dplyr::mutate(
    ANO_MES = 100*lubridate::year(DATA) + lubridate::month(DATA),
    DIA_DO_MES = lubridate::day(DATA),
    DIA_DA_SEMANA = lubridate::wday(DATA, label = TRUE, abbr = TRUE,
                         week_start = getOption("lubridate.week.start", 7),
                         locale = Sys.getlocale("LC_TIME"))
    )

writexl::write_xlsx(
  list(venda_categoria_dia = df_venda_hierarquia_produtos_dia),
  'output_dashboard_book_crm/15.venda_categoria_dia.xlsx',
)

df_venda_hierarquia_produtos_dia <- readxl::read_xlsx(
  'output_dashboard_book_crm/15.venda_categoria_dia.xlsx'
)

# VENDA DAS SEÇÕES
df_venda_secao <- df_venda_hierarquia_produtos_dia %>% 
  #dplyr::select(-NOME_GRUPO, -NOME_SUBGRUPO) %>% 
  dplyr::group_by(DIA_DA_SEMANA, NOME_SECAO,NOME_GRUPO,NOME_SUBGRUPO) %>% 
  dplyr::summarise(QUANTIDADE_TOTAL = sum(QUANTIDADE_TOTAL), PRECO_TOTAL = sum(PRECO_TOTAL))

df_venda_secao %<>% 
  dplyr::group_by(NOME_SECAO, NOME_GRUPO, NOME_SUBGRUPO) %>% 
  dplyr::arrange(desc(PRECO_TOTAL)) %>% 
  top_n(3) %>% 
  dplyr::arrange(NOME_SECAO,NOME_GRUPO, NOME_SUBGRUPO) %>% 
  dplyr::mutate(DIA_DA_SEMANA = stringr::str_to_upper(DIA_DA_SEMANA))

# Salvando o arquivo
writexl::write_xlsx(
  list(dias_com_mais_venda_categorias = df_venda_secao),
  'output_dashboard_book_crm/15.dias_com_mais_venda_categorias.xlsx'
  )

df_venda_secao %>% 
  dplyr::filter(NOME_SECAO == 'BEBIDAS I' & NOME_GRUPO == 'CERVEJAS') %>% 
  dplyr::arrange(desc(PRECO_TOTAL))

# Final do código
end.time <- Sys.time()
(time.taken <- round(end.time - start.time,2))

# Apaga todos os dados, com exceção da conexão com o banco
rm(list=setdiff(ls(), "con"))
