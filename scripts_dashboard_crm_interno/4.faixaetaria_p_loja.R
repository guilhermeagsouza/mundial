##### 1. Carrega dados
pacman::p_load(
  odbc, DBI, tidyverse, hms, DataExplorer, writexl,readxl, 
  magrittr, feather,data.table
)

# 1.1 Leitura dos dados
df_descricao_loja <- readxl::read_xlsx('dados/descricao_lojas.xlsx') %>% 
  dplyr::mutate(loja = as.character(LOJA))

arquivo <- data.table::fread(
"C:/Users/guilherme.souza/Desktop/analises_r/analises_r/dados/2023-8-17-export-00_-_Todos_Os_Clientes-0.tsv"
)

df_cpf_idade <- arquivo %>% 
  dplyr::mutate(
    averageTicket = as.numeric(stringr::str_replace(string = averageTicket, pattern = ",", replacement = ".")),
    idade = lubridate::year(Sys.Date()) - lubridate::year(dateOfBirth)
  ) %>% 
  na.omit()

# VENDA 
df_venda <- DBI::dbGetQuery(
  conn = con, 
  statement = "
SELECT loja, data, 100*year(data)+month(data) as ano_mes, VALORIDENTCLIENTE
FROM VM_INTEGRACAO.dbo.vw_propz v
LEFT JOIN VM_DATABSP.dbo.PRODUTOS p ON v.COD_INTERNO = p.PRODUTO_ID
WHERE DATA >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0) AND TIPOIDENTCLIENTE = 1
  ")

df_loja_cpf <- df_venda %>% 
  dplyr::distinct(loja, VALORIDENTCLIENTE) %>% 
  dplyr::rename(customerId = VALORIDENTCLIENTE) %>% 
  dplyr::mutate(loja = as.character(loja))

df_segmentacao_p_idade_loja <- df_loja_cpf %>% 
  left_join(df_cpf_idade, by = c('customerId')) %>% 
  dplyr::filter(!is.na(idade) & idade >= 18 & idade <= 99) %>% 
  dplyr::mutate(
    loja = as.character(loja),
      segmentacao_idade = cut(idade, breaks = seq(20, 100, by = 10), include.lowest = FALSE)
) %>% 
  group_by(loja) %>% 
  count(segmentacao_idade) %>% 
  dplyr::left_join(df_descricao_loja, by = c('loja')) %>% 
  na.omit() %>% 
  ungroup() %>% 
  dplyr::select(NOME_LOJA, segmentacao_idade, n) %>% 
  dplyr::rename(SEGMENTACAO_IDADE = segmentacao_idade, TOTAL_CPF = n)

writexl::write_xlsx(
  list(faixa_etaria_p_loja = df_segmentacao_p_idade_loja),
  'output_dashboard_crm_interno/4.faixaetaria_p_loja.xlsx'
)

rm(list=setdiff(ls(), c("con","df_venda")))
