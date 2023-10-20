# 12.MIGRAÇÃO CLIENTES MATOSO

# Início do código
start.time <- Sys.time()

# 0.0 Carrega os pacotes
pacman::p_load(odbc, DBI, tidyverse, hms, DataExplorer, writexl, magrittr, feather)

# 0.1 Conecta ao banco de dados
source(
  'scripts_dashboard_book_crm/0.conecta_banco.R', 
  echo=TRUE, 
  max.deparse.length=60
)

data_inicio <- '2023-01-02'
data_hoje <- lubridate::today()

df_ident_cliente_loja <-  DBI::dbGetQuery(
  conn = con, 
  statement = paste0("SELECT distinct
100*YEAR(DATA) + MONTH(DATA) AS ANO_MES,
LOJA,
VALORIDENTCLIENTE
FROM VM_INTEGRACAO.dbo.vw_propz
WHERE DATA BETWEEN '2023-01-02' AND '2023-10-19' AND TIPOIDENTCLIENTE = 1"
  )
)

#### 1.2 CARREGA NOME LOJA ####
descricao_loja <- readxl::read_xlsx('dados/descricao_lojas.xlsx') %>% 
  dplyr::mutate(LOJA = as.numeric(LOJA))

df_ident_cliente_loja %<>% 
  dplyr::left_join(descricao_loja, by = c('LOJA')) %>% 
  dplyr::arrange(desc(ANO_MES), VALORIDENTCLIENTE)

clientes_matoso <- df_ident_cliente_loja %>% 
  dplyr::filter(LOJA == 6) %>% 
  dplyr::select(VALORIDENTCLIENTE) %>% 
  dplyr::distinct()
nrow(clientes_matoso)

# Vendas em 202301 e 202302, totalizando 31.857 identificações distintas na filial F06 - MATOSO.

df_consolidada <- df_ident_cliente_loja %>% 
  #dplyr::mutate(CLIENTE_DA_MATOSO = ifelse(LOJA == 6, 1,0)) %>% 
  #dplyr::filter(!ANO_MES %in% c(202301,202302)) %>% 
  # FILTRA OS CLIENTS QUE ERAM DA MATOSO
  dplyr::filter(VALORIDENTCLIENTE %in% c(clientes_matoso$VALORIDENTCLIENTE)) %>% 
  #dplyr::filter(ANO_MES == 202303) %>% 
  dplyr::group_by(VALORIDENTCLIENTE,ANO_MES) %>% 
  summarize(LOJAS_VISITADAS = paste(sort(unique(NOME_LOJA)),collapse=", ")) %>% 
  ungroup() %>% 
  group_by(LOJAS_VISITADAS, ANO_MES) %>% 
  count() %>% 
  arrange(desc(n))

writexl::write_xlsx(
  list(analise_matoso = df_consolidada),
  'output_dashboard_book_crm/12.analise_loja_matoso.xlsx'
)

# Final do código
end.time <- Sys.time()
(time.taken <- round(end.time - start.time,2))

rm(list=setdiff(ls(), "con"))
