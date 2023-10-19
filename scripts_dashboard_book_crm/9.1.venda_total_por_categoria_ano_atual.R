#### 9.1 VENDA TOTAL POR CATEGORIA ANO ATUAL ####
# 0.0 Carrega os pacotes
pacman::p_load(odbc, DBI, tidyverse, hms, DataExplorer, writexl, magrittr, feather)

# 0.1 Conecta ao banco de dados
source(
  'scripts_dashboard_book_crm/0.conecta_banco.R', 
  echo=TRUE, 
  max.deparse.length=60
)

# Início do processo
start.time <- Sys.time()

#### 1.1 CARREGA BASE COMPRADORES ####
df_comprador <- readxl::read_xlsx(path = 'dados/compradores.xlsx') %>% 
  dplyr::rename(CODIGO_COMPRADOR = codigo, NOME_COMPRADOR = nome) %>% 
  dplyr::arrange(CODIGO_COMPRADOR)

#### 1.2 CARREGA NOME LOJA ####
descricao_loja <- readxl::read_xlsx('dados/descricao_lojas.xlsx') %>% 
  dplyr::mutate(LOJA = as.numeric(LOJA))

#### 1.3 CARREGA TABELA SKUS ####
df_produtos <- DBI::dbGetQuery(
  conn = con, 
  statement = "SELECT PRODUTO_ID AS COD_INTERNO, LTRIM(DESCRICAO_COMPLETA) AS DESCRICAO_COMPLETA,
  MERCADOLOGICO2 AS SECAO, MERCADOLOGICO3 AS GRUPO, MERCADOLOGICO4 AS SUBGRUPO,
  SITUACAO
FROM VM_DATABSP.dbo.PRODUTOS"
) %>% 
  dplyr::mutate_at(c('SECAO','GRUPO','SUBGRUPO'), as.numeric)

#### 1.4 CARREGA TABELA CATEGORIA SKUS ####
df_categorias <- DBI::dbGetQuery(
  conn = con, 
  statement = "select codsec AS SECAO, secao as NOME_SECAO, grupo as GRUPO, nomgrup as NOME_GRUPO, subgrupo as SUBGRUPO, nom_sub as NOME_SUBGRUPO
from [sgm].[dbo].[secao]"
) %>% 
  dplyr::mutate_at(c('SECAO','GRUPO','SUBGRUPO'), as.numeric)

#### 1.5 JUNTA AS BASES DOS SKUS POR CATEGORIA ####
df_produtos %<>% 
  dplyr::left_join(df_categorias, by = c('SECAO','GRUPO','SUBGRUPO')) %>% 
  dplyr::select(COD_INTERNO, DESCRICAO_COMPLETA, NOME_SECAO, NOME_GRUPO, NOME_SUBGRUPO, SITUACAO)
#########

data_de_hoje <- lubridate::today()

#### 2.0 CARREGA BASE VENDA ####
df_venda_loja <- DBI::dbGetQuery(
  conn = con, 
  statement = paste0("SELECT
  v.LOJA,
  100*YEAR(DATA) + MONTH(DATA) AS ANO_MES,
v.COD_INTERNO,
SUM(QUANTIDADE) AS QUANTIDADE_TOTAL,
(sum(PRECO_TOTAL)-ISNULL(SUM(DESCONTO), 0)) AS PRECO_TOTAL,
SUM(PRECO_TOTAL)/SUM(QUANTIDADE) AS PRECO_MEDIO
FROM VM_INTEGRACAO.dbo.vw_propz v
LEFT JOIN VM_DATABSP.dbo.PRODUTOS p ON v.COD_INTERNO = p.PRODUTO_ID
WHERE DATA BETWEEN '2023-01-01' AND '",data_de_hoje,"'","
GROUP BY v.LOJA,100*YEAR(DATA) + MONTH(DATA), v.COD_INTERNO, p.DESCRICAO_COMPLETA
ORDER BY SUM(PRECO_TOTAL) DESC"
)
)

#### 2.1 JUNTA BASE DE VENDA COM CATEGORIAS DOS SKUS ####
df_venda_consolidada <- df_venda_loja %>% 
  dplyr::left_join(df_produtos, by = c('COD_INTERNO')) %>% 
  dplyr::left_join(descricao_loja, by = c('LOJA'))

df_mes_nome <- data.frame(
  ANO_MES = seq(202301,202312),
  MES = c("Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", 
          "Ago", "Set", "Out", "Nov", "Dez")
)

# Altera os nomes das situações dos skus
df_venda_consolidada %<>% 
  dplyr::left_join(df_mes_nome, by = c('ANO_MES')) %>% 
  dplyr::mutate(
    SITUACAO = case_when(
      SITUACAO == "A" ~ "Ativo",
      SITUACAO == "E" ~ "Excluído",
      SITUACAO == "D" ~ "Descontinuado",
      TRUE ~ NA_character_  # Caso contrário, definir como NA
  ),
  PROP_PRECO_TOTAL = PRECO_TOTAL/sum(PRECO_TOTAL)
  )

df_venda_consolidada %>% 
  write.csv2("output_dashboard_book_crm/11.venda_total_ano_atual.csv", sep = ",",dec = ",", row.names = FALSE)

# Final do processo
end.time <- Sys.time()
(time.taken <- round(end.time - start.time,2))

rm(list=setdiff(ls(), "con"))
