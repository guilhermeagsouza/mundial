# 0.0 Carrega os pacotes
pacman::p_load(odbc, DBI, tidyverse, hms, DataExplorer, writexl, magrittr, feather)

# 0.1 Conecta ao banco de dados
source(
  'scripts_dashboard_book_crm/0.conecta_banco.R', 
  echo=TRUE, 
  max.deparse.length=60
)

# 1.1 Nome lojas
df_loja <- readxl::read_xlsx('dados/descricao_lojas.xlsx') %>% 
  dplyr::mutate(LOJA = as.numeric(LOJA))

#### 1.2 CARREGA BASE COMPRADORES ####

df_comprador <- DBI::dbGetQuery(
  conn = con, 
  statement = "
SELECT CODIGO AS CODIGO_COMPRADOR, NOME AS NOME_COMPRADOR 
FROM [sgm].[dbo].[compra]"
)

# 2.1 Nome dos SKUS
df_produtos <- DBI::dbGetQuery(
  conn = con, 
  statement = "SELECT PRODUTO_ID AS COD_INTERNO, LTRIM(DESCRICAO_COMPLETA) AS DESCRICAO_COMPLETA,
  MERCADOLOGICO2 AS SECAO, MERCADOLOGICO3 AS GRUPO, MERCADOLOGICO4 AS SUBGRUPO,
  SITUACAO
  FROM VM_DATABSP.dbo.PRODUTOS"
) %>% 
  dplyr::mutate_at(c('SECAO','GRUPO','SUBGRUPO'), as.numeric)

#### 2.2 CARREGA TABELA CATEGORIA SKUS ####
df_categorias <- DBI::dbGetQuery(
  conn = con, 
  statement = "select codsec AS SECAO, secao as NOME_SECAO, grupo as GRUPO, nomgrup as NOME_GRUPO, subgrupo as SUBGRUPO, nom_sub as NOME_SUBGRUPO
from [sgm].[dbo].[secao]"
) %>% 
  dplyr::mutate_at(c('SECAO','GRUPO','SUBGRUPO'), as.numeric)

#### 2.3 SKU e Imagem associada a ele ####
df_sku_img <- DBI::dbGetQuery(
  conn = con, 
  statement = "SELECT [codigo] as COD_INTERNO, [imagem] as IMAGEM
  FROM [sgm].[dbo].[estoq]
  "
) %>% 
  dplyr::mutate(
    COD_INTERNO = as.numeric(COD_INTERNO),
    IMAGEM = stringr::str_to_lower(IMAGEM), #letras minúsculas para todas as strings
    IMAGEM = trimws(IMAGEM) #remove todos os espaços
)

#### 2.4 JUNTANDO TABELAS #### 
df_produtos %<>% 
  dplyr::left_join(df_categorias, by = c('SECAO','GRUPO','SUBGRUPO')) %>% 
  dplyr::select(COD_INTERNO, DESCRICAO_COMPLETA, NOME_SECAO, NOME_GRUPO, NOME_SUBGRUPO, SITUACAO) %>% 
  dplyr::left_join(df_sku_img, by = c('COD_INTERNO'))

# 3. Estoque Loja
df_query_estoque <- DBI::dbGetQuery(
  conn = con, 
  statement = "SELECT  [Filial] AS LOJA, [Codigo] AS COD_INTERNO,
  [Estoque] AS QUANTIDADE_ESTOQUE,[UltimaVenda] AS ULTIMA_VENDA FROM [sgm].[dbo].[VW_EstoqueLojas]
WHERE ESTOQUE > 0
  "
) %>% 
  dplyr::mutate(DATA_ATUALIZACAO_DADOS = now() %>% lubridate::ymd_hms())

#### 3.1 CONSOLIDANDO AS BASES ####
df_estoque_loja <- df_query_estoque %>% 
  dplyr::left_join(df_produtos, by = c('COD_INTERNO')) %>% 
  dplyr::filter(LOJA >=1 & LOJA <=27 & DESCRICAO_COMPLETA != "") %>% 
  dplyr::left_join(df_loja, by = c('LOJA')) %>% 
  dplyr::filter(!is.na(NOME_LOJA))

writexl::write_xlsx(
  list(estoque_loja = df_estoque_loja),
  'output_dashboard_book_crm/10.estoque_loja.xlsx'
)

rm(list=setdiff(ls(), "con"))
