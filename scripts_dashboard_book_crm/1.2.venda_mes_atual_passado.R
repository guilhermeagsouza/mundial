##### 0.0 carrega pacotes ####
pacman::p_load(odbc, DBI, tidyverse, hms, DataExplorer, writexl, magrittr, feather)

# 0.1 Conecta ao banco de dados
source(
  'scripts_dashboard_book_crm/0.conecta_banco.R', 
  echo=TRUE, 
  max.deparse.length=60
)

hoje <- lubridate::today()
ano_mes_selecionado <- 100*lubridate::year(hoje) + lubridate::month(hoje)

df_comprador <- DBI::dbGetQuery(
  conn = con, 
  statement = "
SELECT CODIGO AS CODIGO_COMPRADOR, NOME AS NOME_COMPRADOR 
FROM [sgm].[dbo].[compra]"
  )

cadastros_skus <- readxl::read_xlsx('dados/cadastro_skus.xlsx') %>% 
  dplyr::select(descri, codigo, compra) %>% 
  dplyr::rename(DESCRICAO_COMPLETA = descri, COD_INTERNO = codigo, CODIGO_COMPRADOR = compra) %>% 
  dplyr::mutate(COD_INTERNO = as.numeric(COD_INTERNO)) %>% 
  dplyr::left_join(df_comprador, by = 'CODIGO_COMPRADOR') %>% 
  dplyr::arrange(DESCRICAO_COMPLETA)

descricao_loja <- readxl::read_xlsx('dados/descricao_lojas.xlsx') %>% 
  dplyr::mutate(LOJA = as.numeric(LOJA))

# VENDA 
df_venda_sku <- DBI::dbGetQuery(
  conn = con, 
  statement = "
SELECT 
100*YEAR(DATA) + MONTH(DATA) AS ANO_MES,
v.COD_INTERNO,
(sum(PRECO_TOTAL)-ISNULL(SUM(DESCONTO), 0)) AS PRECO_TOTAL
FROM VM_INTEGRACAO.dbo.vw_propz v
WHERE DATA >= '2023-08-01' AND DATA <= '2023-09-30'
GROUP BY 100*YEAR(DATA) + MONTH(DATA), v.COD_INTERNO
ORDER BY SUM(PRECO_TOTAL) DESC;
  ")

df_venda_sku_mes_atual <- df_venda_sku %>% 
  dplyr::filter(ANO_MES == 202309)

# 2. VENDA SKU MÊS PASSADO
df_venda_sku_mes_passado <- df_venda_sku %>% 
  dplyr::filter(ANO_MES == 202308)

df_venda_append <- df_venda_sku_mes_atual %>% 
  rbind(df_venda_sku_mes_passado) %>% 
  tidyr::pivot_wider(
    names_from = ANO_MES, 
    values_from = c(PRECO_TOTAL)
  ) %>% 
  dplyr::mutate_if(is.numeric, coalesce, 0)

fun_renomeia_coluna <- function(df) {
  nomes <- names(df)
  if(as.numeric(nomes[2]) > as.numeric(nomes[3])) {
    nomes <- c(nomes[1],'PRECO_TOTAL_MES_ATUAL', 'PRECO_TOTAL_MES_PASSADO')
  } else {
    nomes <- c(nomes[1],'PRECO_TOTAL_MES_PASSADO', 'PRECO_TOTAL_MES_ATUAL')
  }
  print(nomes)
}

names(df_venda_append) <- fun_renomeia_coluna(df_venda_append) 

## 3. VENDA MEU MUNDIAL
df_venda_sku_mes_atual_meumundial <- DBI::dbGetQuery(
  conn = con, 
  statement = "
SELECT COD_INTERNO, SUM(PRECO_TOTAL)-ISNULL(SUM(DESCONTO), 0) AS VENDA_TOTAL
FROM VM_LOG.dbo.ITEM_DESCONTO
WHERE DATA >= '2023-09-01' AND DATA <= '2023-09-30' AND TIPO_PRODUTO=4 AND CANCELADO=0 
GROUP BY COD_INTERNO"
)

df_venda_sku_mes_atual_meumundial <- df_venda_sku_mes_atual_meumundial %>% 
  dplyr::select(COD_INTERNO,VENDA_TOTAL) %>% 
  dplyr::rename(PRECO_TOTAL_MES_ATUAL_MEUMUNDIAL = VENDA_TOTAL)

######
tbl_casadinha <- readxl::read_xlsx(
  'output_dashboard_book_crm/3.2.promo_casadinha_mes_atual_consolidada.xlsx'
)

tbl_casadinha %<>% 
  dplyr::group_by(COD_INTERNO) %>% 
  dplyr::summarise(PRECO_TOTAL_MES_ATUAL_MEUMUNDIAL = sum(TOTAL_VENDA))

tbl_casadinha_apenas_para_append <- data.frame(
  COD_INTERNO = tbl_casadinha$COD_INTERNO, 
  PRECO_TOTAL_MES_ATUAL = rep(0,nrow(tbl_casadinha)),
  PRECO_TOTAL_MES_PASSADO = rep(0,nrow(tbl_casadinha)),
  PRECO_TOTAL_MES_ATUAL_MEUMUNDIAL = tbl_casadinha$PRECO_TOTAL_MES_ATUAL_MEUMUNDIAL
  )
#######

# Consolida a base
df_venda_consolidada <- df_venda_append %>% 
  dplyr::left_join(df_venda_sku_mes_atual_meumundial, by = c('COD_INTERNO')) %>% 
  rbind(tbl_casadinha_apenas_para_append) %>% 
  dplyr::group_by(COD_INTERNO) %>% 
  dplyr::summarise(
    PRECO_TOTAL_MES_ATUAL = sum(PRECO_TOTAL_MES_ATUAL,na.rm = TRUE), 
    PRECO_TOTAL_MES_PASSADO  = sum(PRECO_TOTAL_MES_PASSADO,na.rm = TRUE),
    PRECO_TOTAL_MES_ATUAL_MEUMUNDIAL  = sum(PRECO_TOTAL_MES_ATUAL_MEUMUNDIAL,na.rm = TRUE)
  ) %>% 
  dplyr::left_join(cadastros_skus, by = c('COD_INTERNO')) %>% 
  dplyr::mutate(
    DESCRICAO_COMPLETA = ifelse(is.na(DESCRICAO_COMPLETA), 'SEM DESCRIÇÃO', DESCRICAO_COMPLETA),
    CODIGO_COMPRADOR = ifelse(is.na(CODIGO_COMPRADOR), 'AUSENTE', CODIGO_COMPRADOR),
    NOME_COMPRADOR = ifelse(is.na(NOME_COMPRADOR), 'AUSENTE', NOME_COMPRADOR)
  ) %>% 
  dplyr::mutate_if(is.numeric, coalesce, 0) %>% 
  dplyr::mutate(
      PARTICIPACAO_MEU_MUNDIAL = PRECO_TOTAL_MES_ATUAL_MEUMUNDIAL/PRECO_TOTAL_MES_ATUAL,
      PARTICIPACAO_MEU_MUNDIAL = ifelse(is.nan(PARTICIPACAO_MEU_MUNDIAL),0, PARTICIPACAO_MEU_MUNDIAL),
      PARTICIPACAO_MEU_MUNDIAL = ifelse(PARTICIPACAO_MEU_MUNDIAL>1,1,PARTICIPACAO_MEU_MUNDIAL)
  ) %>% 
  dplyr::filter(!NOME_COMPRADOR %in% c('AUSENTE'))

## SKU e Imagem associada a ele
df_sku_img <- DBI::dbGetQuery(
  conn = con, 
  statement = "SELECT [codigo] as COD_INTERNO,
      [imagem] as IMAGEM
  FROM [sgm].[dbo].[estoq]
  "
)

df_sku_img %<>% dplyr::mutate(COD_INTERNO = as.numeric(COD_INTERNO))

df_venda_consolidada %<>% dplyr::left_join(df_sku_img, by = c('COD_INTERNO'))

df_venda_consolidada %<>% dplyr::mutate(DESCRICAO_COMPLETA = stringr::str_to_title(DESCRICAO_COMPLETA))
df_venda_consolidada %<>% dplyr::filter(NOME_COMPRADOR !="")

##########
df_produtos <- DBI::dbGetQuery(
  conn = con, 
  statement = "SELECT PRODUTO_ID AS COD_INTERNO, LTRIM(DESCRICAO_COMPLETA) AS DESCRICAO_COMPLETA,
  MERCADOLOGICO2 AS SECAO, MERCADOLOGICO3 AS GRUPO, MERCADOLOGICO4 AS SUBGRUPO,
  SITUACAO
FROM VM_DATABSP.dbo.PRODUTOS"
) %>% 
  dplyr::mutate_at(c('SECAO','GRUPO','SUBGRUPO'), as.numeric)

#### CARREGA TABELA CATEGORIA SKUS ####
df_categorias <- DBI::dbGetQuery(
  conn = con, 
  statement = "select codsec AS SECAO, secao as NOME_SECAO, grupo as GRUPO, nomgrup as NOME_GRUPO, subgrupo as SUBGRUPO, nom_sub as NOME_SUBGRUPO
from [sgm].[dbo].[secao]"
) %>% 
  dplyr::mutate_at(c('SECAO','GRUPO','SUBGRUPO'), as.numeric)

#### JUNTA AS BASES DOS SKUS POR CATEGORIA ####
df_produtos %<>% 
  dplyr::left_join(df_categorias, by = c('SECAO','GRUPO','SUBGRUPO')) %>% 
  dplyr::select(COD_INTERNO, DESCRICAO_COMPLETA, NOME_SECAO, NOME_GRUPO, NOME_SUBGRUPO, SITUACAO)
##########

df_venda_consolidada %<>% 
  dplyr::left_join(df_produtos, by = c('COD_INTERNO')) %>% 
  dplyr::select(-DESCRICAO_COMPLETA.y) %>% 
  dplyr::rename(DESCRICAO_COMPLETA = DESCRICAO_COMPLETA.x)

df_venda_consolidada %>% 
  writexl::write_xlsx('output_dashboard_book_crm/1.vendas_preco_clube.xlsx')

rm(list=setdiff(ls(), "con"))
