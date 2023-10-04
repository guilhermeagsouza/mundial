# 0.0 Carrega os pacotes
pacman::p_load(odbc, DBI, tidyverse, hms, DataExplorer, writexl, magrittr, feather)

# 0.1 Conecta ao banco de dados
source(
  'scripts_dashboard_book_crm/0.conecta_banco.R', 
  echo=TRUE, 
  max.deparse.length=60
)

# EXTRAI A BASE DE CUPONS
df_venda_sku_loja <- DBI::dbGetQuery(
  conn = con, 
  statement = "
    SELECT LOJA, COD_INTERNO, SUM(PRECO_TOTAL)-ISNULL(SUM(DESCONTO), 0) AS VENDA_TOTAL , SUM(DESCONTO) AS DESCONTO, SUM(QUANTIDADE) AS QUANTIDADE

FROM VM_LOG.dbo.ITEM_DESCONTO 

WHERE DATA >= '2023-09-01' AND DATA <= '2023-09-30' AND TIPO_PRODUTO=4 AND CANCELADO=0

GROUP BY LOJA, COD_INTERNO
ORDER BY LOJA, VENDA_TOTAL DESC
  "
)

casadinha <- readxl::read_xlsx('output_dashboard_book_crm/3.2.promo_casadinha_mes_atual_consolidada.xlsx')
casadinha %<>% 
  dplyr::rename(VENDA_TOTAL = TOTAL_VENDA, QUANTIDADE = QUANTIDADE_PROMOCAO) %>% 
  dplyr::select(LOJA, COD_INTERNO, VENDA_TOTAL, DESCONTO,QUANTIDADE)

df_venda_sku_loja <- df_venda_sku_loja %>% rbind(casadinha)

df_descricao_sku <- DBI::dbGetQuery(
  conn = con, 
  statement = "  SELECT * FROM VM_DATABSP.dbo.PRODUTOS
--WHERE SITUACAO = 'A'
  "
) %>%
  dplyr::select(PRODUTO_ID, DESCRICAO_COMPLETA) %>% 
  dplyr::rename(COD_INTERNO = PRODUTO_ID)

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

###################

df_venda_consolidado <- df_venda_sku_loja %>% 
  dplyr::left_join(cadastros_skus, by = c('COD_INTERNO')) %>% 
  dplyr::left_join(descricao_loja, by = c('LOJA'))

mes_criado <- 'Agosto'

## SKU e Imagem associada a ele
df_sku_img <- DBI::dbGetQuery(
  conn = con, 
  statement = "SELECT [codigo] as COD_INTERNO, [imagem] as IMAGEM
  FROM [sgm].[dbo].[estoq]
  "
)

df_sku_img %<>% 
  dplyr::mutate(
    COD_INTERNO = as.numeric(COD_INTERNO),
    IMAGEM = stringr::str_to_lower(IMAGEM), #letras minúsculas para todas as strings
    IMAGEM = trimws(IMAGEM) #remove todos os espaços
)

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

df_produtos %<>% 
  dplyr::left_join(df_categorias, by = c('SECAO','GRUPO','SUBGRUPO')) %>% 
  dplyr::select(COD_INTERNO, NOME_SECAO, NOME_GRUPO, NOME_SUBGRUPO, SITUACAO)

###############

venda_consolidada <- df_venda_consolidado %>% 
  dplyr::left_join(df_produtos, by = c('COD_INTERNO')) %>% 
  dplyr::left_join(df_sku_img, by = c('COD_INTERNO')) %>%
  dplyr::mutate(QUANTIDADE = tidyr::replace_na(QUANTIDADE, 0)) %>% 
  dplyr::arrange(DESCRICAO_COMPLETA) %>% 
  dplyr::select(COD_INTERNO, DESCRICAO_COMPLETA, IMAGEM, NOME_LOJA, QUANTIDADE, NOME_COMPRADOR,
                NOME_SECAO, NOME_GRUPO, NOME_SUBGRUPO) %>% 
  #tidyr::pivot_wider(names_from = NOME_LOJA, values_from = QUANTIDADE) %>% 
  dplyr::mutate(MES = mes_criado, DESCRICAO_COMPLETA = stringr::str_to_title(DESCRICAO_COMPLETA)) %>% 
  dplyr::mutate_if(is.numeric, coalesce, 0) %>% 
  na.omit() %>% 
  dplyr::filter(NOME_COMPRADOR !="")

venda_consolidada %>% head()
  
writexl::write_xlsx(
  list(dash_aba2 = venda_consolidada),
  'output_dashboard_book_crm/2.sku_loja.xlsx'
)
  
rm(list=setdiff(ls(), "con"))

                