# 0.0 Carrega os pacotes
pacman::p_load(odbc, DBI, tidyverse, hms, DataExplorer, writexl, magrittr, feather)

# 0.1 Conecta ao banco de dados
source(
  'scripts_dashboard_book_crm/0.conecta_banco.R', 
  echo=TRUE, 
  max.deparse.length=60
)

df_cadastro_skus <- readxl::read_xlsx('dados/cadastro_skus.xlsx') %>% 
  dplyr::select(secao, grupo, subgrupo, codigo) %>% 
  dplyr::mutate(
    codigo = as.numeric(codigo),
    secao = as.numeric(secao),
    grupo = as.numeric(grupo),
    subgrupo = as.numeric(subgrupo)
  ) %>% 
  dplyr::rename(COD_INTERNO = codigo)
names(df_cadastro_skus) <- stringr::str_to_upper(names(df_cadastro_skus))

df_descricao_sku <- DBI::dbGetQuery(
  conn = con, 
  statement = "SELECT PRODUTO_ID AS COD_INTERNO, DESCRICAO_COMPLETA
FROM VM_DATABSP.dbo.PRODUTOS
WHERE SITUACAO = 'A'
  "
) %>% 
  dplyr::mutate(COD_INTERNO = as.numeric(COD_INTERNO))

nome_categorias_sku <- DBI::dbGetQuery(
  conn = con, 
  statement = "select codsec AS SECAO, secao as NOME_SECAO, grupo as GRUPO, nomgrup as NOME_GRUPO, subgrupo as SUBGRUPO, nom_sub as NOME_SUBGRUPO
from [sgm].[dbo].[secao]"
) %>% 
  dplyr::mutate(
    SECAO = as.numeric(SECAO), GRUPO = as.numeric(GRUPO), SUBGRUPO = as.numeric(SUBGRUPO)
  )

## SKU e Imagem associada a ele
df_sku_img <- DBI::dbGetQuery(
  conn = con, 
  statement = "SELECT [codigo] as COD_INTERNO, [imagem] as IMAGEM
  FROM [sgm].[dbo].[estoq]
  "
)
df_sku_img %<>% dplyr::mutate(COD_INTERNO = as.numeric(COD_INTERNO))

df_consolidado_categoria_skus <- df_descricao_sku %>% 
  dplyr::left_join(df_cadastro_skus, by = c('COD_INTERNO')) %>% 
  dplyr::left_join(nome_categorias_sku, by = c('SECAO','GRUPO','SUBGRUPO')) %>% 
  dplyr::left_join(df_sku_img, by = c('COD_INTERNO')) %>% 
  dplyr::mutate_if(is.character, stringr::str_to_title) %>% 
  dplyr::mutate(
    NOME_SECAO = str_to_upper(NOME_SECAO),
    #GRUPO = str_to_title(GRUPO),
    #SUBGRUPO = str_to_title(SUBGRUPO),
    IMAGEM = stringr::str_to_lower(IMAGEM), #letras minúsculas para todas as strings
    IMAGEM = trimws(IMAGEM) #remove todos os espaços
)

df_consolidado_categoria_skus %<>% 
  na.omit()

writexl::write_xlsx(
  list(hierarquia_skus = df_consolidado_categoria_skus),
  'output_dashboard_book_crm/4.hierarquia_skus_powerbi.xlsx'
)

rm(list=setdiff(ls(), "con"))