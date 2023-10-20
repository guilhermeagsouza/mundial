# 6.1 INTRODUCTION OPPORTUNITY EMAIL
# 0.0 Carrega os pacotes
pacman::p_load(odbc, DBI, tidyverse, hms, DataExplorer, writexl, magrittr, feather)

# 0.1 Conecta ao banco de dados
source(
  'scripts_dashboard_book_crm/0.conecta_banco.R', 
  echo=TRUE, 
  max.deparse.length=60
)

arquivo <- read_tsv(
  stringr::str_c('dados/',data_atualizacao,'-export-01.3_-_Contataveis_Email-0.tsv')
)

############## 1. carrega arquivos
introductionOpportunity <- arquivo %>% 
  dplyr::filter(!is.na(introductionOpportunity) & !introductionOpportunity == '')

#df_compradores <- readxl::read_xlsx('dados/compradores.xlsx') %>% 
#  dplyr::rename(compra = codigo, nome_comprador = nome)

df_compradores <- DBI::dbGetQuery(
  conn = con, 
  statement = "
SELECT CODIGO AS compra, NOME AS nome_comprador
FROM [sgm].[dbo].[compra]"
)

df_cadastro_skus <- readxl::read_xlsx('dados/cadastro_skus.xlsx') %>% 
  dplyr::select(descri, secao,grupo,subgrupo,compra)

df_skus_compradores <- df_compradores %>% 
  dplyr::left_join(df_cadastro_skus, by = c('compra'))

df_hierarquia_skus <- DBI::dbGetQuery(
  conn = con, 
  statement = "SELECT DISTINCT codsec as secao, secao as nome_secao, grupo, nomgrup, subgrupo, nom_sub
FROM [sgm].[dbo].[secao]"
)

df_hierarquia_skus %<>% 
  dplyr::left_join(df_skus_compradores, by = c('secao','grupo','subgrupo')) %>% 
  dplyr::distinct()

df_hierarquia_skus %<>% 
  dplyr::select(nome_secao, nomgrup, nom_sub, nome_comprador) %>%
  dplyr::distinct() %>% 
  dplyr::rename(secao = nome_secao, nom_grup = nomgrup)
#############

##### 2. funcao extrai string
fun_extrai_string_cria_categoria <- function(string) {
  
  armazena_dados <- strsplit(string, ";")[[1]][1] %>% 
    strsplit(split = ":") %>% 
    unlist()
  
  df <- data.frame(categoria1 = armazena_dados[1], categoria2 = armazena_dados[2],
                   categoria3 = armazena_dados[3])
  
  final_df <- data.frame(
    categoria1 = character(),categoria2 = character(), categoria3 = character(),  
    stringsAsFactors = FALSE
  )
  
  final_df <- rbind(final_df, df)
  
}
#####

######################## 3. consolidado funcao
final_df <- data.frame(
  categoria1 = character(),categoria2 = character(), categoria3 = character(),  
  stringsAsFactors = FALSE
)

for(i in 1:nrow(introductionOpportunity)) {
  df <- introductionOpportunity[i,] %>% 
    dplyr::select(introductionOpportunity) %>% 
    dplyr::pull() %>% 
    fun_extrai_string_cria_categoria()
  
  final_df <- rbind(final_df, df)
  
}
print(final_df)  

df_final <- final_df %>% 
  dplyr::mutate(segmento = introductionOpportunity$segment) %>% 
  dplyr::group_by(categoria1,categoria2, categoria3, segmento) %>% 
  dplyr::count() %>% 
  dplyr::mutate(
    segmento = stringr::str_to_upper(segmento),
    oportunidade = "introduction"
  ) %>% 
  na.omit() %>% 
  dplyr::rename(secao = categoria1, nom_grup = categoria2, nom_sub = categoria3)



######## 5. SALVA ARQUIVO NA PASTA OUTPUT 
  
  # 5. Salva a pasta na base output
  writexl::write_xlsx(
    list(introduction = df_final),
    'output_dashboard_book_crm/6.1.output_introductionOpportunity_email.xlsx',
  )

mensagem <- 'A base INTRODUCTION OPPORTUNITY E-MAIL está carregada!'
print(mensagem)

# Apaga todos os dados, com exceção da conexão com o banco
rm(list=setdiff(ls(), c("con",'data_atualizacao')))
