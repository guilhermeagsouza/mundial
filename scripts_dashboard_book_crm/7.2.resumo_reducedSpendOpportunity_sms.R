# 7.2 REDUCED SPEND OPPORTUNITY
# 0.0 Carrega os pacotes
pacman::p_load(odbc, DBI, tidyverse, hms, DataExplorer, writexl, magrittr, feather)

# 0.1 Conecta ao banco de dados
source(
  'scripts_dashboard_book_crm/0.conecta_banco.R', 
  echo=TRUE, 
  max.deparse.length=60
)

############## 1. carrega arquivo
arquivo <- data.table::fread('dados/2023-10-19-export-01.4_-_Contataveis_SMS-0.tsv')

reducedSpendOpportunity <- arquivo %>% 
  dplyr::filter(!is.na(reducedSpendOpportunity) & !reducedSpendOpportunity == '')
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

######################## 3. consolidado geral
final_df <- data.frame(
  categoria1 = character(),categoria2 = character(), categoria3 = character(),  
  stringsAsFactors = FALSE
)

for(i in 1:nrow(reducedSpendOpportunity)) {
  df <- reducedSpendOpportunity[i,] %>% 
    dplyr::select(reducedSpendOpportunity) %>% 
    dplyr::pull() %>% 
    fun_extrai_string_cria_categoria()
  
  final_df <- rbind(final_df, df)
  
}
print(final_df)  

df_final <- final_df %>% 
  dplyr::mutate(segmento = reducedSpendOpportunity$segment) %>% 
  dplyr::group_by(categoria1,categoria2, categoria3, segmento) %>% 
  dplyr::count() %>% 
  dplyr::mutate(
    segmento = stringr::str_to_upper(segmento),
    oportunidade = "reducedspend sms"
  ) %>% 
  na.omit() %>% 
  dplyr::rename(secao = categoria1, nom_grup = categoria2, nom_sub = categoria3)

# 5. Salva a pasta na base output
writexl::write_xlsx(
  list(reducedspend_sms = df_final),
  'output_dashboard_book_crm/7.2.output_reducedSpendOpportunity_sms.xlsx'
)

mensagem <- 'A base REDUCED SPEND OPPORTUNITY SMS está carregada!'
print(mensagem)

# Apaga todos os dados, com exceção da conexão com o banco
rm(list=setdiff(ls(), "con"))
