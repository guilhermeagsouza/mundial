# 6.3 RELATIONSHIP OPPORTUNITY EMAIL
# 0.0 Carrega os pacotes
pacman::p_load(odbc, DBI, tidyverse, hms, DataExplorer, writexl, magrittr, feather)

# 0.1 Conecta ao banco de dados
source(
  'scripts_dashboard_book_crm/0.conecta_banco.R', 
  echo=TRUE, 
  max.deparse.length=60
)

arquivo <- read_tsv('dados/2023-10-19-export-01.3_-_Contataveis_Email-0.tsv')

relationshipOpportunity <- arquivo %>% 
  dplyr::filter(!is.na(relationshipOpportunity) & !relationshipOpportunity == '')
#############

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

final_df <- data.frame(
  categoria1 = character(),categoria2 = character(), categoria3 = character(),  
  stringsAsFactors = FALSE
)

for(i in 1:nrow(relationshipOpportunity)) {
  df <- relationshipOpportunity[i,] %>% 
    dplyr::select(relationshipOpportunity) %>% 
    dplyr::pull() %>% 
    fun_extrai_string_cria_categoria()
  
  final_df <- rbind(final_df, df)
  
}
print(final_df)  

df_final <- final_df %>% 
  dplyr::mutate(segmento = relationshipOpportunity$segment) %>% 
  dplyr::group_by(categoria1,categoria2, categoria3, segmento) %>% 
  dplyr::count() %>% 
  dplyr::mutate(
    segmento = stringr::str_to_upper(segmento),
    oportunidade = "relationship"
  ) %>% 
  na.omit() %>% 
  dplyr::rename(secao = categoria1, nom_grup = categoria2, nom_sub = categoria3)

  # 5. Salva a base na pasta output
  writexl::write_xlsx(
    list(relationship = df_final),
  'output_dashboard_book_crm/6.3.output_relationshipOpportunity_email.xlsx',
  )

mensagem <- 'A base RELATIONSHIP OPPORTUNITY EMAIL está carregada!'
print(mensagem)

# Apaga todos os dados, com exceção da conexão com o banco
rm(list=setdiff(ls(), "con"))
