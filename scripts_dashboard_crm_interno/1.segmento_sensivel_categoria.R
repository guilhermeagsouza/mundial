##### 1. Carrega dados
pacman::p_load(
  odbc, DBI, tidyverse, hms, DataExplorer, writexl,readxl, 
  magrittr, feather,data.table
)

# 1.1 Leitura dos dados
arquivo <- data.table::fread('dados/2023-8-10-export-00_-_Todos_Os_Clientes.csv')
#arquivo %<>% head(50000)

# 1.2 Criando a variável
arquivo[, averageTicket := as.numeric(stringr::str_replace(string = averageTicket, pattern = ",", replacement = "."))]
arquivo2 <-  arquivo[!is.na(reducedSpendOpportunity) & !reducedSpendOpportunity == '']

reducedSpendOpportunity <- arquivo2

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
  df <- reducedSpendOpportunity[i,list(reducedSpendOpportunity)] %>% 
    dplyr::pull() %>% 
    fun_extrai_string_cria_categoria()
  
  final_df <- rbind(final_df, df)
  
}
print(final_df)  

#final_df_c_cpfs <- final_df[,c("cpf","segment","registro_completo","sensibilityprice","averageTicket") := .(reducedSpendOpportunity$customerId,reducedSpendOpportunity$segment,
#                              reducedSpendOpportunity$isRegisterComplete,
#                              reducedSpendOpportunity$sensibilityPrice,arquivo2$averageTicket)]

final_df %<>% as.data.table()
final_df_c_cpfs <- final_df[, ':='(cpf = reducedSpendOpportunity$customerId,segment = reducedSpendOpportunity$segment,
              registro_completo = reducedSpendOpportunity$isRegisterComplete,
              sensibilityprice = reducedSpendOpportunity$sensibilityPrice,
              averageTicket = arquivo2$averageTicket)]

df_final <- final_df_c_cpfs %>% 
  dplyr::mutate(
    segment = stringr::str_to_upper(segment)
  ) %>% 
  na.omit() %>% 
  dplyr::filter(sensibilityprice != 0) %>%
  dplyr::mutate(
    sensibilityprice = ifelse(sensibilityprice==1, 'Sensível às promoções','Não sensível às promoções')
  ) %>% 
  dplyr::rename(secao = categoria1, nom_grup = categoria2, nom_sub = categoria3) %>% 
  dplyr::mutate(
    registro_completo = ifelse(registro_completo == 1, 'Sim','Não'),
    segment = stringr::str_to_title(segment)
  )

writexl::write_xlsx(
  list(cpfs_seg_sens_cat = df_final),
  'output/cpfs_segmentos_sensibilidade_categoria.xlsx'
)

df_final_consolidado <- df_final %>% 
  dplyr::group_by(segment, sensibilityprice, registro_completo, secao,nom_grup, nom_sub) %>% 
  dplyr::summarise(
    ticket_medio = mean(averageTicket, na.rm = TRUE),
    total_cpfs = n()
    ) %>% 
  # Alterna as strings de seção, nome grupo e nome subgrupo.
  dplyr::mutate(
    secao = str_to_upper(secao),
    nom_grup = str_to_title(nom_grup),
    nom_sub = str_to_title(nom_sub)
  )

writexl::write_xlsx(
  list(seg_sensi_cat = df_final_consolidado),
  'output_dashboard_crm_interno/1.segmento_sensibilidade_categoria.xlsx'
)

rm(list=setdiff(ls(), "con"))
