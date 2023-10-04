# 0.0 Carrega os pacotes
pacman::p_load(odbc, DBI, tidyverse, hms, DataExplorer, writexl, magrittr, feather)

# 0.1 Conecta ao banco de dados
source(
  'scripts_dashboard_book_crm/0.conecta_banco.R', 
  echo=TRUE, 
  max.deparse.length=60
)

dados <- data.table::fread(
  'dados/2023-9-26-export-00_-_Todos_Os_Clientes-0.csv'
)

df_loja <- readxl::read_xlsx('dados/descricao_lojas.xlsx') %>% 
  dplyr::mutate(LOJA = as.numeric(LOJA))

 df_registro_completo <- dados %>% 
   dplyr::filter(isRegisterComplete == 1)

 df_registro_completo_c_bairro <-  df_registro_completo %>% 
   dplyr::filter(homeDistrict !='') %>% 
   count(homeDistrict) %>% 
   arrange(desc(n))
 
 clientes_ocasionais <- df_registro_completo %>% 
   dplyr::filter(segment %in% c('Ocasional')) %>% 
   dplyr::select(customerId)
 clientes_ocasionais <- clientes_ocasionais$customerId
 
 # Análise do cliente ocasional
 df_registro_completo %>% 
   dplyr::filter(segment %in% c('Ocasional') & homeDistrict !='') %>% 
   dplyr::group_by(segment) %>% 
   dplyr::count(homeDistrict) %>% 
   dplyr::arrange(desc(n)) %>% View
 
 ################
 # Define o número de partes em que deseja dividir os clientes ocasionais
 num_partes <- 4
 
 # Inicializa uma lista para armazenar os data frames resultantes
 df_lista <- list()
 
 # Divide os clientes ocasionais em partes iguais
 partes <- split(clientes_ocasionais, cut(seq_along(clientes_ocasionais), num_partes, labels = FALSE))
 
 # Start - Consulta ao Banco
 start.time <- Sys.time()
 
 # Loop para criar os data frames
 for (i in 1:num_partes) {
   df <- DBI::dbGetQuery(
     conn = con,
     statement = paste0("SELECT DISTINCT DATA, NUM_CUPOM, LOJA, VALORIDENTCLIENTE 
                        FROM VM_INTEGRACAO.dbo.vw_propz v 
                        WHERE DATA BETWEEN '2023-06-26' AND '2023-09-26' AND 
                        VALORIDENTCLIENTE IN (", paste0("'", partes[[i]], "'", collapse = ","), ")")
   )
   
   # Adiciona o data frame à lista
   df_lista[[i]] <- df
 }
 
 # Combina todos os data frames em um único data frame
 df_venda_ocasional_junho_setembro <- do.call(rbind, df_lista)
 
 # Tempo final - Consulta ao banco
 end.time <- Sys.time()
 (time.taken <- round(end.time - start.time,2))
 
 
 writexl::write_xlsx(
   list(clientes_ocasionais_cupom_identificacao = df_venda_ocasional_junho_setembro),
   'output_dashboard_book_crm/13.1.clientes_ocasionais_cupom_identificacao.xlsx'
 )
   
 
df_venda_ocasional_junho_setembro %>% 
  dplyr::rename(customerId = VALORIDENTCLIENTE) %>% 
  dplyr::left_join(df_loja, by = c('LOJA')) %>% 
  dplyr::left_join(
    df_registro_completo %>% 
      dplyr::select(customerId,segment,homeDistrict), by = c('customerId')
    ) %>% 
  count(NOME_LOJA, homeDistrict) %>% 
  arrange(desc(n)) -> EIII
 
 df_bairro_lojaondecomprou <- EIII %>% 
   tidyr::separate_wider_delim(NOME_LOJA, " - ", names = c('COD_LOJA','NOME_LOJA')) %>% 
   dplyr::mutate(LOJA_ONDE_COMPROU = str_to_title(NOME_LOJA)) %>% 
   dplyr::rename(BAIRRO_ONDE_MORA = homeDistrict, TOTAL_VISITAS = n) %>% 
   dplyr::select(BAIRRO_ONDE_MORA, COD_LOJA, LOJA_ONDE_COMPROU, TOTAL_VISITAS)
 
 df_consolidado <- df_bairro_lojaondecomprou %>% 
   dplyr::filter(BAIRRO_ONDE_MORA != '') %>% 
   group_by(LOJA_ONDE_COMPROU) %>% 
   dplyr::summarise(
     COD_LOJA = COD_LOJA,
     BAIRRO_ONDE_MORA = BAIRRO_ONDE_MORA,
     TOTAL_VISITAS = TOTAL_VISITAS,
     PORCENTAGEM_VISITAS = TOTAL_VISITAS/sum(TOTAL_VISITAS)
   ) %>% 
   dplyr::mutate(LOJA = paste(COD_LOJA, '-',LOJA_ONDE_COMPROU))

 writexl::write_xlsx(
   list(clientes_ocasionais_bairro_loja = df_consolidado),
   'output_dashboard_book_crm/13.2.analise_ocasionais_bairro_loja.xlsx'
 )
 
 rm(list=setdiff(ls(), "con"))
 