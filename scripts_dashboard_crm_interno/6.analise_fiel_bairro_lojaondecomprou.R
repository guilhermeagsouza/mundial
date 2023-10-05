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

clientes_fiel <- df_registro_completo %>% 
  dplyr::filter(segment %in% c('Fiel')) %>% 
  dplyr::select(customerId)
clientes_fiel <- clientes_fiel$customerId

# Análise do cliente ocasional
df_registro_completo %>% 
  dplyr::filter(segment %in% c('Fiel') & homeDistrict !='') %>% 
  dplyr::group_by(segment) %>% 
  dplyr::count(homeDistrict) %>% 
  dplyr::arrange(desc(n)) %>% View

################
# Define o número de partes em que deseja dividir os clientes fiel
num_partes <- 12

# Inicializa uma lista para armazenar os data frames resultantes
df_lista <- list()

# Divide os clientes fiel em partes iguais
partes <- split(clientes_fiel, cut(seq_along(clientes_fiel), num_partes, labels = FALSE))

# Start - Consulta ao Banco
start.time <- Sys.time()

# Loop para criar os data frames
for (i in 1:num_partes) {
  df <- DBI::dbGetQuery(
    conn = con,
    statement = paste0("SELECT DISTINCT DATA, NUM_CUPOM, LOJA, VALORIDENTCLIENTE 
                        FROM VM_INTEGRACAO.dbo.vw_propz v 
                        WHERE DATA BETWEEN '2023-06-30' AND '2023-09-30' AND 
                        VALORIDENTCLIENTE IN (", paste0("'", partes[[i]], "'", collapse = ","), ")")
  )
  
  # Adiciona o data frame à lista
  df_lista[[i]] <- df
}

# Combina todos os data frames em um único data frame
df_venda_fiel_junho_setembro <- do.call(rbind, df_lista)

# Tempo final - Consulta ao banco
end.time <- Sys.time()
(time.taken <- round(end.time - start.time,2))


writexl::write_xlsx(
  list(clientes_fiel_cupom_identificacao = df_venda_ocasional_junho_setembro),
  'output_dashboard_crm_interno/6.1.clientes_fiel_cupom_identificacao.xlsx'
)


df_venda_fiel_junho_setembro %>% 
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
  list(clientes_fiel_bairro_loja = df_consolidado),
  'output_dashboard_crm_interno/6.2.analise_fiel_bairro_loja.xlsx'
)

rm(list=setdiff(ls(), "con"))
