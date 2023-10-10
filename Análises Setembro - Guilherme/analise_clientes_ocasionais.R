df_clientes_ocasionais <- readxl::read_xlsx(
  'Clientes Ocasionais - CPF e CUPONS.xlsx',sheet = 'Consolidada'
)
df_clientes_ocasionais %>% head()

df_clientes_ocasionais %<>% 
  dplyr::mutate(DIA = lubridate::day(DATA)) %>% 
  dplyr::mutate(PERIODO_DIA = case_when(
    DIA >= 1 & DIA <= 10 ~ "Dia 1 ao 10",
    DIA >= 11 & DIA <= 20 ~ "Dia 11 ao 20",
    DIA >= 21 & DIA <= 31 ~ "Dia 21 ao 31",
    TRUE ~ NA_character_
  ))

# TICKET MÉDIO
df_clientes_ocasionais %>% 
  dplyr::group_by(PERIODO_DIA) %>% 
  dplyr::summarise(TICKET_MEDIO = mean(PRECO_TOTAL))

df_clientes_ocasionais %>% 
  dplyr::rename(CPF_ou_CNPJ = VALORIDENTCLIENTE) %>% 
  dplyr::select(CPF_ou_CNPJ, PERIODO_DIA) %>% 
  dplyr::distinct() %>% 
  dplyr::arrange(desc(CPF_ou_CNPJ)) %>% 
  writexl::write_xlsx('cpf_ou_cnpj_clientes_ocasionais.xlsx')

# QUANTOS CPFs ou CNPJs distintos
df_clientes_ocasionais %>% 
 dplyr::rename(CPF_ou_CNPJ = VALORIDENTCLIENTE) %>% 
 distinct(CPF_ou_CNPJ) %>% 
  nrow()

# PEGA A BASE
analise_cpf_cnpj_categorias <- readxl::read_xlsx(
'Análises Setembro - Guilherme/analise_cpf_cnpj_categorias.xlsx'
)

# 67911
df_final <- df_clientes_ocasionais %>% 
  dplyr::filter(VALORIDENTCLIENTE %in% analise_cpf_cnpj_categorias$customerId)

df_final %>% 
  dplyr::group_by(VALORIDENTCLIENTE) %>% 
  dplyr::summarise(ULTIMA_DATA_COMPRA = max(DATA)) %>% 
  dplyr::rename(customerId = VALORIDENTCLIENTE) %>% 
  dplyr::mutate(customerId = str_c(customerId)) %>% 
  dplyr::left_join(analise_cpf_cnpj_categorias, by = c('customerId')) %>% 
  dplyr::mutate(
    ULTIMA_DATA_COMPRA = ymd_hms(ULTIMA_DATA_COMPRA), 
    ULTIMA_DATA_COMPRA = ymd(ULTIMA_DATA_COMPRA)
  ) %>% 
  writexl::write_xlsx(
    'Análises Setembro - Guilherme/analise_final_cpf_cnpj_categorias.xlsx'
  )



# QUANTIDADE QUE O CLIENTE VEM NO MÊS NO MUNDIAL
df_clientes_ocasionais %>% 
  dplyr::rename(CPF_ou_CNPJ = VALORIDENTCLIENTE) %>% 
  mutate(MES = lubridate::month(DATA)) %>% 
  dplyr::select(CPF_ou_CNPJ, PERIODO_DIA,MES) %>% 
  dplyr::group_by(CPF_ou_CNPJ) %>% 
  count(PERIODO_DIA, MES) %>% 
  dplyr::arrange(desc(CPF_ou_CNPJ)) %>% 
  View

df_clientes_ocasionais %>% 
  dplyr::rename(CPF_ou_CNPJ = VALORIDENTCLIENTE) %>% 
  mutate(MES = lubridate::month(DATA)) %>% 
  dplyr::select(CPF_ou_CNPJ, PERIODO_DIA,MES) %>% 
  dplyr::group_by(CPF_ou_CNPJ) %>% 
  count(MES) %>% 
  dplyr::arrange(desc(CPF_ou_CNPJ)) %>% 
  View

rm(list=setdiff(ls(), "con"))
