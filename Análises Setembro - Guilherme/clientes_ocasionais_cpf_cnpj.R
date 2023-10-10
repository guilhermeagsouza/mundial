df_clientes_ocasionais <- readxl::read_xlsx(
  'Clientes Ocasionais - CPF e CUPONS.xlsx', sheet = 'Consolidada'
)

df_clientes_ocasionais %<>% 
  dplyr::mutate(DIA = lubridate::day(DATA)) %>% 
  dplyr::mutate(
    PERIODO_DIA = case_when(
      DIA >= 1 & DIA <= 10 ~ "Dia 1 ao 10",
      DIA >= 11 & DIA <= 20 ~ "Dia 11 ao 20",
      DIA >= 21 & DIA <= 31 ~ "Dia 21 ao 31",
      TRUE ~ NA_character_
  ))

df_clientes_ocasionais %>% 
  dplyr::rename(CPF_ou_CNPJ = VALORIDENTCLIENTE) %>% 
  mutate(MES = lubridate::month(DATA)) %>% 
  dplyr::select(CPF_ou_CNPJ, PERIODO_DIA,MES) %>% 
  dplyr::group_by(CPF_ou_CNPJ) %>% 
  count(MES) %>% 
  dplyr::arrange(desc(CPF_ou_CNPJ)) %>% 
  ungroup() %>% 
  group_by(CPF_ou_CNPJ) %>% 
  summarise(MEDIA_DIAS_EM_LOJA = mean(n)) %>% 
  dplyr::filter(MEDIA_DIAS_EM_LOJA == 1) %>% 
  arrange(desc(MEDIA_DIAS_EM_LOJA)) %>% 
  writexl::write_xlsx('Análises Setembro - Guilherme/frequencia_1_ida_mercado_mes.xlsx')

# 1 vez em setembro ida ao mercado até o dia 15
