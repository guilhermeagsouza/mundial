end <- list.files(path = 'dados/refazer_cadastros/',full.names = TRUE)
teste1 <- 'Meu Mundial - Versão 6 - LP Personalizada_Leads_2023-08-03_2023-08-03 (1).xls'

df_1 <- readxl::read_xlsx('dados/refazer_cadastros/Meu Mundial - Versão 6 - LP Personalizada_Leads_2023-08-03_2023-08-03 (1).xlsx')

df_1 %<>% 
  dplyr::mutate(
    gender = stringr::str_to_upper(gender),
    gender = ifelse(gender %in% c('FEMALE','FEMENINO','FEMININO','F'), 'F','M'),
    validacao_cpf = ifelse(grepl("^\\d{3}\\.\\d{3}\\.\\d{3}-\\d{2}$", CPF), CPF, ""),
    validacao_celular = ifelse(str_detect(Celular, "^\\+55[0-9]{2}[0-9]{9}$"), Celular, "")
  )


df_1 %>% 
  head() %>% View


Celular <-c ("+5521997369688","1234567890")
ifelse(str_detect(Celular, "^\\+5521[0-9]{9}$"), Celular, "")
