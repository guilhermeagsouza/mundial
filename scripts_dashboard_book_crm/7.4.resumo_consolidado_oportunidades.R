# 0.0 Carrega os pacotes
pacman::p_load(odbc, DBI, tidyverse, hms, DataExplorer, writexl, magrittr, feather)

# 0.1 Conecta ao banco de dados
source(
  'scripts_dashboard_book_crm/0.conecta_banco.R', 
  echo=TRUE, 
  max.deparse.length=60
)

email_introduction <- readxl::read_xlsx('output_dashboard_book_crm/6.1.output_introductionOpportunity_email.xlsx') %>% 
  dplyr::mutate(tipo_contato = 'email') %>% 
  dplyr::rename(n_email = n)

email_reduced <- readxl::read_xlsx('output_dashboard_book_crm/6.2.output_reducedSpendOpportunity_email.xlsx') %>% 
  dplyr::mutate(tipo_contato = 'email', oportunidade = 'reducedspend') %>% 
  dplyr::rename(n_email = n)

email_relationship <- readxl::read_xlsx('output_dashboard_book_crm/6.3.output_relationshipOpportunity_email.xlsx') %>% 
  dplyr::mutate(tipo_contato = 'email') %>% 
  dplyr::rename(n_email = n)

sms_introduction <- readxl::read_xlsx('output_dashboard_book_crm/7.1.output_introductionOpportunity_sms.xlsx') %>% 
  dplyr::mutate(tipo_contato = 'sms', oportunidade = 'introduction') %>% 
  dplyr::rename(n_sms = n)

sms_reduced <- readxl::read_xlsx('output_dashboard_book_crm/7.2.output_reducedSpendOpportunity_sms.xlsx') %>% 
  dplyr::mutate(tipo_contato = 'sms', oportunidade = 'reducedspend') %>% 
  dplyr::rename(n_sms = n)

sms_relationship <- readxl::read_xlsx('output_dashboard_book_crm/7.3.output_relationshipOpportunity_sms.xlsx') %>% 
  dplyr::mutate(tipo_contato = 'sms', oportunidade = 'relationship') %>% 
  dplyr::rename(n_sms = n)

##### CONSOLIDAÇÃO #####
df_introduction <- email_introduction %>% 
  dplyr::full_join(sms_introduction, by = c('secao','nom_grup','nom_sub','segmento','oportunidade')) %>% 
  dplyr::distinct() %>% 
  dplyr::group_by(secao,nom_grup,nom_sub,segmento,oportunidade) %>% 
  dplyr::summarise(n_email = sum(n_email, na.rm = TRUE), n_sms = sum(n_sms, na.rm = TRUE)) %>% 
  dplyr::arrange(desc(n_email))

df_reduced <- email_reduced %>% 
  dplyr::full_join(sms_reduced, by = c('secao','nom_grup','nom_sub','segmento','oportunidade')) %>% 
  dplyr::distinct() %>% 
  dplyr::group_by(secao,nom_grup,nom_sub,segmento,oportunidade) %>% 
  dplyr::summarise(n_email = sum(n_email, na.rm = TRUE), n_sms = sum(n_sms, na.rm = TRUE)) %>% 
  dplyr::arrange(desc(n_email))

df_relationship <- email_relationship %>% 
  dplyr::full_join(sms_relationship, by = c('secao','nom_grup','nom_sub','segmento','oportunidade')) %>% 
  dplyr::distinct() %>% 
  dplyr::group_by(secao,nom_grup,nom_sub,segmento,oportunidade) %>% 
  dplyr::summarise(n_email = sum(n_email, na.rm = TRUE), n_sms = sum(n_sms, na.rm = TRUE)) %>% 
  dplyr::arrange(desc(n_email))

# Salva a base na pasta output
writexl::write_xlsx(
  list(introduction_consolidado = df_introduction,
       reducedspend_consolidado = df_reduced,
       relationship_consolidado = df_relationship),
  'output_dashboard_book_crm/7.4.output_oportunidade_email_sms.xlsx'
)

rm(list=setdiff(ls(), "con"))
