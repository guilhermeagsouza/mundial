# Atualize primeiro os arquivos nos scripts de Email e SMS

source('scripts_dashboard_book_crm/6.1.resumo_introductionOpportunity_email.R')
source('scripts_dashboard_book_crm/6.2.resumo_reducedSpendOpportunity_email.R')
source('scripts_dashboard_book_crm/6.3.resumo_relationshipopportunity_email.R')
source('scripts_dashboard_book_crm/7.1.resumo_introductionOpportunity_sms.R')
source('scripts_dashboard_book_crm/7.2.resumo_reducedSpendOpportunity_sms.R')
source('scripts_dashboard_book_crm/7.3.resumo_relationshipopportunity_sms.R')

Sys.sleep(60)

source('scripts_dashboard_book_crm/7.4.resumo_consolidado_oportunidades.R')