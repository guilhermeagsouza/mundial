# 0.0 Carrega os pacotes
pacman::p_load(odbc, DBI, tidyverse, hms, DataExplorer, writexl, magrittr, feather)

# 0.1 Conecta ao banco de dados
source(
  'scripts_dashboard_book_crm/0.conecta_banco.R', 
  echo=TRUE, 
  max.deparse.length=60
)

source('fun_uteis/fun_uteis.R')

# DICA! O filtro da Propz será D+1, enquanto no SQL o dia será exatamente.
# Se eu quero extrair a data 2023-07-20 no SQL, na PROPZ o filtro será 2023-07-20 a 2023-07-21
# Se eu quero extrair a data 2023-07-17 a 2023-07-20, na Propz o filtro será 2023-07-17 a 2023-07-21.

# Descrição lojas
df_lojas <- readxl::read_xlsx('dados/descricao_lojas.xlsx')

data_inicial_mes <- '2023-01-01'
data_final_mes <- lubridate::today()

start.time <- Sys.time()  
# Extrai a base de CUPONS
df_total_cupons <- DBI::dbGetQuery(
  conn = con, 
  statement = stringr::str_c("SELECT *
  FROM [VM_LOG].[dbo].[TOTAL_CUPOM]
  WHERE DATA BETWEEN '",data_inicial_mes,"' AND '",data_final_mes, "' AND CANCELADO <> -1 --AND QT_ITENS_CANCEL = 0
  ")
)
end.time <- Sys.time()
(time.taken <- round(end.time - start.time,2))

#df_total_cupons_provisorio %>% feather::write_feather('dados/venda_total_cupom_2023.xlsx')
#df_total_cupons_provisorio <- df_total_cupons

df_total_cupons <- df_total_cupons %>% 
  dplyr::select(LOJA, DATA, TIPOIDENTCLIENTE, VALORIDENTCLIENTE, VENDA_BRUTA, QT_ITENS_VENDA)

df_total_cupons %<>% 
  # TIPOIDENTCLIENTE = 0 -> cliente não identificado; 1: cliente identificado;
  dplyr::filter(TIPOIDENTCLIENTE %in% c(0,1)) %>% 
  # Não considera a CENTRAL - Loja 1
  dplyr::filter(!LOJA %in% c(1)) %>% 
  dplyr::mutate(
    ANO_MES = 100*lubridate::year(DATA) + lubridate::month(DATA),
    LOJA = str_c(LOJA),
    CLIENTENOPROGRAMA = str_c(ifelse(TIPOIDENTCLIENTE == 0, 'FORA','DENTRO'))
  )

#### CONSOLIDADO GERAL ####

# 1. Total de tickets
(total_tickets <- df_total_cupons %>% 
   dplyr::group_by(ANO_MES) %>% 
   dplyr::summarise(TICKETS = n())
)

# 2. Total de tickets identificados (1: CPF; 12: CNPJ; 13:??)
(total_tickets_identificados <- df_total_cupons %>% 
    dplyr::filter(!TIPOIDENTCLIENTE %in% c(0)) %>% 
    dplyr::group_by(ANO_MES) %>% 
    dplyr::summarise(TICKETS_IDENTIFICADOS = n())
)

# Porcentagem de tickets identificados
total_tickets_identificados$TICKETS_IDENTIFICADOS/total_tickets$TICKETS*100


# 3. Total de Faturamento
(total_faturamento <- df_total_cupons %>% 
    dplyr::group_by(ANO_MES) %>% 
    dplyr::summarise(VENDA_TOTAL_BRUTA = sum(VENDA_BRUTA, na.rm = TRUE))
)

# 4. Total de Faturamento identificado
total_faturamento_identificado <- df_total_cupons %>% 
  dplyr::filter(!TIPOIDENTCLIENTE %in% c(0)) %>% 
  dplyr::group_by(ANO_MES) %>% 
  dplyr::summarise(VENDA_TOTAL_BRUTA = sum(VENDA_BRUTA, na.rm = TRUE))

# Porcentagem de faturamento identificado,
total_faturamento_identificado$VENDA_TOTAL_BRUTA/total_faturamento$VENDA_TOTAL_BRUTA*100


#### INÍCIO DO CONSOLIDADO POR LOJA ####


# 1. Total de tickets
total_tickets_p_loja <- df_total_cupons %>% 
  dplyr::group_by(LOJA,ANO_MES) %>% 
  dplyr::summarise(numero_tickets = n()) %>% 
  dplyr::left_join(df_lojas, by = 'LOJA') %>% 
  dplyr::arrange(desc(numero_tickets))
total_tickets_p_loja

# 2. Total de tickets identificados (1: CPF; 12: CNPJ; 13:??)
total_tickets_identificados_p_loja <- df_total_cupons %>% 
  dplyr::filter(!TIPOIDENTCLIENTE %in% c(0)) %>% 
  dplyr::group_by(LOJA,ANO_MES) %>% 
  dplyr::summarise(numero_tickets_identificados = n()) %>% 
  dplyr::left_join(df_lojas, by = 'LOJA')
total_tickets_identificados_p_loja

# Porcentagem de tickets identificados
df_consolidado_tickets <- total_tickets_identificados_p_loja %>% 
  dplyr::left_join(total_tickets_p_loja, by = c('LOJA', 'ANO_MES','NOME_LOJA')) %>% 
  dplyr::mutate(tickets_identificados_porcentagem = numero_tickets_identificados/numero_tickets)
df_consolidado_tickets


# 3. Total de Faturamento
total_faturamento_p_loja <- df_total_cupons %>% 
  dplyr::group_by(LOJA,ANO_MES) %>% 
  dplyr::summarise(VENDA_TOTAL_BRUTA = sum(VENDA_BRUTA, na.rm = TRUE)) %>% 
  dplyr::left_join(df_lojas, by = c('LOJA')) %>% 
  arrange(desc(VENDA_TOTAL_BRUTA))
total_faturamento_p_loja

# 4. Total de Faturamento identificado
total_faturamento_identificado_p_loja <- df_total_cupons %>% 
    dplyr::filter(!TIPOIDENTCLIENTE %in% c(0)) %>% 
  dplyr::group_by(LOJA,ANO_MES) %>% 
    dplyr::summarise(VENDA_TOTAL_BRUTA_IDENTIFICADA = sum(VENDA_BRUTA, na.rm = TRUE)) %>% 
  dplyr::left_join(df_lojas, by = c('LOJA')) %>% 
  arrange(desc(VENDA_TOTAL_BRUTA_IDENTIFICADA))
total_faturamento_identificado_p_loja


# Porcentagem de faturamento identificados
df_consolidado_faturamento <- total_faturamento_p_loja %>% 
  dplyr::left_join(total_faturamento_identificado_p_loja, by = c('LOJA', 'ANO_MES','NOME_LOJA')) %>% 
  dplyr::mutate(faturamento_identificados_porcentagem = VENDA_TOTAL_BRUTA_IDENTIFICADA/VENDA_TOTAL_BRUTA)
df_consolidado_faturamento

df_consolidado_geral <- df_consolidado_tickets %>% 
  dplyr::left_join(df_consolidado_faturamento, by = c('LOJA', 'ANO_MES','NOME_LOJA')) %>% 
  dplyr::select(
    NOME_LOJA, 
    ANO_MES,
    VENDA_TOTAL_BRUTA, 
    VENDA_TOTAL_BRUTA_IDENTIFICADA,
    faturamento_identificados_porcentagem,
    numero_tickets, 
    numero_tickets_identificados,
    tickets_identificados_porcentagem
    )

# CONSOLIDANDO A BASE E VIRANDO: PAINEL DE IDENTIFICAÇÃO (Disponível na PROPZ)
painel_identificacao <- df_consolidado_geral %>% 
  dplyr::arrange(desc(VENDA_TOTAL_BRUTA)) %>% 
  dplyr::filter(!is.na(NOME_LOJA))
  

writexl::write_xlsx(
  list(painel_identificacao = painel_identificacao),
  "output_dashboard_book_crm/5.1.painel_identificacao.xlsx"
  )

df_ticketmedio_por_loja <- df_total_cupons %>% 
  dplyr::group_by(LOJA, CLIENTENOPROGRAMA) %>% 
  dplyr::summarise(
    VENDA_BRUTA_TOTAL = sum(VENDA_BRUTA),
    TICKETS = n()
    ) %>% 
  ungroup() %>% 
  dplyr::left_join(df_lojas, by = c('LOJA')) %>% 
  dplyr::select(NOME_LOJA, CLIENTENOPROGRAMA, VENDA_BRUTA_TOTAL, TICKETS)
df_ticketmedio_por_loja


df_ticketmedio_por_loja_consolidado <- df_ticketmedio_por_loja %>% 
  dplyr::mutate(TICKET_MEDIO = VENDA_BRUTA_TOTAL/TICKETS) %>%
  dplyr::select(NOME_LOJA, CLIENTENOPROGRAMA, TICKET_MEDIO) %>% 
  na.omit() %>% 
  tidyr::pivot_wider(
    names_from = CLIENTENOPROGRAMA,
    values_from = TICKET_MEDIO,
    values_fill = 0
    ) %>% 
  dplyr::mutate(DIFERENCA_TICKETS = DENTRO - FORA) %>% 
  dplyr::arrange(desc(DENTRO))

df_ticketmedio_por_loja_consolidado %>% 
  writexl::write_xlsx('output_dashboard_book_crm/5.2.ticketmedio_por_loja.xlsx')

rm(list=setdiff(ls(), "con"))
  
