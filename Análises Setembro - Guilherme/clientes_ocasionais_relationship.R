start.time <- Sys.time()

df_extracao <- DBI::dbGetQuery(
  conn = con,
  statement = "SELECT 
  DATA,
NUM_CUPOM,
VALORIDENTCLIENTE,
SUM(QUANTIDADE) AS QUANTIDADE_TOTAL,
SUM(PRECO_TOTAL) AS PRECO_TOTAL,
SUM(PRECO_TOTAL)/SUM(QUANTIDADE) AS PRECO_MEDIO
FROM VM_INTEGRACAO.dbo.vw_propz v
LEFT JOIN VM_DATABSP.dbo.PRODUTOS p ON v.COD_INTERNO = p.PRODUTO_ID
LEFT JOIN [sgm].[dbo].[secao] s ON p.MERCADOLOGICO2=s.codsec AND p.MERCADOLOGICO3=s.grupo AND p.MERCADOLOGICO3=s.subgrupo
WHERE DATA BETWEEN '2023-06-21' AND '2023-09-21'
GROUP BY DATA, NUM_CUPOM, VALORIDENTCLIENTE
ORDER BY SUM(PRECO_TOTAL) DESC
;
  "
)

#df_extracao %>% write_rds('dados/extracao_venda_18062023_18092023.Rds')
end.time <- Sys.time()
(time.taken <- round(end.time - start.time,2))

# A variável customerId precisa 
df_todos_clientes <- data.table::fread('dados/2023-9-21-export-00_-_Todos_Os_Clientes-0.csv')
df_todos_clientes %<>% select(customerId, segment, relationshipOpportunity)

df_clientes_ocasionais <- df_todos_clientes %>% 
  dplyr::filter(segment %in% 'Ocasional')

# Não precisamos mais da base da Propz
rm(df_todos_clientes)

# Extrai os cupons e os CPFs associados
df_extracao_clientes_ocasionais <- df_extracao %>% 
  dplyr::filter(VALORIDENTCLIENTE %in% c(df_clientes_ocasionais$customerId))

clientes_ocasionais <- df_extracao_clientes_ocasionais$VALORIDENTCLIENTE %>% unique()
cientes_oc_1 <- clientes_ocasionais[1:20000]
cientes_oc_2 <- clientes_ocasionais[20001:40000]
cientes_oc_3 <- clientes_ocasionais[40001:60000]
cientes_oc_4 <- clientes_ocasionais[60001:80000]
cientes_oc_5 <- clientes_ocasionais[80001:length(clientes_ocasionais)]

# Não precisamos mais da base de extração completa
rm(df_extracao)

#for (i in 1:20000) {
#}

df_extracao <- DBI::dbGetQuery(
  conn = con,
  statement = paste0("SELECT DATA, NUM_CUPOM, VALORIDENTCLIENTE 
                     FROM VM_INTEGRACAO.dbo.vw_propz v 
                     WHERE DATA BETWEEN '2023-06-21' AND '2023-09-21' AND 
                     VALORIDENTCLIENTE IN (",paste0("'",cientes_oc_5,"'",collapse = ","),")")
) %>% 
  dplyr::distinct()

df_clientes_1_unica_compra <- df_extracao %>% 
  dplyr::group_by(VALORIDENTCLIENTE) %>% 
  count() %>% 
  dplyr::filter(n == 1)

leva_5_clientes_ocasionais <- df_extracao %>% 
  dplyr::filter(VALORIDENTCLIENTE %in% c(df_clientes_1_unica_compra$VALORIDENTCLIENTE)) %>% 
  dplyr::group_by(VALORIDENTCLIENTE) %>% 
  dplyr::summarise(DATA = max(DATA))

leva_4_clientes_ocasionais <- df_extracao %>% 
  dplyr::filter(VALORIDENTCLIENTE %in% c(df_clientes_1_unica_compra$VALORIDENTCLIENTE)) %>% 
  dplyr::group_by(VALORIDENTCLIENTE) %>% 
  dplyr::summarise(DATA = max(DATA))

leva_3_clientes_ocasionais <- df_extracao %>% 
  dplyr::filter(VALORIDENTCLIENTE %in% c(df_clientes_1_unica_compra$VALORIDENTCLIENTE)) %>% 
  dplyr::group_by(VALORIDENTCLIENTE) %>% 
  dplyr::summarise(DATA = max(DATA))

leva_2_clientes_ocasionais <- df_extracao %>% 
  dplyr::filter(VALORIDENTCLIENTE %in% c(df_clientes_1_unica_compra$VALORIDENTCLIENTE)) %>% 
  dplyr::group_by(VALORIDENTCLIENTE) %>% 
  dplyr::summarise(DATA = max(DATA))

leva_1_clientes_ocasionais <- df_extracao %>% 
  dplyr::filter(VALORIDENTCLIENTE %in% c(df_clientes_1_unica_compra$VALORIDENTCLIENTE)) %>% 
  dplyr::group_by(VALORIDENTCLIENTE) %>% 
  dplyr::summarise(DATA = max(DATA))


leva_clientes_ocasionais <- leva_1_clientes_ocasionais %>% 
  rbind(leva_2_clientes_ocasionais,leva_3_clientes_ocasionais,leva_4_clientes_ocasionais) %>% 
  dplyr::mutate(ULTIMA_DATA_COMPRA = lubridate::as_datetime(DATA))

###########
# Defina uma função para extrair a primeira palavra
extrair_primeira_palavra <- function(text) {
  primeira_palavra <- str_extract(text, "\\w+")
  return(primeira_palavra)
}
###########

leva_clientes_ocasionais %<>%
  dplyr::rename(customerId = VALORIDENTCLIENTE) %>% 
  dplyr::select(customerId, ULTIMA_DATA_COMPRA)

########## APLICA-SE A FUNÇÃO PARA EXTRAÇÃO DAS CATEGORIAS ##########
relationshipOpportunity <- df_clientes_ocasionais %>% 
  dplyr::filter(!is.na(relationshipOpportunity) & !relationshipOpportunity == '')


# Início da função
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
##########

final_df %<>% dplyr::mutate(customerId = relationshipOpportunity$customerId)

### FINAL
leva_clientes_ocasionais %>% 
  dplyr::mutate(ULTIMA_DATA_COMPRA = lubridate::ymd(ULTIMA_DATA_COMPRA)) %>% 
  dplyr::arrange(desc(ULTIMA_DATA_COMPRA)) %>% 
  dplyr::left_join(final_df, by = c('customerId')) %>%
  dplyr::filter(!is.na(categoria1)) %>% 
  dplyr::rename(SECAO = categoria1, GRUPO = categoria2, SUBGRUPO = categoria3) %>% 
  writexl::write_xlsx('Análises Setembro - Guilherme/analise_ocasionais_automatizada.xlsx')

rm(list=setdiff(ls(), "con"))

