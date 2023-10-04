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

clientes_fieis <- dados %>% 
  dplyr::filter(segment %in% c('Fiel')) %>% 
  dplyr::select(customerId)
clientes_fieis <- clientes_fieis$customerId

################
# Define o número de partes em que deseja dividir os clientes ocasionais
num_partes <- 25

# Inicializa uma lista para armazenar os data frames resultantes
df_lista <- list()

# Divide os clientes ocasionais em partes iguais
partes <- split(clientes_fieis, cut(seq_along(clientes_fieis), num_partes, labels = FALSE))

# Start - Consulta ao Banco
start.time <- Sys.time()

# Loop para criar os data frames
for (i in 1:num_partes) {
  df <- DBI::dbGetQuery(
    conn = con,
    statement = paste0("SELECT 
s.secao AS NOME_SECAO,
s.nomgrup as NOME_GRUPO,
s.nom_sub as NOME_SUBGRUPO,
SUM(PRECO_TOTAL-isnull(DESCONTO,0)) AS GASTO_TOTAL
FROM VM_INTEGRACAO.dbo.vw_propz v
LEFT JOIN VM_DATABSP.dbo.PRODUTOS p ON v.COD_INTERNO = p.PRODUTO_ID
LEFT JOIN [sgm].[dbo].[secao] s ON p.MERCADOLOGICO2=s.codsec AND p.MERCADOLOGICO3=s.grupo AND p.MERCADOLOGICO4=s.subgrupo
WHERE DATA BETWEEN '2023-09-01' AND '2023-09-28' AND VALORIDENTCLIENTE IN (", paste0("'", partes[[i]], "'", collapse = ","), ")
GROUP BY s.secao, s.nomgrup,s.nom_sub
ORDER BY GASTO_TOTAL DESC")
                        
  )
  
  # Adiciona o data frame à lista
  df_lista[[i]] <- df
}

# Combina todos os data frames em um único data frame
df_venda_fiel_mes <- do.call(rbind, df_lista)

df_venda_fiel_mes %<>% 
  group_by(NOME_SECAO, NOME_GRUPO, NOME_SUBGRUPO) %>% 
  dplyr::summarise(GASTO_TOTAL = sum(GASTO_TOTAL)) %>% 
  arrange(desc(GASTO_TOTAL))

# Tempo final - Consulta ao banco
end.time <- Sys.time()
(time.taken <- round(end.time - start.time,2))


