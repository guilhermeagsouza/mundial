dados <- data.table::fread(
  'dados/2023-10-9-export-00_-_Todos_Os_Clientes-0.csv'
)

clientes_fieis <- dados %>% 
  dplyr::filter(segment %in% c('Fiel')) %>% 
  dplyr::select(customerId)
clientes_fieis <- clientes_fieis$customerId

rm(dados)

################
# Define o número de partes em que deseja dividir os clientes fieis
num_partes <- ceiling(length(clientes_fieis)/20000)

# Inicializa uma lista para armazenar os data frames resultantes
df_lista <- list()

# Divide os clientes ocasionais em partes iguais
partes <- split(clientes_fieis, cut(seq_along(clientes_fieis), num_partes, labels = FALSE))

data_hoje <- lubridate::today()
data_1m_antes <- lubridate::today() - 30

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
v.COD_INTERNO,
SUM(PRECO_TOTAL-isnull(DESCONTO,0)) AS GASTO_TOTAL
FROM VM_INTEGRACAO.dbo.vw_propz v
LEFT JOIN VM_DATABSP.dbo.PRODUTOS p ON v.COD_INTERNO = p.PRODUTO_ID
LEFT JOIN [sgm].[dbo].[secao] s ON p.MERCADOLOGICO2=s.codsec AND p.MERCADOLOGICO3=s.grupo AND p.MERCADOLOGICO4=s.subgrupo
WHERE DATA BETWEEN '2023-09-01' AND '2023-09-30' AND VALORIDENTCLIENTE IN (", paste0("'", partes[[i]], "'", collapse = ","), ")
GROUP BY s.secao, s.nomgrup,s.nom_sub,v.COD_INTERNO
ORDER BY GASTO_TOTAL DESC")
                        
  )
  
  # Adiciona o data frame à lista
  df_lista[[i]] <- df
}

# Combina todos os data frames em um único data frame
df_venda_fiel_mes <- do.call(rbind, df_lista)

df_venda_fiel_mes %<>% 
  group_by(NOME_SECAO, NOME_GRUPO, NOME_SUBGRUPO,COD_INTERNO) %>% 
  dplyr::summarise(GASTO_TOTAL = sum(GASTO_TOTAL)) %>% 
  arrange(desc(GASTO_TOTAL))

# PUXA A VENDA CONSOLIDADA DO MÊS DAS CATEGORIAS
df_venda_mes_categoria <- DBI::dbGetQuery(
  conn = con,
  statement = paste0(
  "SELECT 
  s.secao AS NOME_SECAO,
  s.nomgrup as NOME_GRUPO,
  s.nom_sub as NOME_SUBGRUPO,
  v.COD_INTERNO,
  SUM(PRECO_TOTAL-isnull(DESCONTO,0)) AS VENDA_LIQUIDA
  FROM VM_INTEGRACAO.dbo.vw_propz v
  LEFT JOIN VM_DATABSP.dbo.PRODUTOS p ON v.COD_INTERNO = p.PRODUTO_ID
  LEFT JOIN [sgm].[dbo].[secao] s ON p.MERCADOLOGICO2=s.codsec AND p.MERCADOLOGICO3=s.grupo AND p.MERCADOLOGICO4=s.subgrupo
  WHERE DATA BETWEEN '2023-09-01' AND '2023-09-30'
  GROUP BY s.secao, s.nomgrup,s.nom_sub,v.COD_INTERNO
  ORDER BY VENDA_LIQUIDA DESC"
  )
) %>% 
  dplyr::mutate(DATA_ATUALIZACAO_DADOS = now() %>% lubridate::ymd_hms())

# Tempo final - Consulta ao banco
end.time <- Sys.time()
(time.taken <- round(end.time - start.time,2))

df_produtos <- DBI::dbGetQuery(
  conn = con, 
  statement = "SELECT PRODUTO_ID AS COD_INTERNO, LTRIM(DESCRICAO_COMPLETA) AS DESCRICAO_COMPLETA
FROM VM_DATABSP.dbo.PRODUTOS"
)

##### SKU e Imagem associada a ele #####
df_sku_img <- DBI::dbGetQuery(
  conn = con, 
  statement = "SELECT [codigo] as COD_INTERNO,
      [imagem] as IMAGEM
  FROM [sgm].[dbo].[estoq]
  "
)
df_sku_img %<>% dplyr::mutate(COD_INTERNO = as.numeric(COD_INTERNO))
######

df_venda_consolidado <- df_venda_mes_categoria %>% 
  dplyr::left_join(df_venda_fiel_mes, by = c('NOME_SECAO','NOME_GRUPO','NOME_SUBGRUPO','COD_INTERNO')) %>% 
  dplyr::left_join(df_produtos, by = c('COD_INTERNO')) %>% 
  dplyr::group_by(NOME_SECAO,NOME_GRUPO,NOME_SUBGRUPO,COD_INTERNO) %>% 
  dplyr::mutate(
    GASTO_TOTAL = ifelse(is.na(GASTO_TOTAL),0,GASTO_TOTAL),
    PERCENTUAL_GASTO = GASTO_TOTAL/VENDA_LIQUIDA
  ) %>% 
  # Juntando a base com imagem
  dplyr::left_join(df_sku_img, by = c('COD_INTERNO'))

writexl::write_xlsx(
  list(venda_categoria_fieis = df_venda_consolidado),
  'output_dashboard_crm_interno/5.venda_categoria_fieis.xlsx'
)

# Apaga todos os dados, com exceção da conexão com o banco
rm(list=setdiff(ls(), "con"))