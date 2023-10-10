# Start - Consulta ao Banco
start.time <- Sys.time()

df_venda <- DBI::dbGetQuery(
  conn = con,
  statement = paste0("SELECT 
	DATA,
	NUM_CUPOM,
	VALORIDENTCLIENTE,
	p.PRODUTO_ID AS COD_INTERNO,
	p.DESCRICAO_COMPLETA AS DESCRICAO_COMPLETA,
	s.secao AS NOME_SECAO,
	s.nomgrup as NOME_GRUPO,
	s.nom_sub as NOME_SUBGRUPO,
	QUANTIDADE,
	PRECO_TOTAL,
	ISNULL(DESCONTO,0) AS DESCONTO
FROM VM_INTEGRACAO.dbo.vw_propz v
LEFT JOIN VM_DATABSP.dbo.PRODUTOS p ON v.COD_INTERNO = p.PRODUTO_ID
LEFT JOIN [sgm].[dbo].[secao] s ON p.MERCADOLOGICO2=s.codsec AND p.MERCADOLOGICO3=s.grupo AND p.MERCADOLOGICO4=s.subgrupo
WHERE DATA BETWEEN '2023-09-29' AND '2023-10-01'")
)

# Tempo final - Consulta ao banco
end.time <- Sys.time()
(time.taken <- round(end.time - start.time,2))
#rm(list=setdiff(ls(), "con"))
#df_venda <- readRDS('venda_cupom_2909_0110.Rds')

skus_selecionados <- c(63427,
63428,
63429,
69000,
63425,
35392,
31546,
55712,
54256,
50690,
67987,
33479,
63215,
33480)

df_venda %>% 
  dplyr::mutate(
    CERVEJA_HEINEKEN = ifelse(grepl("^CERVEJA HEINEKEN", DESCRICAO_COMPLETA), "SIM", "NAO"),
    DATA_CUPOM = str_c(DATA,NUM_CUPOM, sep = '_')
    ) -> salva_arquivo

cupons_compraram_cerveja_heineken <- salva_arquivo %>% 
  group_by(DATA_CUPOM) %>% 
  count(CERVEJA_HEINEKEN) %>% 
  dplyr::filter(CERVEJA_HEINEKEN == 'SIM') %>% 
  rename(contagem_cerveja = n)

quantidade_produtos_datacupom <- salva_arquivo %>% 
  filter(DATA_CUPOM %in% c(cupons_compraram_cerveja_heineken$DATA_CUPOM)) %>% 
  group_by(DATA_CUPOM) %>% 
  count()

df_final <- quantidade_produtos_datacupom %>% 
  dplyr::left_join(cupons_compraram_cerveja_heineken, by = c('DATA_CUPOM')) %>% 
  dplyr::mutate(IGUAL = ifelse(n==contagem_cerveja, "SOMENTE CERVEJA","NAO SOMENTE CERVEJA")) %>% 
  dplyr::filter(IGUAL == 'SOMENTE CERVEJA')

# RESULTADO FINAL
df_final %>% 
  dplyr::left_join(
    salva_arquivo %>% dplyr::select(DATA_CUPOM, VALORIDENTCLIENTE) %>% distinct(), by = c('DATA_CUPOM')
    ) %>% 
  dplyr::mutate(TIPOIDENTCLIENTE = ifelse(VALORIDENTCLIENTE !=0,1,0)) %>% 
  arrange(DATA_CUPOM) %>% 
  group_by(TIPOIDENTCLIENTE) %>% 
  count(TIPOIDENTCLIENTE)

# VALOR ARRECADADO CADA PRODUTO
salva_arquivo %>% 
  dplyr::filter(DATA_CUPOM %in% c(df_final$DATA_CUPOM)) %>% 
  dplyr::mutate(PRECO_TOTAL_COM_DESCONTO = PRECO_TOTAL - DESCONTO) %>% 
  group_by(DESCRICAO_COMPLETA, COD_INTERNO) %>% 
  summarise(
    QUANTIDADE_TOTAL = sum(QUANTIDADE), 
    PRECO_TOTAL_COM_DESCONTO = sum(PRECO_TOTAL_COM_DESCONTO)
  ) %>% 
  arrange(desc(PRECO_TOTAL_COM_DESCONTO)) %>% 
  writexl::write_xlsx(
    'Análises Outubro - Guilherme/resultado_final_heineken.xlsx'
  )

rm(list=setdiff(ls(), "con"))
