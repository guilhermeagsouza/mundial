# 0. Conecta ao banco de dados
source(
  'scripts_dashboard_book_crm/0.conecta_banco.R', 
  echo=TRUE, 
  max.deparse.length=60
  )

# CADASTRO SKUS
df_cadastro_skus <- readxl::read_xlsx('dados/cadastro_skus.xlsx') %>% 
  dplyr::select(secao, grupo, subgrupo, codigo) %>% 
  dplyr::mutate(
    codigo = as.numeric(codigo),
    secao = as.numeric(secao),
    grupo = as.numeric(grupo),
    subgrupo = as.numeric(subgrupo)
  ) %>% 
  dplyr::rename(COD_INTERNO = codigo)
names(df_cadastro_skus) <- stringr::str_to_upper(names(df_cadastro_skus))

df_secao <- DBI::dbGetQuery(
  conn = con,
  statement = "select codsec AS SECAO, secao as NOME_SECAO, grupo as GRUPO, nomgrup as NOME_GRUPO, subgrupo as SUBGRUPO, nom_sub as NOME_SUBGRUPO
  from [sgm].[dbo].[secao]
  "
) %>% 
  dplyr::mutate(
    SECAO = as.numeric(SECAO), 
    GRUPO = as.numeric(GRUPO), 
    SUBGRUPO = as.numeric(SUBGRUPO)
  ) %>% 
  dplyr::mutate_if(is.factor, trimws)

df_secoes_skus <- df_cadastro_skus %>% 
  dplyr::left_join(df_secao, by = c('SECAO','GRUPO','SUBGRUPO')) %>% 
  dplyr::mutate(
    SECAO = as.numeric(SECAO), 
    GRUPO = as.numeric(GRUPO), 
    SUBGRUPO = as.numeric(SUBGRUPO)
  ) %>% 
  dplyr::mutate_if(is.factor, trimws)
  

# OFERTA PERSONALIZADA
df_promocoes_casadinha_mes_atual <- DBI::dbGetQuery(
  conn = con, 
  statement = "SELECT DISTINCT CODIGO AS CODIGO_CASADINHA, DESCRICAO AS LEGENDA, DATAINICIAL,DATAFINAL FROM [VM_DATABSP].dbo.[CASADINHA] 
WHERE (DATAINICIAL >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0) -- Início do mês atual
       OR DATAINICIAL >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 4, 0)) -- Início do mês passado 4m pra trás
ORDER BY CODIGO_CASADINHA, DATAINICIAL
  "
)

# OFERTA PERSONALIZADA
df_promocoes_casadinha_mes_atual <- DBI::dbGetQuery(
  conn = con, 
  statement = "SELECT DISTINCT casadinha.CODIGO AS CODIGO_CASADINHA,
  DESCRICAO AS LEGENDA, DATAINICIAL, DATAFINAL
  FROM VM_DATABSP.dbo.CASADINHA casadinha
left join VM_DATABSP.dbo.CASADINHA_LOJA casadinha_loja on casadinha.CODIGO=casadinha_loja.CODIGOCASADINHA 
left join VM_INTEGRACAO.DBO.VW_CASADINHA_VENDA view_venda_cas on casadinha.CODIGO=view_venda_cas.COD_CASADINHA and 
casadinha_loja.CODIGO=view_venda_cas.LOJA
where data between '2023-09-01' and '2023-09-30' and CUPONAGEM=10
  "
)

df_resultado_promo_casadinha_mes_atual <- DBI::dbGetQuery(
  conn = con, 
  statement = "SELECT
LEGENDA,
COD_CASADINHA AS CODIGO_CASADINHA,
sum(qt_promocao) as QUANTIDADE_PROMOCAO,
sum(preco_venda) - ISNULL(SUM(DESCONTO), 0) as TOTAL_VENDA,
sum(DESCONTO) as DESCONTO,
COUNT (DISTINCT LOJA) as LOJAS_PARTICIPANTES
FROM VM_INTEGRACAO.DBO.VW_CASADINHA_VENDA
WHERE DATA >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0)
GROUP BY COD_CASADINHA, LEGENDA
ORDER BY TOTAL_VENDA DESC
  "
)

df_resultado_promo_casadinha_mes_atual <- DBI::dbGetQuery(
  conn = con, 
  statement = "SELECT LEGENDA,COD_CASADINHA AS CODIGO_CASADINHA, sum(qt_promocao) as QUANTIDADE_PROMOCAO, sum(PRECO_VENDA) - ISNULL(SUM(DESCONTO), 0) AS TOTAL_VENDA, sum(DESCONTO) as DESCONTO, 
COUNT(DISTINCT LOJA) AS LOJAS_PARTICIPANTES
  FROM VM_DATABSP.dbo.CASADINHA casadinha
left join VM_DATABSP.dbo.CASADINHA_LOJA casadinha_loja on casadinha.CODIGO=casadinha_loja.CODIGOCASADINHA 
left join VM_INTEGRACAO.DBO.VW_CASADINHA_VENDA view_venda_cas on casadinha.CODIGO=view_venda_cas.COD_CASADINHA and 
casadinha_loja.CODIGO=view_venda_cas.LOJA
where data between '2023-09-01' and '2023-09-30' and CUPONAGEM=10
GROUP BY COD_CASADINHA,LEGENDA
ORDER BY TOTAL_VENDA DESC
  "
)

df_consolidado <- df_resultado_promo_casadinha_mes_atual %>% 
  dplyr::left_join(
    df_promocoes_casadinha_mes_atual,by = c('CODIGO_CASADINHA','LEGENDA'))

# PEGA A CASADINHA COM MAIOR VENDA (PARA EVITAR PROMOÇÕES DUPLICADAS, PORÉM COM CÓDIGOS DIFERENTES)
df_consolidado %<>% 
  dplyr::group_by(LEGENDA) %>% 
  dplyr::slice(which.max(TOTAL_VENDA))

writexl::write_xlsx(
  list(promo_casadinha_mes_atual = df_consolidado),
  'output_dashboard_book_crm/3.1.promo_casadinha_mes_atual.xlsx'
)

tbl_geral <- DBI::dbGetQuery(
  conn = con, 
  statement = " SELECT
v.COD_INTERNO,
p.DESCRICAO_COMPLETA,
v.LOJA,
v.LEGENDA,
v.COD_CASADINHA,
sum(qt_promocao) as QUANTIDADE_PROMOCAO,
sum(preco_venda) - ISNULL(SUM(DESCONTO), 0) as TOTAL_VENDA,
sum(DESCONTO) as DESCONTO
FROM VM_INTEGRACAO.DBO.VW_CASADINHA_VENDA v LEFT JOIN VM_DATABSP.dbo.PRODUTOS p ON v.COD_INTERNO = p.PRODUTO_ID
WHERE DATA >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0)
GROUP BY v.COD_INTERNO,p.DESCRICAO_COMPLETA,v.COD_CASADINHA, v.LEGENDA,v.LOJA
ORDER BY TOTAL_VENDA DESC
  "
)

tbl_geral <- DBI::dbGetQuery(
  conn = con, 
  statement = "SELECT view_venda_cas.COD_INTERNO AS COD_INTERNO, view_venda_cas.DESCRICAO_COMPLETA, view_venda_cas.LOJA,
LEGENDA,casadinha.CODIGO, sum(qt_promocao) as QUANTIDADE_PROMOCAO, 
sum(PRECO_VENDA) - ISNULL(SUM(DESCONTO), 0) as TOTAL_VENDA, sum(DESCONTO) as DESCONTO, 
COUNT(DISTINCT LOJA) AS LOJAS_PARTICIPANTES
  FROM VM_DATABSP.dbo.CASADINHA casadinha
left join VM_DATABSP.dbo.CASADINHA_LOJA casadinha_loja on casadinha.CODIGO=casadinha_loja.CODIGOCASADINHA 
left join VM_INTEGRACAO.DBO.VW_CASADINHA_VENDA view_venda_cas on casadinha.CODIGO=view_venda_cas.COD_CASADINHA and 
casadinha_loja.CODIGO=view_venda_cas.LOJA
LEFT JOIN VM_DATABSP.dbo.PRODUTOS p ON  view_venda_cas.COD_INTERNO= p.PRODUTO_ID
where data between '2023-09-01' and '2023-09-30' and CUPONAGEM=10
GROUP BY view_venda_cas.COD_INTERNO, view_venda_cas.DESCRICAO_COMPLETA, casadinha.CODIGO,LEGENDA,view_venda_cas.LOJA
ORDER BY TOTAL_VENDA DESC
  "
)

tbl_geral %<>% dplyr::left_join(df_secoes_skus, by = c('COD_INTERNO'))
tbl_geral %<>% dplyr::mutate(
  DESCRICAO_COMPLETA = stringr::str_to_title(DESCRICAO_COMPLETA)
  )

writexl::write_xlsx(
  list(promo_casadinha_consolidada = tbl_geral),
  'output_dashboard_book_crm/3.2.promo_casadinha_mes_atual_consolidada.xlsx'
)

tbl_casadinha_por_loja <- DBI::dbGetQuery(
  conn = con, 
  statement = "SELECT view_venda_cas.COD_INTERNO AS COD_INTERNO, view_venda_cas.DESCRICAO_COMPLETA, view_venda_cas.LOJA,
LEGENDA,casadinha.CODIGO, sum(qt_promocao) as QUANTIDADE_PROMOCAO, 
sum(PRECO_VENDA) - ISNULL(SUM(DESCONTO), 0) AS TOTAL_VENDA, sum(DESCONTO) as DESCONTO
  FROM VM_DATABSP.dbo.CASADINHA casadinha
left join VM_DATABSP.dbo.CASADINHA_LOJA casadinha_loja on casadinha.CODIGO=casadinha_loja.CODIGOCASADINHA 
left join VM_INTEGRACAO.DBO.VW_CASADINHA_VENDA view_venda_cas on casadinha.CODIGO=view_venda_cas.COD_CASADINHA and 
casadinha_loja.CODIGO=view_venda_cas.LOJA
LEFT JOIN VM_DATABSP.dbo.PRODUTOS p ON  view_venda_cas.COD_INTERNO= p.PRODUTO_ID
where data between '2023-09-01' and '2023-09-30' and CUPONAGEM=10
GROUP BY view_venda_cas.COD_INTERNO, view_venda_cas.DESCRICAO_COMPLETA, casadinha.CODIGO,LEGENDA,view_venda_cas.LOJA
ORDER BY TOTAL_VENDA DESC
  "
)

df_lojas <- readxl::read_xlsx('dados/descricao_lojas.xlsx') %>% 
  dplyr::mutate(LOJA = as.numeric(LOJA))

df_casadinha_loja <- tbl_casadinha_por_loja %>% 
  dplyr::left_join(df_lojas, by = c('LOJA'))

writexl::write_xlsx(
  list(promo_casadinha_loja = df_casadinha_loja),
  'output_dashboard_book_crm/3.3.promo_casadinha_mes_atual_loja.xlsx'
)

rm(list=setdiff(ls(), "con"))
