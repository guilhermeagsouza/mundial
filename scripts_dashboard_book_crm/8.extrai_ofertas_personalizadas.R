# 0.0 Carrega os pacotes
pacman::p_load(odbc, DBI, tidyverse, hms, DataExplorer, writexl, magrittr, feather)

# 0.1 Conecta ao banco de dados
source(
  'scripts_dashboard_book_crm/0.conecta_banco.R', 
  echo=TRUE, 
  max.deparse.length=60
)

end_arquivos <- list.files(path = 'dados/ofertas_personalizadas/',full.names = TRUE)
#end_arquivos <- 'dados/ofertas_personalizadas/Promoções Personalizadas MUN - JUL20232.xlsx'

if(str_detect(end_arquivos,pattern = c(".tsv"))) {
  df_ofertas_personalizadas <- data.table::fread(input = end_arquivos)
} else {
  df_ofertas_personalizadas <- readxl::read_xlsx(end_arquivos)
}

#### Extração dos dados
df <- df_ofertas_personalizadas %>% 
  dplyr::filter(TYPE %in% c("PERSONALIZED")) %>% 
  dplyr::select(
    PARTNER_PROMOTION_ID, TYPE, DESCRIPTION,PRODUCT_ID_INCLUSION,
    START_DATE, END_DATE, "1_URL_IMAGE"
  ) %>% 
  dplyr::mutate(
    START_DATE = lubridate::as_datetime(START_DATE), 
    END_DATE = lubridate::as_datetime(END_DATE)
    ) %>% 
  dplyr::rename(URL_IMAGEM = "1_URL_IMAGE")
  

output <- df$PRODUCT_ID_INCLUSION
# Dividir cada elemento por vírgulas e criar um data frame
split_output <- strsplit(output, ",")
max_length <- max(lengths(split_output))
output_df <- as.data.frame(do.call(rbind, lapply(split_output, `length<-`, max_length)))

# Renomear colunas
colnames(output_df) <- paste0("SKU", seq_len(max_length))

df_consolidado_promo_sku_personalizada <- df %>% 
  dplyr::mutate(SKU1 = output_df[,1]) %>% #, SKU2 = output_df[,2]) %>% 
  pivot_longer(cols = c(SKU1), names_to = "SKU", values_to = c("PRODUTO")) %>% 
  dplyr::filter(!is.na(PRODUTO))

final_df <- data.frame()

for(i in 1:nrow(df_consolidado_promo_sku_personalizada)) {

cod_promo <- df_consolidado_promo_sku_personalizada$PARTNER_PROMOTION_ID[i]
data_inicio <- df_consolidado_promo_sku_personalizada$START_DATE[i] %>% as.Date() %>% ymd()
data_final <- df_consolidado_promo_sku_personalizada$END_DATE[i] %>% as.Date() %>% ymd()
sku_escolhido <- df_consolidado_promo_sku_personalizada$PRODUTO[i]
descricao_oferta <- df_consolidado_promo_sku_personalizada$DESCRIPTION[i]
url_imagem <- df_consolidado_promo_sku_personalizada$URL_IMAGEM[i]

sql <- paste0("
SELECT LOJA, DATA, COD_INTERNO, PRECO_VENDA,QUANTIDADE,
PRECO_TOTAL AS VALOR_TOTAL,DESCONTO AS DESCONTO_TOTAL, 
PRECO_TOTAL-DESCONTO AS VALOR_TOTAL_C_DESCONTO, (PRECO_TOTAL-DESCONTO)/QUANTIDADE as PRECO_C_DESCONTO
from VM_LOG.dbo.ITEM_DESCONTO 
where CANCELADO = 0 AND cod_interno IN (",sku_escolhido,")",
" AND DATA >= '",data_inicio,"'"," AND DATA <= '",data_final,"'","AND TIPO_PRODUTO = 26 ORDER BY LOJA"
)

df_venda_sku_oferta_personalizada <- DBI::dbGetQuery(
  conn = con, 
  statement = sql
)

df_consolidado <- df_venda_sku_oferta_personalizada %>% 
  dplyr::group_by(COD_INTERNO) %>% 
  dplyr::summarise(
    SOMA_QUANTIDADE = sum(QUANTIDADE), 
    SOMA_VALOR_TOTAL_C_DESCONTO = sum(VALOR_TOTAL_C_DESCONTO)
    ) %>% 
  dplyr::mutate(
    DESCRICAO_OFERTA = descricao_oferta,
    URL_IMAGEM = url_imagem,
    COD_PROMO = cod_promo,
    DATA_INICIO = data_inicio,
    DATA_FINAL = data_final
  )
final_df <- rbind(final_df, df_consolidado)

}

writexl::write_xlsx(
  list(ofertas_personalizadas = final_df),
  'output_dashboard_book_crm/8.resultado_ofertas_personalizadas.xlsx'
)

rm(list=setdiff(ls(), c("con")))
