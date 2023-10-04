pacman::p_load(odbc, DBI, tidyverse, hms, DataExplorer, writexl, magrittr, feather)
arquivo <- read_tsv('dados/2023-8-1-export-01.3_-_Contataveis_Email-0.tsv')

relationshipOpportunity <- arquivo %>% 
  dplyr::filter(!is.na(relationshipOpportunity)) %>% 
  writexl::write_xlsx('dados/relationshipopportunity.xlsx')
  

# String fornecida
input_string <- "LIMPEZA:LIMPEZA PESADA:DESINFETANTES;SUPERGELADOS:COMIDAS PRONTAS:PRATOS PRONTOS;CONSERVAS:ESPECIARIAS:TODAS;BISCOITOS:DOCES:SABORIZADOS;LATICINIOS I:QUEIJO PRATO P CORTE:PECAS INTEIRAS;BOMBONIERE:CHOCOLATES:BARRA;HIGIENE:PAPEIS DESCARTAVEIS:HIGIENICO F DUPLA;LATICINIOS II:QUEIJOS:CURADOS"

fun_string_completa <- function(input_string) {
  
  # Função para extrair a categoria (palavras antes do ":")
  extract_category <- function(input_string) {
    category <- sub(":.*", "", input_string)
    return(category)
  }
  
  # Divide a string em substrings separadas pelo ";"
  strings <- unlist(strsplit(input_string, ";"))
  
  # Extrai a categoria de cada substring
  categories <- sapply(strings, extract_category)
  
  # Criar o dataframe com a coluna "categoria"
  result_df <- data.frame(categoria = categories, stringsAsFactors = FALSE)
  #result_df
  
  ##############################################################################
  
  # Função para extrair o segundo e terceiro termo depois de ":"
  extract_second_and_third_term <- function(input_string) {
    terms <- unlist(strsplit(input_string, ":"))
    second_term <- ifelse(length(terms) >= 2, terms[2], NA)
    third_term <- ifelse(length(terms) >= 3, terms[3], NA)
    return(c(Segundo_Termo = second_term, Terceiro_Termo = third_term))
  }
  
  # Divide a string em substrings separadas pelo ";"
  strings <- unlist(strsplit(input_string, ";"))
  
  # Extrai o segundo e terceiro termo de cada substring
  result2 <- t(sapply(strings, extract_second_and_third_term))
  
  # Cria o dataframe com as colunas 2 e 3
  result_df2 <- data.frame(result2, stringsAsFactors = FALSE)
  
  # Imprime o dataframe resultante
  result_df3 <- cbind(result_df, result_df2)
  print(result_df3)
}

fun_string_completa(input_string)

################################################################################
relationshipOpportunity <- arquivo %>% 
  dplyr::filter(!is.na(relationshipOpportunity))

final_df <- data.frame(relationshipOpportunity = character(), stringsAsFactors = FALSE)

for (i in 1:nrow(relationshipOpportunity)) { #nrow(relationshipOpportunity)
  df <- relationshipOpportunity[i,2] %>% dplyr::pull() %>% fun_string_completa()
  rownames(df) <- c()
  # Adicione a linha do dataframe atual ao dataframe final usando rbind
  final_df <- rbind(final_df, df)
}

