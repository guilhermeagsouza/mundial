pacman::p_load(odbc, DBI, tidyverse, hms, DataExplorer, writexl, magrittr, feather)

#### Conexão com o banco ####
con <- dbConnect(
  odbc::odbc(), 
  Driver = "SQL Server", 
  Server = "vmintegracao", # remember \\ if your path has a \ 
  Database = "VM_INTEGRACAO",
  user = "MUNDIAL\\guilherme.souza", # remember \\ if your username has a \
  Trusted_Connection = "True"
)
