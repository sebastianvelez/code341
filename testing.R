# conexión con la bd
ch <- odbcConnect("Teradata", uid = "jvelezve", pwd = "jindec2023")

# nombre de la tabla
tabla <- "PRO_SK_DATA_V.V_GT23_F341_SOL1"

# tipo de columnas tal y como son 
tipo_columnas <- sqlColumns(ch, tabla, as.is = TRUE)

# intentos de queries con la CIIU

# 0
consulta <- paste0(
  "SELECT DISTINCT CAST(CIIU AS CHAR(4)) ",
  "FROM ", tabla
)

# modificar el parámetro as.is para evitar la conversión de tipos

df_ciiu <- sqlQuery(ch, consulta) 
df_ciiu <- sqlQuery(ch, consulta, as.is = TRUE)

# 1
consulta <- paste0(
  "SELECT SUBSTRING(CIIU FROM 1 FOR 4) AS CIIU, ",
  "COUNT(*) AS N_REG ",
  "FROM ", tabla, " ",
  "GROUP BY CIIU"
)

# modificar el parámetro as.is para evitar la conversión de tipos

df_ciiu <- sqlQuery(ch, consulta) 
df_ciiu <- sqlQuery(ch, consulta, as.is = TRUE)

# 2
consulta <- paste0(
  "SELECT CAST(CIIU AS CHAR(4)) AS CIIU, ",
  "COUNT(*) AS N_REG ",
  "FROM ", tabla, " ",
  "GROUP BY CIIU"
)

# modificar el parámetro as.is para evitar la conversión de tipos

df_ciiu <- sqlQuery(ch, consulta) 
df_ciiu <- sqlQuery(ch, consulta, as.is = TRUE)

# 3
consulta <- paste0(
  "SELECT CIIU(CHAR(4)), ",
  "COUNT(*) AS N_REG ",
  "FROM ", tabla, " ",
  "GROUP BY CIIU"
)

# modificar el parámetro as.is para evitar la conversión de tipos

df_ciiu <- sqlQuery(ch, consulta) 
df_ciiu <- sqlQuery(ch, consulta, as.is = TRUE)

# camino largo (ejecutar solo si lo anterior no funciona)

# obtenemos la query
odbcQuery(ch, consulta)

# si regresa -1, tenemos error
odbcGetErrMsg(ch)

# si regresa 1, la podemos ejecutar
odbcQuery(ch, consulta)

sqlGetResults(ch)

# odbcGetInfo(ch)

# consulta <- paste0("SHOW STATISTICS VALUES ON ", tabla)

# tdplyr

# install.packages('tdplyr',repos=c('https://r-repo.teradata.com','https://cloud.r-project.org'))

# ch <- td_create_context(host = <host-name>, uid=<username>, pwd=<password>, dType = "native", logmech = "TD2")

