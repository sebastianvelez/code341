# conexión con la bd
ch <- odbcConnect("Teradata", uid = "jvelezve", pwd = "jindec2023")

# nombre de la tabla
tabla <- "PRO_SK_DATA_V.V_GT23_F341_SOL1"

# (1) Consulta usando funciones propias de Teradata (JSVV)
consulta <- paste0("SELECT DISTINCT ",
                   "CAST(FECHA_INFORMACION - 19000000 AS DATE) ",
                   "FROM ", tabla)

df_fechas <- dbGetQuery(conn, consulta)

# (2) Validar que el álgebra funcione como debe
consulta <- paste0(
  "SELECT FECHA_INFORMACION, FECHA_INICIAL_DEL_CREDITO,",
  " CAST(FECHA_INFORMACION - 19000000 AS DATE) - CAST(FECHA_INICIAL_DEL_CREDITO - 19000000 AS DATE) AS DIAS_ORIGINACION",
  " FROM ", tabla,
  " SAMPLE 1000"
)

# Muestra de diferencia de fechas
muestra_fechas <- sqlQuery(ch, consulta)

# (3) ¿Cuántos créditos son nuevos?
consulta <- paste0(
  "SELECT FECHA_INFORMACION,",
  " CASE WHEN CAST(FECHA_INFORMACION - 19000000 AS DATE) - CAST(FECHA_INICIAL_DEL_CREDITO - 19000000 AS DATE) < 90 THEN 'NUEVO' ELSE 'RECURRENTE' END AS TIPO_ACREDITADO,",
  " COUNT(*) AS N_REG",
  " FROM ", tabla, " ",
  " GROUP BY FECHA_INFORMACION, TIPO_ACREDITADO"
)

numero_creditos <- sqlQuery(ch, consulta)