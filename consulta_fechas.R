# conexión con la bd
ch <- odbcConnect("Teradata", uid = "jvelezve", pwd = "jindec2023")

# nombre de la tabla
tabla <- "PRO_SK_DATA_V.V_GT23_F341_SOL1"

# (1) Consulta usando funciones propias de Teradata (JSVV) (OK)
consulta <- paste0("SELECT DISTINCT",
                   " CAST(FECHA_INFORMACION - 19000000 AS DATE),",
                   " CAST(CAST(CAST(FECHA_INFORMACION AS INTEGER) AS VARCHAR (10)) AS DATE FORMAT 'YYYYMMDD')",
                   "FROM ", tabla)

df_fechas <- sqlQuery(ch, consulta)

# (2.1) Validar que el álgebra funcione como debe
consulta <- paste0(
  "SELECT FECHA_INFORMACION, FECHA_INICIAL_DEL_CREDITO,",
  " CAST(FECHA_INFORMACION - 19000000 AS DATE) - CAST(FECHA_INICIAL_DEL_CREDITO - 19000000 AS DATE) AS DIAS_ORIGINACION,",
  " FROM ", tabla,
  " WHERE CAST(FECHA_INICIAL_DEL_CREDITO AS INTEGER) BETWEEN 19600101 AND 20231231",
  " SAMPLE 1000"
)

# Muestra de diferencia de fechas
system.time({muestra_fechas_1 <- sqlQuery(ch, consulta)})

# (2.2) Validar que el álgebra funcione como debe
consulta <- paste0(
  "SELECT FECHA_INFORMACION, FECHA_INICIAL_DEL_CREDITO,",
  " CAST(CAST(CAST(FECHA_INFORMACION AS INTEGER) AS VARCHAR (10)) AS DATE FORMAT 'YYYYMMDD') - CAST(CAST(CAST(FECHA_INICIAL_DEL_CREDITO AS INTEGER) AS VARCHAR (10)) AS DATE FORMAT 'YYYYMMDD') AS DIAS_ORIGINACION,",
  " FROM ", tabla,
  " WHERE CAST(FECHA_INICIAL_DEL_CREDITO AS INTEGER) BETWEEN 19600101 AND 20231231",
  " SAMPLE 1000"
)

# Muestra de diferencia de fechas
system.time({muestra_fechas_2 <- sqlQuery(ch, consulta)})

# (3) ¿Cuántos créditos son nuevos?
consulta <- paste0(
  "SELECT FECHA_INFORMACION,",
  " CASE WHEN CAST(FECHA_INFORMACION - 19000000 AS DATE) - CAST(FECHA_INICIAL_DEL_CREDITO - 19000000 AS DATE) AS DIAS_ORIGINACION < 90 THEN 'NUEVO' ELSE 'RECURRENTE' END,",
  " COUNT(*) AS N_REG",
  " FROM ", tabla,
  " WHERE CAST(FECHA_INICIAL_DEL_CREDITO AS INTEGER) BETWEEN 19600101 AND 20231231",
  " GROUP BY FECHA_INFORMACION, (CASE WHEN CAST(FECHA_INFORMACION - 19000000 AS DATE) - CAST(FECHA_INICIAL_DEL_CREDITO - 19000000 AS DATE) AS DIAS_ORIGINACION < 90 THEN 'NUEVO' ELSE 'RECURRENTE' END)"
)

numero_creditos <- sqlQuery(ch, consulta)
