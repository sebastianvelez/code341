# conexión con la bd
ch <- odbcConnect("Teradata", uid = "jvelezve", pwd = "jindec2023")

# nombre de la tabla
tabla <- "PRO_SK_DATA_V.V_GT23_F341_SOL1"

# Consulta usando funciones propias de Teradata
consulta <- paste0(
  "SELECT DISTINCT TO_DATE(TO_CHAR(FECHA_INFORMACION, '999999'), 'YYYYMMDD') AS FECHA_INFORMACION",
  " FROM ", tabla
)

# V2 (en caso de que la consulta anterior no funcione)
# consulta <- paste0(
#   "SELECT DISTINCT TO_DATE(FECHA_INFORMACION) AS FECHA_INFORMACION",
#   " FROM ", tabla
# )

# ¿Regresa valores en formato fecha? 
fechas_dt <- sqlQuery(ch, consulta)

# Camino alternativo (usando funciones de SQL base)
# consulta <- paste0(
#   "SELECT DISTINCT CAST(CAST(FECHA_INFORMACION AS VARCHAR(8)) AS DATE)",
#   " FROM ", tabla
# )
# 
# fechas_sql <- sqlQuery(ch, consulta)

consulta <- paste0(
  "SELECT FECHA_INFORMACION, FECHA_INICIAL_DEL_CREDITO,",
  " TO_DATE(FECHA_INFORMACION, 'YYYYMMDD') - TO_DATE(FECHA_INICIAL_DEL_CREDITO, 'YYYYMMDD') AS DIAS_ORIGINACION",
  " FROM ", tabla,
  " SAMPLE 10000"
)

# muestra de diferencia de fechas
muestra_fechas <- sqlQuery(ch, consulta)
