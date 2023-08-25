# conexión con la bd
ch <- odbcConnect("Teradata", uid = "jvelezve", pwd = "jindec2023")

# nombre de la tabla
tabla <- "PRO_SK_DATA_V.V_GT23_F341_SOL1"

# Consulta usando funciones propias de Teradata
consulta <- paste0(
  "SELECT DISTINCT To_Date(FECHA_INFORMACION)",
  " FROM ", tabla
)

# ¿Regresa valores en formato fecha? 
fechas_dt <- sqlQuery(ch, consulta)

# Camino alternativo (usando funciones de SQL base)
consulta <- paste0(
  "SELECT DISTINCT CAST(CAST(FECHA_INFORMACION AS VARCHAR(8)) AS DATE)",
  " FROM ", tabla
)

fechas_sql <- sqlQuery(ch, consulta)
