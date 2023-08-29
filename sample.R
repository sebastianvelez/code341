# Query para generar muestra aleatoria de la base

# conexión con la bd
ch <- odbcConnect("Teradata", uid = "jvelezve", pwd = "jindec2023")

# nombre de la tabla
tabla <- "PRO_SK_DATA_V.V_GT23_F341_SOL1"

# tamaño de la muestra
limite <- 1000000L

# query para seleccionar observaciones aleatorias
# consulta <- paste0("SELECT *",
#                    " FROM ", tabla,
#                    " ORDER BY RANDOM()",
#                    " LIMIT ", limite)

consulta <- paste0("SELECT *",
                   " FROM ", tabla,
                   " SAMPLE ", limite)


consulta

# muestra
muestra <- sqlQuery(ch, consulta)
