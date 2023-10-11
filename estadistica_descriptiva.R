library(RODBC)
library(dplyr)

# conexion con la bd
ch <- odbcConnect("Teradata", uid = "jvelezve", pwd = "Jimnyjb74")

# nombre de la tabla
tabla <- "PRO_SK_DATA_V.V_GT23_F341_SOL1"

# estadistica descriptiva (por tipo de acreditado)
consulta <- paste0(
  "SELECT FECHA_INFORMACION,",
  " CASE WHEN CAST(FECHA_INFORMACION - 19000000 AS DATE) - CAST(FECHA_INICIAL_DEL_CREDITO - 19000000 AS DATE)  < 90 THEN 'NUEVO' ELSE 'RECURRENTE' END,",
  " COUNT(*) AS N_REG,",
  " SUM(SALDO_CAPITAL) AS CAPITAL,",
  " SUM(SALDO_INTERESES) AS INTERESES,",
  " SUM(VALOR_GARANTIA) AS GARANTIA,",
  " AVG(TASA_PROMEDIO_DEL_CREDITO) AS TASA_CREDITO,",
  " SUM(TASA_PROMEDIO_DEL_CREDITO * SALDO_CAPITAL) / SUM(SALDO_CAPITAL) AS TASA_CREDITO_PONDERADA",
  " FROM ", tabla,
  " WHERE CAST(FECHA_INICIAL_DEL_CREDITO AS INTEGER) BETWEEN 19600101 AND 20231231",
  " GROUP BY FECHA_INFORMACION, (CASE WHEN CAST(FECHA_INFORMACION - 19000000 AS DATE) - CAST(FECHA_INICIAL_DEL_CREDITO - 19000000 AS DATE) < 90 THEN 'NUEVO' ELSE 'RECURRENTE' END)"
)

res_tipo_acreditado <- sqlQuery(ch, consulta)

ratio_garantia <- res_tipo_acreditado %>%
  rename(TIPO_ACREDITADO = 2) %>%
  filter(TIPO_ACREDITADO == "NUEVO") %>%
  mutate(RATIO_GARANTIA = GARANTIA / (CAPITAL + INTERESES))

# estadistica descriptiva (por CIIU)
consulta <- paste0(
  "SELECT FECHA_INFORMACION,",
  " SUBSTRING(CAST(CIIU AS VARCHAR(12)), 1, 4),",
  " COUNT(*) AS N_REG,",
  " SUM(SALDO_CAPITAL) AS CAPITAL,",
  " SUM(SALDO_INTERESES) AS INTERESES,",
  " SUM(VALOR_GARANTIA) AS GARANTIA,",
  " AVG(TASA_PROMEDIO_DEL_CREDITO) AS TASA_CREDITO,",
  # " SUM(TASA_PROMEDIO_DEL_CREDITO * SALDO_CAPITAL) / SUM(SALDO_CAPITAL) AS TASA_CREDITO_PONDERADA",
  " SUM(TASA_PROMEDIO_DEL_CREDITO * SALDO_CAPITAL) / NULLIF(SUM(SALDO_CAPITAL), 0) AS TASA_CREDITO_PONDERADA",
  " FROM ", tabla,
  " WHERE CAST(FECHA_INICIAL_DEL_CREDITO AS INTEGER) BETWEEN 19600101 AND 20231231",
  # " AND SALDO_CAPITAL > 0"
  " GROUP BY FECHA_INFORMACION, SUBSTRING(CAST(CIIU AS VARCHAR(12)), 1, 4)"
)

res_ciiu <- sqlQuery(ch, consulta)

# última fecha de avalúo
consulta <- paste0(
  "SELECT FECHA_INFORMACION,",
  " COUNT(*) AS N_REG,",
  " COUNT(FECHA_ULTIMO_AVALUO_GARANTIA) AS N_REG_AVALUO,",
  " AVG(CAST(FECHA_INFORMACION - 19000000 AS DATE) - CAST(FECHA_ULTIMO_AVALUO_GARANTIA - 19000000 AS DATE)),",
  " STDDEV_SAMP(CAST(FECHA_INFORMACION - 19000000 AS DATE) - CAST(FECHA_ULTIMO_AVALUO_GARANTIA - 19000000 AS DATE))",
  " FROM ", tabla,
  " WHERE CAST(FECHA_ULTIMO_AVALUO_GARANTIA AS INTEGER) BETWEEN 20000101 AND 20231231",
  " AND FECHA_ULTIMO_AVALUO_GARANTIA IS NOT NULL",
  " GROUP BY FECHA_INFORMACION"
)

res_fecha_avaluo <- sqlQuery(ch, consulta)

# estadistica descriptiva (acreditado individual)
consulta <- paste0(
  "SELECT (IDENTIFICACION_DEUDOR || '_' || FECHA_INICIAL_DEL_CREDITO || '_' || FECHA_FINAL_DEL_CREDITO || '_' || ENTIDAD_CODIGO_SUPER),",
  " COUNT(*) AS N_REG,",
  " AVG(SALDO_CAPITAL) AS PROMEDIO_CAPITAL,",
  " AVG(VALOR_GARANTIA) AS PROMEDIO_GARANTIA,",
  " STDDEV_SAMP(SALDO_CAPITAL) AS SD_CAPITAL,",
  " STDDEV_SAMP(VALOR_GARANTIA) AS SD_GARANTIA",
  " FROM ", tabla,
  " GROUP BY (IDENTIFICACION_DEUDOR || '_' || FECHA_INICIAL_DEL_CREDITO || '_' || FECHA_FINAL_DEL_CREDITO || '_' || ENTIDAD_CODIGO_SUPER)"
)

res_acreditados <- sqlQuery(ch, consulta)


save(ratio_garantia,file = "C:/Users/jvelezve/OneDrive - Banco de la República/Documents/Bancos/Felipe and Rebecca/output/ratio_garantia.rdata")

save(res_ciiu, file = "C:/Users/jvelezve/OneDrive - Banco de la República/Documents/Bancos/Felipe and Rebecca/output/res_ciiu.rdata")

save(res_tipo_acreditado, file = "C:/Users/jvelezve/OneDrive - Banco de la República/Documents/Bancos/Felipe and Rebecca/output/res_tipo_acreditado.rdata")

save(res_acreditados, file = "C:/Users/jvelezve/OneDrive - Banco de la República/Documents/Bancos/Felipe and Rebecca/output/res_acreditados.rdata")
