#####################################################################
#                   Reads F341 odbc teradata                        #
#####################################################################


library(RODBC)

ch=odbcConnect("Teradata",  uid="jvelezve",pwd="benhelkias")

# seeing columns
mycol <- sqlColumns(ch, "PRO_SK_DATA_V.V_GT23_F341_SOL1")


# query that shows the dates of reports

mydates <-
  sort(
    as.vector(
      t(
        sqlQuery(ch, "SELECT DISTINCT FECHA_INFORMACION AS fechainfo FROM PRO_SK_DATA_V.V_GT23_F341_SOL1")
        )
      )
  )


# extracting tables by date of report

for (i in 1:length(mydates)) {
df <- sqlQuery(ch, paste("SELECT * FROM PRO_SK_DATA_V.V_GT23_F341_SOL1 WHERE FECHA_INFORMACION = ",mydates[86+i] ))
save(df, file = paste("//wcalisrv/CEARN/RAWF341/f341_",mydates[86+i],".rdata", sep = ""))
}

# Warning: query was interrupted at the end of each day#
# some tables might be incomplete?                     #


#################################
# sample data for testing code #
###############################
library(data.table)
library(parallel)


testdt <-dt[][][sample (.N,100000)]

save(testdt,file = "C:/Users/jvelezve/OneDrive - Banco de la República/Documents/Bancos/Felipe and Rebecca/output/test.rdata")


#####################################################################
# selecting columns, entidades and firms to reduce the size         #
#####################################################################

#find all files in folders
files <- list.files(path="//wcalisrv/CEARN/RAWF341/", pattern="*.rdata", full.names=TRUE, recursive=FALSE)

files <- files[1:91]

#columns to keep

keep_cols  <- c(
  "FECHA_INFORMACION",
  "CARTERA_TIPO",
  "CREDITO_TIPO",
  "TIPO_ENTIDAD",
  "ENTIDAD_RAZON_SOCIAL",
  "IDENTIFICACION_DEUDOR",
  "NUMERO_OPERACIONES",
  "SALDO_CAPITAL",
  "FECHA_INICIAL_DEL_CREDITO",
  "FECHA_FINAL_DEL_CREDITO",
  "TASA_PROMEDIO_DEL_CREDITO"
  )

# keep these entidades:
# 1- Establecimientos bancarios
# 4- Compañías de Financiamiento
# 22- Instituciones oficiales especiales
# 32- Entidades cooperativas de carácter financiero.
entidades <- c(1,32,22,4)

# function to load data and keep desired cols, commercial loans, and remove undesired entidades
myfun <- function(dataset){
  library(data.table)
    load(dataset)
  dt <- data.table(df)
  dt <- dt[,keep_cols, with=FALSE]
  dt <- dt[TIPO_ENTIDAD %in% entidades & CARTERA_TIPO == "Hipotecaria",] # "Comercial", "Hipotecaria", "Microcrédito"
  dt <- na.omit(dt,cols = "TIPO_ENTIDAD")
  return(dt)
}



# setting up parallel
cores <- detectCores()-6
cl <-makeCluster(cores)
clusterExport(cl, varlist=c("entidades","files","keep_cols"))

start <- Sys.time()
alldt <-parLapply(cl,files,myfun)
alldt <- rbindlist(alldt)
end <- Sys.time()




stopCluster(cl)

gc()



save(alldt, file = "//wcalisrv/CEARN/RAWF341/f341_hipo.rdata")

# f341_commercial.rdata all commercial loans, no Instituciones oficiales especiales
# f341_micro.rdata all microcreditos, no Instituciones oficiales especiales or Compañías de Financiamiento
# f341_hipo.rdata all hipotecaria, o Instituciones oficiales especiales