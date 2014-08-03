##################################################################################
#######################    SQLite Speed      #####################################
##################################################################################
install.packages("sqldf")
require(sqldf)
#DataPath<-"C:/Data/NewTables/"
#Datatbl<-list.files(DataPath)
#Cwd<-getwd()
#setwd(DataPath)
#dat = lapply(Datatbl,read.csv, header = TRUE)
#setwd(Cwd)


#Integers
Nmbs<-sample.int(1000, 1000000, replace = TRUE)

#Floats
Mtrx<-rnorm(1000000)

#Dates
DTS<-c()

for (i in 1:1000){
  DtS<-as.Date(rnorm(1000)*5000, origin="1970-01-01")
  DTS<-c(DTS,DtS)
}

rm(DtS)

#index
indx<-1:1000000

#Dates as Text
DTStxt<-format(as.Date(DTS, origin='1970-01-01'), "%Y-%m-%d" )

#Combine to a matrix then dataframe
#DF has a combination of Text, Numbers and Dates
DF<-c()
DF<-cbind(indx, DTStxt)
for (n in 1:8){
  DF<-cbind(DF, DTS)
  DF<-cbind(DF, DTStxt)
  DF<-cbind(DF, Nmbs)
  DF<-cbind(DF, Mtrx)
}

DF<-data.frame(DF)

rm(DTS,DTStxt,Mtrx,Txt,Nmbs, indx)

db<-dbConnect(SQLite(), dbname="R&M_Tools.sqlite")
summary(db)



dbWriteTable(db, "SQLQ", DF)
dbWriteTable(db, "SQLQIndx", DF)

dbListTables(db)

dbGetQuery(db,"Create index indxIndx on SQLQIndx(indx, DTStxt, Nmbs)")

dbGetQuery(db,"Create index indxIndx2 on SQLQIndx(indx, DTStxt, Nmbs, Nmbs_1, Mtrx)")

dbGetQuery(db,  "select * from sqlite_master where tbl_name = 'SQLQIndx'")

###############################################################################################
##################              Working With Tables             ###############################
###############################################################################################

system.time(dbGetQuery(db, "Select Count(indx) from SQLQ"))

system.time(dbGetQuery(db, "Select Count(indx) from SQLQindx"))

system.time(dbGetQuery(db, "Select Count(indx) from SQLQ where Nmbs>200"))

system.time(dbGetQuery(db, "Select Count(indx) from SQLQindx where Nmbs>200"))

system.time(dbGetQuery(db, "Select Count(indx) from SQLQ where DTStxt>'1970-01-01'"))

system.time(dbGetQuery(db, "Select Count(indx) from SQLQindx where DTStxt>'1970-01-01'"))

dbGetQuery(db, "Select Count(indx) from SQLQ as PINDEX where Nmbs>995")

dbGetQuery(db, "Select Count(indx) from SQLQindx where Nmbs>995")


SqlTst<-"Select  
Current.Nmbs, Current.DTStxt, Current.NMBS_1, Current.Mtrx, Current.NMBS_2, Current.NMBS_3, Current.NMBS_4, 
PINDX.INDX, PINDX.DTStxt, PINDX.NMBS_1, PINDX.Mtrx,PINDX.NMBS_2, PINDX.NMBS_3, PINDX.NMBS_4 
From 
(Select  
ReCu.Nmbs, ReCu.DTStxt, ReCu.NMBS_1, ReCu.Mtrx, ReCu.NMBS_2, ReCu.NMBS_3, ReCu.NMBS_4, 
(Select  Innr.INDX 
From 
SQLQ as Innr 
Where 
Innr.NMBS = ReCu.Nmbs 
and Innr.NMBS_1 > 985 
and Innr.DTStxt < ReCu.DTStxt 
Order by Innr.DTStxt DESC 
Limit 1) 
as LastINDX 
From 
SQLQ as ReCu 
Where 
ReCu.NMBS_1 >985 or ReCu.NMBS_1=970
Limit 1000) 
as Current 
LEFT JOIN 
SQLQ AS PINDX 
ON 
Current.LastINDX = PINDX.INDX;" 

SqlTst2<-"Select  
Current.Nmbs, Current.DTStxt, Current.NMBS_1, Current.Mtrx, Current.NMBS_2, Current.NMBS_3, Current.NMBS_4, 
PINDX.INDX, PINDX.DTStxt, PINDX.NMBS_1, PINDX.Mtrx,PINDX.NMBS_2, PINDX.NMBS_3, PINDX.NMBS_4 
From 
(Select  
ReCu.Nmbs, ReCu.DTStxt, ReCu.NMBS_1, ReCu.Mtrx, ReCu.NMBS_2, ReCu.NMBS_3, ReCu.NMBS_4, 
(Select  Innr.INDX 
From 
SQLQindx as Innr 
Where 
Innr.NMBS = ReCu.Nmbs 
and Innr.NMBS_1 > 985 
and Innr.DTStxt < ReCu.DTStxt 
Order by Innr.DTStxt DESC 
Limit 1) 
as LastINDX 
From 
SQLQindx as ReCu 
Where 
ReCu.NMBS_1 >985 or ReCu.NMBS_1=970 
Limit 1000) 
as Current 
LEFT JOIN 
SQLQindx AS PINDX 
ON 
Current.LastINDX = PINDX.INDX;" 

SQLRwr<-"Select SQLQindx.Nmbs, SQLQindx.DTStxt, SQLQindx.NMBS_1, SQLQindx.Mtrx, 
SQLQindx.NMBS_2, SQLQindx.NMBS_3, SQLQindx.NMBS_4,
(Select 
INDX 
from SQLQindx as Prec 
Where 
Prec.NMBS_1>985
and Prec.DTStxt<SQLQindx.DTStxt
and Prec.Mtrx<SQLQindx.Mtrx
order by DTStxt Desc
Limit 1) as PreIndx, Pact.Indx
from SQLQindx
Left Join
SQLQindx as PAct
on
PreIndx=PAct.Indx
Where
SQLQindx.NMBS_1=985 ;
"
dbGetQuery(db,"Drop index indxIndx3")

system.time(RSqlTst<-dbGetQuery(db, SqlTst))

system.time(RSqlTst<-dbGetQuery(db, SqlTst2))
# This query would totally time out as wrtten on the indexing availible
#system.time(RSqlTst<-dbGetQuery(db, SQLRwr))

dbGetQuery(db, "EXPLAIN QUERY PLAN Select SQLQindx.Nmbs, SQLQindx.DTStxt, SQLQindx.NMBS_1, SQLQindx.Mtrx, 
           SQLQindx.NMBS_2, SQLQindx.NMBS_3, SQLQindx.NMBS_4,
           (Select 
           INDX 
           from SQLQindx as Prec 
           Where 
           Prec.NMBS_1>985
           and Prec.DTStxt<SQLQindx.DTStxt
           and Prec.Mtrx<SQLQindx.Mtrx
           order by DTStxt Desc
           Limit 1) as PreIndx, Pact.Indx
           from SQLQindx
           Left Join
           SQLQindx as PAct
           on
           PreIndx=PAct.Indx
           Where
           SQLQindx.NMBS_1=985 ;"
           

           #So Create a covering index
           
           dbGetQuery(db,"Create index indxIndx3 on SQLQIndx(Nmbs_1,indx, DTStxt, Mtrx, Nmbs,NMBS_2 ,NMBS_3,NMBS_4 )")
           
           
           
           dbGetQuery(db, "EXPLAIN QUERY PLAN Select SQLQindx.Nmbs, SQLQindx.DTStxt, SQLQindx.NMBS_1, SQLQindx.Mtrx, 
                      SQLQindx.NMBS_2, SQLQindx.NMBS_3, SQLQindx.NMBS_4,
                      (Select 
                      INDX 
                      from SQLQindx as Prec 
                      Where 
                      Prec.NMBS_1>985
                      and Prec.DTStxt<SQLQindx.DTStxt
                      and Prec.Mtrx<SQLQindx.Mtrx
                      order by DTStxt Desc
                      Limit 1) as PreIndx, Pact.Indx
                      from SQLQindx
                      Left Join
                      SQLQindx as PAct
                      on
                      PreIndx=PAct.Indx
                      Where
                      SQLQindx.NMBS_1=985 ;")
           
           system.time(RSqlTst<-dbGetQuery(db, SQLRwr))
           
           dbGetQuery(db, "EXPLAIN QUERY PLAN Select  
                      Current.Nmbs, Current.DTStxt, Current.NMBS_1, Current.Mtrx, Current.NMBS_2, Current.NMBS_3, Current.NMBS_4, 
                      PINDX.INDX, PINDX.DTStxt, PINDX.NMBS_1, PINDX.Mtrx,PINDX.NMBS_2, PINDX.NMBS_3, PINDX.NMBS_4 
                      From 
                      (Select  
                      ReCu.Nmbs, ReCu.DTStxt, ReCu.NMBS_1, ReCu.Mtrx, ReCu.NMBS_2, ReCu.NMBS_3, ReCu.NMBS_4, 
                      (Select  Innr.INDX 
                      From 
                      SQLQindx as Innr 
                      Where 
                      Innr.NMBS = ReCu.Nmbs 
                      and Innr.NMBS_1 > 985 
                      and Innr.DTStxt < ReCu.DTStxt 
                      Order by Innr.DTStxt DESC 
                      Limit 1) 
                      as LastINDX 
                      From 
                      SQLQindx as ReCu 
                      Where 
                      ReCu.NMBS_1 >985 or ReCu.NMBS_1=970 
                      Limit 1000) 
                      as Current 
                      LEFT JOIN 
                      SQLQindx AS PINDX 
                      ON 
                      Current.LastINDX = PINDX.INDX;")
           
           system.time(RSqlTst<-dbGetQuery(db, SqlTst2))
           
           dbDisconnect(db)
           