library(h2o)
localH2O <- h2o.init(nthreads = -1)
h2o.init()
nnmodel<-h2o.loadModel("/home/gayatri/Fortiate/Build/Workspaces/R/ML_Transaction-model_R/Models/DeepLearning_model_R_1571115759676_2")
consumer=rkafka.createConsumer("127.0.0.1:2181","rupay-generated-data",autoOffsetReset="smallest")
#rkafka.createConsumer("127.0.0.1:2181","test2", groupId="test-consumer-group", zookeeperConnectionTimeoutMs="100000", consumerTimeoutMs="10000", autoCommitEnable="NULL", autoCommitInterval="NULL", autoOffsetReset="smallest")
#rkafka.read(consumer2)
#rkafka.read(consumer)
while(TRUE){
rkafka.read(consumer)
data<-fromJSON(rkafka.read(consumer))

data
for(i in 1:16){
if(data[["dataElements"]][[i]][["id"]] == 3)
  PROCESSING_CODE<-data[["dataElements"]][[i]][["value"]]
else if(data[["dataElements"]][[i]][["id"]] == 4)
  TRANSACTION_AMOUNT<-data[["dataElements"]][[i]][["value"]]
else if(data[["dataElements"]][[i]][["id"]] == 18)
  MERCHANT_CATEGORY_CODE<-data[["dataElements"]][[i]][["value"]]
else if(data[["dataElements"]][[i]][["id"]] == 22)
  POS_ENTRY_MODE<-data[["dataElements"]][[i]][["value"]]
}


df6<-data.frame(PROCESSING_CODE,TRANSACTION_AMOUNT,MERCHANT_CATEGORY_CODE,POS_ENTRY_MODE)
POS_DATA = '000030100000'
df6<-cbind(df6, POS_DATA)
df6$TERM_CARD_READ_CAP <- sapply(strsplit(as.character(df6$POS_DATA),''), "[", 1)
df6$TERM_CH_VERI_CAP <- sapply(strsplit(as.character(df6$POS_DATA),''), "[", 2)
df6$TERM_CARD_CAPTURE_CAP<- sapply(strsplit(as.character(df6$POS_DATA),''), "[", 3)
df6$TERM_ATTEND_CAP <- sapply(strsplit(as.character(df6$POS_DATA),''), "[", 4)
df6$CH_PRESENCE_IND <- sapply(strsplit(as.character(df6$POS_DATA),''), "[", 5)
df6$CARD_PRESENCE_IND <- sapply(strsplit(as.character(df6$POS_DATA),''), "[", 6)
df6$TXN_CARD_READ_IND <- sapply(strsplit(as.character(df6$POS_DATA),''), "[", 7)
df6$TXN_CH_VERI_IND <- sapply(strsplit(as.character(df6$POS_DATA),''), "[", 8)
df6$TXN_CARD_VERI_IND<- sapply(strsplit(as.character(df6$POS_DATA),''), "[", 9)
df6$TRACK_REWRITE_CAP <- sapply(strsplit(as.character(df6$POS_DATA),''), "[", 10)
df6$TERM_OUTPUT_IND <- sapply(strsplit(as.character(df6$POS_DATA),''), "[", 11)
df6$PIN_ENTRY_IND <- sapply(strsplit(as.character(df6$POS_DATA),''), "[", 12)
df6$POS_DATA =NULL
df6 = as.h2o(df6)
df6
cols<-colnames(df6)
nnmodel<-h2o.loadModel("/home/gayatri/Fortiate/Build/Workspaces/R/ML_Transaction-model_R/Models/DeepLearning_model_R_1571383247465_1")
predict.nn<-as.data.frame(h2o.predict(nnmodel,df6))
print("---------------------------------------------------------------------------")
print("For new transaction")
print(df6)
print("prediction for response code:")
print(predict.nn$predict)
}




#print(Transaction_amount<-data[["dataElements"]][[transaction_amt]][["value"]])
#print(Merchant_category_code<-data[["dataElements"]][[mcc]][["value"]])
#print(POS_ENTRY_MODE<-data[["dataElements"]][[pos_entry]][["value"]])
#df6<-rbind( df6, data.frame("Processing_code"=Processing_code, "Transaction_amount"=Transaction_amount,"Merchant_category_code"=Merchant_category_code,"POS_ENTRY_MODE"=POS_ENTRY_MODE))
#df6 = rbind(df6,c(Processing_code,"Transaction_amount","Merchant_category_code","POS_ENTRY_MODE"))
#dataframe <- as.data.frame(result)

#write(data, "test.json")
#save(data, file="rupay.JSON")

#print(rkafka.read(consumer2))
#print(rkafka.readPoll(consumer2))
#rkafka.closeProducer(producer1)
rkafka.closeConsumer(consumer)
#consumer <- KafkaConsumer$new(brokers = list(broker), groupId = "test", extraOptions=list(`auto.offset.reset`="earliest"))
#consumer$subscribe(topics = c(TOPIC_NAME))
#result <- consumer$consume(topic=TOPIC_NAME)

#result