pathvar <-"/home/gayatri/Fortiate/Build/Workspaces/R/ML_Transaction-model_R/data_csv.csv"
df = read.csv(pathvar,,colClasses='character')
df = df[c("PROCESSING_CODE","POS_DATA","TRANSACTION_AMOUNT","CARD_ACCEPTOR_ACTIVITY","CODE_ACTION","POS_ENTRY_MODE")]


write.csv(df,'/home/gayatri/Fortiate/Build/Workspaces/R/ML_Transaction-model_R/batch.csv',row.names = FALSE)

library(testthat)
test_that("path is correct",{
  expect_match(pathvar,"/home/gayatri/Fortiate/Build/Workspaces/R/ML_Transaction-model_R/data_csv.csv")
})
x<- dput(names(df))
print(x)

names = c("PROCESSING_CODE","POS_DATA","TRANSACTION_AMOUNT","CARD_ACCEPTOR_ACTIVITY","CODE_ACTION","POS_ENTRY_MODE")

test_that("columns are correct",{
  expect_identical(x,names)
})

library(plyr)
df$POS_ENTRY_MODE = as.character(df$POS_ENTRY_MODE)
df$POS_ENTRY_MODE[df$POS_ENTRY_MODE == "???"] <- "000"

df$POS_ENTRY_MODE = as.factor(df$POS_ENTRY_MODE)
NROW(df$POS_ENTRY_MODE)
df$CODE_ACTION = as.factor(df$CODE_ACTION)
print(df$CODE_ACTION)
df$PROCESSING_CODE = as.factor(df$PROCESSING_CODE)
NROW(df$PROCESSING_CODE)
df$CARD_ACCEPTOR_ACTIVITY = as.factor(df$CARD_ACCEPTOR_ACTIVITY)
NROW(df$CARD_ACCEPTOR_ACTIVITY)

df$TERM_CARD_READ_CAP <- sapply(strsplit(as.character(df$POS_DATA),''), "[", 1)
df$TERM_CH_VERI_CAP <- sapply(strsplit(as.character(df$POS_DATA),''), "[", 2)
df$TERM_CARD_CAPTURE_CAP<- sapply(strsplit(as.character(df$POS_DATA),''), "[", 3)
df$TERM_ATTEND_CAP <- sapply(strsplit(as.character(df$POS_DATA),''), "[", 4)
df$CH_PRESENCE_IND <- sapply(strsplit(as.character(df$POS_DATA),''), "[", 5)
df$CARD_PRESENCE_IND <- sapply(strsplit(as.character(df$POS_DATA),''), "[", 6)
df$TXN_CARD_READ_IND <- sapply(strsplit(as.character(df$POS_DATA),''), "[", 7)
df$TXN_CH_VERI_IND <- sapply(strsplit(as.character(df$POS_DATA),''), "[", 8)
df$TXN_CARD_VERI_IND<- sapply(strsplit(as.character(df$POS_DATA),''), "[", 9)
df$TRACK_REWRITE_CAP <- sapply(strsplit(as.character(df$POS_DATA),''), "[", 10)
df$TERM_OUTPUT_IND <- sapply(strsplit(as.character(df$POS_DATA),''), "[", 11)
df$PIN_ENTRY_IND <- sapply(strsplit(as.character(df$POS_DATA),''), "[", 12)
df$POS_DATA =NULL


library(caTools)

#sample_size = floor(0.8*nrow(rock))
#set.seed(777)

# randomly split data in r
#picked = sample(seq_len(nrow(rock)),size = sample_size)
#df.train =rock[picked,]
#df.test =rock[-picked,]


library(ISLR)
attach(Smarket)
smp_siz = floor(0.75*nrow(Smarket))  # creates a value for dividing the data into train and test. In this case the value is defined as 75% of the number of rows in the dataset
smp_siz

set.seed(123)   # set seed to ensure you always have same random numbers generated
train_ind = sample(seq_len(nrow(Smarket)),size = smp_siz)  # Randomly identifies therows equal to sample size ( defined in previous instruction) from  all the rows of Smarket dataset and stores the row number in train_ind
df.train =Smarket[train_ind,] #creates the training dataset with row numbers stored in train_ind
df.test=Smarket[-train_ind,]


#results = sample.split(df$CODE_ACTION,SplitRatio = 0.8)
#df.train = df[results == TRUE, ]
write.csv(df,'/home/gayatri/Fortiate/Build/Workspaces/R/ML_Transaction-model_R/Train.csv',row.names = FALSE)
train <- fread("/home/gayatri/Fortiate/Build/Workspaces/R/ML_Transaction-model_R/Train.csv",colClasses = 'character', stringsAsFactors = T)
#df.test = df[results == FALSE, ]
write.csv(df,'/home/gayatri/Fortiate/Build/Workspaces/R/ML_Transaction-model_R/Test.csv',row.names = FALSE)
test <- fread("/home/gayatri/Fortiate/Build/Workspaces/R/ML_Transaction-model_R/Test.csv",colClasses = 'character', stringsAsFactors = T)
pred1<- test[68910,c(1:3,5:17)]
pred1

library(h2o)
localH2O <- h2o.init(nthreads = -1)
h2o.init()
train.h2o <- as.h2o(train)
test.h2o <- as.h2o(test)
pred1.h2o<- as.h2o(pred1)
colnames(train.h2o)
y.dep <- 4

x.indep <- c(1:3,5:17)

regression.model <- h2o.glm( y = y.dep, x = x.indep, training_frame = train.h2o, family = "gaussian")
h2o.performance(regression.model)
glmmodelmodel<-h2o.saveModel(object=regression.model,path="/home/gayatri/Fortiate/Build/Workspaces/R/ML_Transaction-model_R/Models",force = TRUE)
predict.reg <- as.data.frame(h2o.predict(kmeans.model, test.h2o))



kmeans.model<- h2o.kmeans(training_frame = train.h2o,x = x.indep,k = 5,nfolds = 5,keep_cross_validation_models = TRUE,keep_cross_validation_predictions = TRUE)
h2o.performance(kmeans.model)
print(h2o.mean_per_class_error(kmeans.model,xval = TRUE))
predict.kmeans <- as.data.frame(h2o.predict(kmeans.model, test.h2o))
h2o.accuracy(object = kmeans.model)
kmeansmodel<-h2o.saveModel(object=kmeans.model,path="/home/gayatri/Fortiate/Build/Workspaces/R/ML_Transaction-model_R/Models",force = TRUE)
#h2o.download_pojo(model = kmeansmodel,path = "/home/gayatri/Fortiate/Build/Workspaces/R/ML_Transaction-model_R/Models")


df1 = data.frame('174000',7000,'6011','0','2','1','0','2','0','1','2','1','0','1','4','P')
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


nnmodel<- h2o.deeplearning(x=x.indep,y=y.dep,training_frame = train.h2o,epochs = 10,hidden = 6)
h2o.saveModel(nnmodel,path = "/home/gayatri/Fortiate/Build/Workspaces/R/ML_Transaction-model_R/Models",force = TRUE)
(performance<-h2o.performance(nnmodel))
predict.nn<-as.data.frame(h2o.predict(nnmodel,df6))

print("")
