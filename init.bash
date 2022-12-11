#!bin/sh


CreateResources=./src/main/resources

if [ ! -d $CreateResources ]; then
  mkdir $CreateResources
fi

CreateMaster=./src/main/resources/master
Createworker=./src/main/resources/worker

if [ ! -d $CreateMaster ]; then
  mkdir $CreateMaster
fi
if [ ! -d $Createworker ]; then
  mkdir $Createworker
fi

CreateMsample=./src/main/resources/master/sample
CreateSinput=./src/main/resources/worker/input
CreateSpartition=./src/main/resources/worker/partition
CreateSsamples=./src/main/resources/worker/samples

if [ ! -d $CreateMsample ]; then
  mkdir $CreateMsample
fi
if [ ! -d $CreateSinput ]; then
  mkdir $CreateSinput
fi
if [ ! -d $CreateSpartition ]; then
  mkdir $CreateSpartition
fi
if [ ! -d $CreateSsamples ]; then
  mkdir $CreateSsamples
fi


rm ./src/main/resources/master/sample/*

rm ./src/main/resources/worker/samples/*

rm ./src/main/resources/worker/partition/*

rm ./src/main/resources/worker/input/*

chmod +x ./gensort

for var in 1 2 3 4 5
do
./gensort -a 10000 ./src/main/resources/worker/input/gensort$var
done

