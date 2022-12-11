#!bin/sh


CreateResources=/home/orange/332project/src/main/resources

if [ ! -d $CreateResources ]; then
  mkdir $CreateResources
fi

CreateMaster=/home/orange/332project/src/main/resources/master
Createworker=/home/orange/332project/src/main/resources/worker

if [ ! -d $CreateMaster ]; then
  mkdir $CreateMaster
fi
if [ ! -d $Createworker ]; then
  mkdir $Createworker
fi

CreateMsample=/home/orange/332project/src/main/resources/master/sample
CreateSinput=/home/orange/332project/src/main/resources/worker/input
CreateSpartition=/home/orange/332project/src/main/resources/worker/partition
CreateSsamples=/home/orange/332project/src/main/resources/worker/samples

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

cd
rm /home/orange/332project/src/main/resources/master/sample/*

cd
rm /home/orange/332project/src/main/resources/worker/samples/*

cd
rm /home/orange/332project/src/main/resources/worker/partition/*