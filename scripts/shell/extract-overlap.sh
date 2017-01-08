PWD=`pwd`
cd $1

rm *.overlap
PROGRAM=~/work/code/mg4j-nyu/scripts/ruby/extractOverlap.rb

for f in *-AND-global.1k
do
  echo $f
  corpus="$(echo $f | sed -e 's/1k\///' | cut -d '-' -f 1)"
  target="$(echo $f | sed -e 's/1k\///' | cut -d '-' -f 2 )"
  trainset=100K_AND
  model=xgboost
  cutsize="$(echo $f | sed -e 's/1k\///' | cut -d '-' -f 3 )"
  ruby $PROGRAM gov2-baseline-AND-global.txt 150 1000 $f $corpus $trainset $model $target $cutsize AND > $f.overlap
done

for f in *-OR-global.1k
do
  echo $f
  corpus="$(echo $f | sed -e 's/1k\///' | cut -d '-' -f 1)"
  target="$(echo $f | sed -e 's/1k\///' | cut -d '-' -f 2 )"
  trainset=10MQ
  model=xgboost
  cutsize="$(echo $f | sed -e 's/1k\///' | cut -d '-' -f 3 )"
  ruby $PROGRAM gov2-baseline-OR-global.txt 150 1000 $f $corpus $trainset $model $target $cutsize OR > $f.overlap
done

cat *.overlap > $corpus.final-overlap.csv

cd $PWD
