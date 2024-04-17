for x in {0..9} {a..z}
do
  python preprocess-seg-csv.py ${x} > output_${x}.log 2>&1 &
  #wait
done

