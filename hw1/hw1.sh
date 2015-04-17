#!/bin/bash
# bash command-line arguments are accessible as $0 (the bash script), $1, etc.
# echo "Running" $0 "on" $1
# vagrant@precise64:~$ ./hw1.sh ebooks_tiny.txt
# assuming "ebook_tiny.txt is $1"
python hw1.py $1

sort -t',' -k 2 tokens.csv -o temp.txt

python hw1-2.py

#rm temp.txt

exit 0

#sort the file to diff