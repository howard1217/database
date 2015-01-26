#!/bin/bash
# bash command-line arguments are accessible as $0 (the bash script), $1, etc.
# echo "Running" $0 "on" $1
# vagrant@precise64:~$ ./hw1.sh ebooks_tiny.txt

echo "Running hw1."
echo "title,author,release_date,ebook_id,language,body" > ebook.csv
echo "token,count" > token_counts.csv

# assuming "ebook_tiny.txt is $1"
python hw1.py $1

exit 0