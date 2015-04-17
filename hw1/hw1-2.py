import csv

# token_counts.csv, name_counts.csv
names = set()
with open('popular_names.txt') as f:
    for line in f:
        names.add(line.rstrip().lower())

index = -1
words = []
counts = []

with open('temp.txt') as f:
    with open('token_counts.csv', 'w') as tcounts_csvfile:
        with open('name_counts.csv', 'w') as ncounts_csvfile:
            fieldnames = ['token', 'count']
            tcounts_writer = csv.DictWriter(tcounts_csvfile, fieldnames=fieldnames)
            tcounts_writer.writeheader()
            ncounts_writer = csv.DictWriter(ncounts_csvfile, fieldnames=fieldnames)
            ncounts_writer.writeheader()
            for line in f:
                currword = line.split(',')[1].rstrip()
                if currword == 'token' and line.split(',')[0].rstrip() == 'ebook_id':
                    continue
                if index < 0 or not words[index] == currword:
                    index += 1
                    words.append(currword)
                    counts.append(1)
                    if index > 0:
                        tcounts_writer.writerow({'token': words[index - 1], 'count': counts[index - 1]})
                        if words[index - 1] in names:
                            ncounts_writer.writerow({'token': words[index - 1], 'count': counts[index - 1]})
                else:
                    counts[index] += 1
            tcounts_writer.writerow({'token': words[index], 'count': counts[index]})
            if words[index] in names:
                ncounts_writer.writerow({'token': words[index], 'count': counts[index]})
             