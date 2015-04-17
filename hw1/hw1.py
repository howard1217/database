import csv
import sys
import re

# ebook.csv, tokens.csv
filename = sys.argv[1]
text = ''
ebook_id = None
reading = False
title, author, release_date, ebook_id, language, body = 'null', 'null', 'null', 'null', 'null', ''
worddic = {}
signOfStart = '*** START OF THE PROJECT GUTENBERG'
signOfEnd = '*** END OF THE PROJECT GUTENBERG'

#towrite.write('title,author,release_date,ebook_id,language,body')
with open(filename) as f:
    with open('ebook.csv', 'w') as csvfile:
        with open('tokens.csv', 'w') as tokencsvfile:
            fieldnames = ['title','author','release_date','ebook_id','language','body']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            tokenfieldnames = ['ebook_id', 'token']
            tokenwriter = csv.DictWriter(tokencsvfile, fieldnames=tokenfieldnames)
            tokenwriter.writeheader()
            for line in f:
                if not reading:
                    l = line.split()
                    if len(l) == 0:
                        continue
                    if l[0] == 'Title:':
                        title = " ".join(line.split(" ")[1:]).rstrip()
                    elif l[0] == 'Author:':
                        author = " ".join(l[1:]).rstrip()
                    elif (l[0] == 'Release' and l[1] == 'Date:'):
                        index = 2
                        while index < len(l) and (not l[index][0] == '['):
                            index += 1
                        release_date = " ".join(l[2:index]).rstrip()
                        if (len(l) > index and l[index + 1][0] == '#'):
                            ebook_id = l[index + 1].rstrip()[1:-1].rstrip()
                    elif l[0] == 'Language:':
                        language = " ".join(l[1:]).rstrip()
                    elif ' '.join(l[0:6]) == signOfStart:
                        reading = True
                else:
                    if ' '.join(line.split(" ")[0:6]) == signOfEnd:
                        reading = False
                        writer.writerow({'title': title,'author': author,'release_date': release_date,'ebook_id': ebook_id,'language': language,'body': body})
                        title, author, release_date, ebook_id, language, body = 'null', 'null', 'null', 'null', 'null', ''
                    else:
                        body += line
                        for word in re.findall(r"[a-zA-Z]+", line):
                            tokenwriter.writerow({'ebook_id': ebook_id, 'token': word.lower().rstrip()})
exit()