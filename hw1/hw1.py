import csv
import sys

# ebook.csv
filename = sys.argv[1]
towrite = open('ebook.csv', 'a')
text = ''
reading = False
dic = {}
signOfStart = '*** START OF THE PROJECT GUTENBERG'
signOfEnd = '*** END OF THE PROJECT GUTENBERG'

#towrite.write('title,author,release_date,ebook_id,language,body')
with open(filename) as f:
    for line in f:
        if not reading:
            l = line.split(" ")
            if l[0] == 'Title:':
                dic['title'] = " ".join(l[1:])
            elif l[0] == 'Author:':
                dic['author'] = " ".join(l[1:])
            elif (l[0] == 'Release' and l[1] == 'Date:'):
                s = '\"' + l[2] + ' ' + l[3] + ' ' + l[4] + '\"'
                dic['release_date'] = s
                if (len(l) > 6 and l[6][0] == '#'):
                    dic['ebook_id'] = l[6][1:-1]
            elif l[0] == 'Language:':
                dic['language'] = l[1]
            elif ' '.join(l[0:6]) == signOfStart:
                reading = True
                toprint = []
                if 'title' in dic.keys():
                    toprint.append(dic['title'].rstrip())
                else:
                    toprint.append('null')
                if 'author' in dic.keys():
                    toprint.append(dic['author'].rstrip())
                else:
                    toprint.append('null')
                if 'release_date' in dic.keys():
                    toprint.append(dic['release_date'].rstrip())
                else:
                    toprint.append('null')
                if 'ebook_id' in dic.keys():
                    toprint.append(dic['ebook_id'].rstrip())
                else:
                    toprint.append('null')
                if 'language' in dic.keys():
                    toprint.append(dic['language'].rstrip())
                else:
                    toprint.append('null')
                toprint.append('\"')
                towrite.write(','.join(toprint))
                dic = {}
        else:
            if ' '.join(line.split(" ")[0:6]) == signOfEnd:
                reading = False
                text += '\"'
                text += '\n'
                towrite.write(text)
                text = ''
            else:
                text += line

exit()