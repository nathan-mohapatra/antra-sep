# 1. Create a script that will read and parse the given files and remove duplicates using python, then write back into a single CSV
unique_lines = set()  # to ignore duplicates
outfile = open('./Python/files/people.csv', 'a', encoding='utf-8')

def get_csv(infile):
    next(infile)  # skip header
    for line in infile:
        line = line.split()

        first, last = line[0], line[1]
        line[0], line[1] = first.title(), last.title()

        phone = line[3]
        if len(phone) == 10:
            line[3] = '-'.join([phone[:3], phone[3:6], phone[6:]])

        address_no = line[4]
        line[4] = address_no[-4:]

        line = ','.join(line)  # csv

        if line not in unique_lines:
            unique_lines.add(line)
            outfile.write(line + '\n')

with open('./Python/files/people_1.txt', 'r', encoding='utf-8') as infile:
    get_csv(infile)

with open('./Python/files/people_2.txt', 'r', encoding='utf-8') as infile:
    get_csv(infile)
        
outfile.close()