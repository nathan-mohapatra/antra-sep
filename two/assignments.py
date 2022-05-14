import json

# 1. Create a script that will read and parse the given files and remove duplicates using python, then write back into a single CSV
def get_csv(infile):
    """
    Parses file, cleans data, and appends unique lines to csv file
    
    :infile: Input file
    """
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

unique_lines = set()  # to ignore duplicates

OUTFILE_PATH = './python/io/people.csv'
outfile = open(OUTFILE_PATH, 'a', encoding='utf-8')

INFILE_PATH = './python/io/people_1.txt'
with open(INFILE_PATH, 'r', encoding='utf-8') as infile:
    get_csv(infile)

INFILE_PATH = './python/io/people_2.txt'
with open(INFILE_PATH, 'r', encoding='utf-8') as infile:
    get_csv(infile)
        
outfile.close()

# 2. Split movie.json into 8 smaller JSON files.
NUM_SPLITS = 8

INFILE_PATH = './python/io/movie.json'
with open(INFILE_PATH, 'r', encoding='utf-8') as infile:
    json_data = json.load(infile)

# number of items in each output file
split_size = sum(1 for _ in json_data['movie']) // NUM_SPLITS
    
for i in range(NUM_SPLITS):
    json_split = json_data.copy()  # initialize
    # subset of items determined by split
    json_split['movie'] = json_data['movie'][(i * split_size):((i + 1) * split_size)]

    with open(f'./python/io/movie_{i + 1}.json', 'w', encoding='utf-8') as outfile:
        json.dump(json_split, outfile, indent=4, ensure_ascii=False)