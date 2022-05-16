import json

# 1. Create a script that will read and parse the given files and remove duplicates using python, 
# then write back into a single CSV
def get_csv(infile):
    with open('./python/io/people.csv', 'a', encoding='utf-8') as outfile:
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

with open('./python/io/people_1.txt', 'r', encoding='utf-8') as infile1:
    get_csv(infile1)

with open('./python/io/people_2.txt', 'r', encoding='utf-8') as infile2:
    get_csv(infile2)


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


# 3. A paragraph on what PaaS, SaaS and IaaS are and the differences between them.
"""
    Infrastructure as a Service (IaaS) provides end users with cloud-based alternatives to physical, 
on-premise infrastructure, allowing businesses to purchase resources on-demand and eliminate capital 
expenditure. IaaS works primarily with cloud-based and pay-as-you-go services such as storage,
networking, and virtualization. Platform as a Service (PaaS) provides developers with a framework, 
software, and tools needed to develop applications and software——all accessible through the internet.
Often seen as a scaled-down version of IaaS, PaaS gives its customers a broader access to servers,
storage, and networking, all managed by a third-party provider. PaaS focuses primarily on hardware
and software tools available over the internet. Software as a Service (SaaS) is the most commonly 
used service within the cloud market. SaaS platforms make software available to users over the 
internet, usually for a monthly subscription fee. They are typically ready-to-use and run from a 
user's web browser, which allows businesses to avoid any additional downloads or application
installations. To summarize, IaaS builds the infrastructure of a cloud-based technology, PaaS helps
developers build custom applications via an API that can be delivered over the cloud, and SaaS is
cloud-based software that companies can sell and use. In addition to differing use cases with pros
and cons, these services present a tradeoff between direct control and flexibility and ease of
operation.
"""


# 4. A paragraph on the differences between ETL and ELT. Also, list the pros and cons of each in a 
# chart.
"""
    While they sound similar, the Extract, Transform, and Load process for data (ETL) and the
Extract, Load, and Transform process for data (ELT) are quite different. In ETL, data moves from
the data source to staging into the data warehouse; however, in ELT, there is no need for data 
staging because the data warehouse (or more practically the data lake) is leveraged to do basic
transformations. ETL can help with data privacy and compliance by cleaning sensitive and secure data
even before loading into the data warehouse, and can perform sophisticated data transformations while
being more cost-effective than ELT.
-----------------------------------------------------------------------------------------------------
ETL Pros                                        | ETL Cons          
------------------------------------------------|----------------------------------------------------                                
- Well-developed process with experts readily   | - Only transforms and loads data that you decide
available; easier to develop                    | is necessary when creating data warehouse
                                                |
- Can redact or remove sensitive information    | - Not compatible with data lakes
before putting it into a data warehouse or      |
cloud server; satisfies compliance standards    | - Not ideal for dealing with massive amounts of
and protects data                               | structured and unstructured data
                                                |
- Ideal for dealing with smaller datasets that  | - Aggregation becomes more complicated as dataset
require complex transformations                 | increases in size
                                                |
- Requires little maintenance (if cloud-based)  | - Multi-stage process necessitates longer load
                                                | times
-----------------------------------------------------------------------------------------------------
ELT Pros                                        | ELT Cons
------------------------------------------------|----------------------------------------------------
- Loads all data immediately; users can         | - Relatively new technology without many experts
determine which data to transform or analyze    | readily available; more challenging to develop
                                                |
- Compatible with data lakes                    | - Requires you to upload data before redacting or
                                                | removing sensitive information; could violate
- Ideal for dealing with massive amount of      | compliance standards or endanger data
structured and unstructured data                |
                                                | - Not ideal for dealing with smaller datasets that
- Can quickly process massive amounts of data   | require complex transformations
                                                |
- Requires little maintenance                   | - The need to continuously transform data slows
                                                | down the total time it takes for analysis
-----------------------------------------------------------------------------------------------------
"""