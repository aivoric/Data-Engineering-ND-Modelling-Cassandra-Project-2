import csv
from cassandra.cluster import Cluster
from cql_queries import create_db, create_queries, insert_queries, select_queries, drop_queries


# Connect to database:
try: 
    cluster = Cluster(['127.0.0.1']) #If you have a locally installed Apache Cassandra instance
    session = cluster.connect()
except Exception as e:
    print(e)


# Create keyspace
try:
    session.execute(create_db)
except Exception as e:
    print(e)
    

# Use the new kespace
try:
    session.set_keyspace('sparkifydb')
except Exception as e:
    print(e)
    

# Create tables:
for query in create_queries:
    try:
        session.execute(query)
    except Exception as e:
        print(e)
    

# Insert data into tables:
file = 'event_datafile_new.csv'
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        session.execute(insert_queries[0], (int(line[8]), int(line[3]), str(line[0]), str(line[9]), float(line[5])))
        session.execute(insert_queries[1], (int(line[10]), int(line[8]), int(line[3]), str(line[0]), str(line[9]),  str(line[1]), str(line[4])))
        session.execute(insert_queries[2], (str(line[9]), int(line[10]), str(line[1]), str(line[4])))


# Select data from tables:
for query in select_queries:
    try:
        print("\n\n#########################################\n Running Query: {}".format(query))
        rows = session.execute(query)
    except Exception as e:
        print(e)
        
    print("\nResults:\n")
    for row in rows:
        print(row)
    

# Drop all tables    
for query in drop_queries:
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)


# Shutdown db connection
session.shutdown()
cluster.shutdown()