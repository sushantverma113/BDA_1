import csv, sqlite3

con = sqlite3.connect("BDA")
cur = con.cursor()

# cur.execute("DROP TABLE IF EXISTS pullreq")
# cur.execute("CREATE TABLE pullreq (seq int , username varchar (50), status varchar (50), timestamp timestamp )")
#
#
# with open('pullreq_events.csv') as csv_file:
#     csv_reader = csv.reader(csv_file, delimiter=',')
#     for row in csv_reader:
#         cur.execute("INSERT INTO pullreq VALUES (? , ?,?, ?)", (int(row[0]) , row[1] , row[2] , row[3]));


cur.execute('select username, max(num) as mx from ( select username, count(*) as num from pullreq where status = "discussed" group by YEAR(timestamp), month (timestamp), username ) as s group by username order by mx desc limit 1;')
print(cur.fetchall())

con.commit()

print("HELLO")