# docker run --name test-cassandra-v2 -p 9042:9042  cassandra:latest
# docker exec -it test-cassandra-v2 bash
# pip3 install cassandra-driver

from cassandra.cluster import Cluster

cluster = Cluster(['0.0.0.0'], port=9042)
session = cluster.connect('employee')

#! Reading Data From Cassandra [simple query]
print("Reading data simply..")
rows = session.execute('SELECT * FROM employee_details;')
for employee_row in rows:
    print(employee_row)
    print(f'Meet {employee_row.name}! He lives in {employee_row.city}.')

#! Reading Data From Cassandra [optimized query]
prepared_statement = session.prepare('SELECT * FROM employee_details WHERE id=?')
employees_to_lookup = [1, 2]

print("Reading data using prepared statements..")
for employee_id in employees_to_lookup:
    employee = session.execute(prepared_statement, [employee_id]).one()
    print(employee)

#! Writing data into cassandra
session.execute("INSERT INTO employee_details (id, age, city, name) VALUES (99,20,'Chicago','Max');")
session.execute_async("INSERT INTO employee_details (id, age, city, name) VALUES (400,25,'Seattle','Bob');")