import sys
import kudu

client = kudu.connect('localhost', 7051)
table = client.table('payment_history')   # Defining table name
print(table.schema, "\n")

scanner = table.scanner()

# scanner.add_predicate(table['timestamp'] == '2016-05-15 10:00:00')

id_col = table['txn_id']
# scanner.add_predicate(table['timestamp'] == '2014-12-02 02:00:00')
# scanner.add_predicates([id_col >= 7292, id_col <= 7295])
scanner.add_predicates([id_col >= 7292, id_col <= 7295])

scanner.open()
print(scanner.read_all_tuples())