import kudu
from kudu.client import Partitioning

client = kudu.connect('localhost', 7051)

# ========================================= Creating Columns Dict for Dummy Data =======================================
table_col_prefix = 'col_'
table_col_suffixes = ['_text', '_int']
text_cols = 5
int_cols = 20

text_columns = []
int_columns = []
for i in range(text_cols):
    text_columns.append(table_col_prefix + str(i) + table_col_suffixes[0])
for i in range(int_cols):
    int_columns.append(table_col_prefix + str(i) + table_col_suffixes[1])
# ======================================================================================================================

builder = kudu.schema_builder()

# Adding columns to Kudu Schema
builder.add_column('wallet_id', kudu.int64, nullable=False)
builder.add_column('txn_id', kudu.int64, nullable=False)
builder.add_column('timestamp', kudu.string, nullable=False)

for col in int_columns:
    builder.add_column(col, kudu.int64, nullable=False)
for col in text_columns:
    builder.add_column(col, kudu.string, nullable=False)  # double

# Adding Primary Keys
builder.set_primary_keys(['wallet_id', 'txn_id'])
# Building Schema
schema = builder.build()

# Creating Table
if client.table_exists('payment_history'):
    print(client.list_tables())
    client.delete_table('payment_history')

# Defining Partitioning Method
# partitioning = Partitioning().add_hash_partitions('wallet_id', 2)
# partitioning = Partitioning().set_range_partition_columns(['wallet_id'])
partitioning = Partitioning().set_range_partition_columns([])
client.create_table('payment_history', schema, partitioning)

print(schema)

print("ok")
