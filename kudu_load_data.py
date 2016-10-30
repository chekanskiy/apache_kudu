import sys
import kudu

client = kudu.connect('localhost', 7051)
table = client.table('payment_history')   # Defining table name
print(table.schema)
session = client.new_session()  # Opening a session with the client

# ============================================= Creating Data Dict for Dummy Data ======================================
table_col_prefix = 'col_'
table_col_suffixes = ['_text', '_int']
text_cols = 5
int_cols = 20
columns = ['wallet_id', 'txn_id', 'timestamp']
columns_data_types = {'wallet_id': int, 'txn_id': int, 'timestamp': str}
for i in range(text_cols):
    col = table_col_prefix + str(i) + table_col_suffixes[0]
    columns.append(col)
    columns_data_types[col] = str
for i in range(int_cols):
    col = table_col_prefix + str(i) + table_col_suffixes[1]
    columns.append(col)
    columns_data_types[col] = int

# ============================================= Reading stdin and writing to DB in batches =============================
cnt_lines = 0
for line in sys.stdin:
    cnt_lines += 1
    data_dict = {}
    try:
        for col, data in zip(columns, line.strip().split("\t")):
            if columns_data_types[col] == int:
                data = int(data)
            elif columns_data_types[col] == str:
                data = str(data)
            data_dict[col] = data
        if len(data_dict) < len(columns):
            continue
    except:
        continue
        # print(cnt_lines, col, data, columns_data_types[col])
        # print(line)
        # print(data_dict)
        # raise
    op = table.new_insert()  # defining an insert
    for key in data_dict.keys():
        op[key] = data_dict[key]  # inserting items
    session.apply(op) # applying insert to session for the last batch

    if cnt_lines % 1000 == 0:
        session.flush()  # writing data to DB for the current batch
session.flush()  # writing data to DB for the last batch

print("ok")
