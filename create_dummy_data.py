from random import choice
import string
import datetime

table_col_prefix = 'col_'
table_col_suffixes = ['_text', '_int']
text_cols = 5
int_cols = 20
columns = ['wallet_id']
for i in range(text_cols):
    columns.append(table_col_prefix + str(i) + table_col_suffixes[0])
for i in range(text_cols):
    columns.append(table_col_prefix + str(i) + table_col_suffixes[1])


def rand_text(len):
    return "".join([choice(string.ascii_letters) for i in range(len)])


def rand_int(len):
    return "".join([choice(string.digits) for i in range(len)])


def generate_row(wallet, timestamp, txn_id, text_cols, int_cols):
    int_len = 5
    str_len = 15

    out = [wallet, str(txn_id), str(timestamp)]

    for txt_col in range(text_cols):
        out.append(rand_text(str_len))

    for int_col in range(int_cols):
        out.append(rand_int(int_len))

    return "\t".join(out)


time_str = '01/12/2016'
ts = datetime.datetime.strptime(time_str, '%d/%m/%Y')
timestamps = []
for day in range(1, 365*2+1):   # ph_len = 7300
    time = ts - datetime.timedelta(day)
    for i in range(1, 11):
        timestamps.append(time + datetime.timedelta(0, 60*60*i))


txn_id = 0
block = ""
wallet = '79' + str(rand_int(9))
for ts in timestamps:
    txn_id += 1
    block += generate_row(wallet, ts, txn_id,  text_cols, int_cols) + "\n"
print(block)










