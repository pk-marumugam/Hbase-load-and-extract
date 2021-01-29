# Check below site for commands in happybase module to access hbase tables.
# https://github.com/python-happybase/happybase/blob/master/doc/user.rst
# Retrieves all rows from Hbase that start with 1.
import hbConnection


if __name__ == '__main__':
    table_name = 'training:orders'
    cn,tbl = connect_to_hbase(table_name)
    for key, data in tbl.scan(row_prefix='1'):
        print(key, data)
    cn.close()
