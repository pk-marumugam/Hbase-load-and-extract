import happybase as hb

def connect_to_hbase(table_name):
    """ Connect to HBase server with localhost port 9090 which is Thrift is running."""

    conn = hb.Connection('localhost', 9090, autoconnect=False)
    conn.open()
    table = conn.table(table_name)

    return conn, table

