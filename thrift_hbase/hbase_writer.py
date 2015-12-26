from thriftpy.protocol import TBinaryProtocol
from thriftpy.thrift import TClient
from thriftpy.transport import TBufferedTransport
from thriftpy.transport import TSocket

from tweeter_crawler.crawler.thrift_hbase.hbase_loader import HBaseThriftLoader

loader = HBaseThriftLoader()

@loader.import_classes
class HBaseConnection(object):

    @classmethod
    def init_resource(cls):
        tsocket = TSocket(host='hbasevm', port=9091)
        return TBufferedTransport(tsocket)

    def resource_get_connection(self):
        transport = HBaseConnection.init_resource()
        transport.open()
        protocol = TBinaryProtocol(transport)
        return TClient(self.THBaseService, protocol), protocol

class HBaseWriter(object):

    def __init__(self, table_name):
        self.conn, protocol = HBaseConnection().resource_get_connection()
        self.table_name = table_name

    def get_rows(self):
        scanner_id = self.conn.openScanner(table=self.table_name, scan=HBaseConnection.TScan())
        scanner_rows = self.conn.getScannerRows(scanner_id)
        return scanner_rows

    def put(self, key, column_family, qualifier, value):
        self.conn.put(self.table_name,
                      HBaseConnection.TPut(key, [HBaseConnection.TColumnValue(column_family, qualifier, value)]))


