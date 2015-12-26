import thriftpy

__author__ = 'Omri Manor'

class HBaseThriftLoader(object):
    def __init__(self):
        self._hbase_service_thrift = thriftpy.load('C:/workspace/trunk/pydycommon/pydycommon/resources/hbase.thrift', module_name='hbase_service_thrift')

    def import_classes(self, cls):
        from hbase_service_thrift import THBaseService, TGet, TScan, TPut, TColumnValue, TColumnValue # @UnresolvedImport

        setattr(cls, THBaseService.__name__, THBaseService)
        setattr(cls, TGet.__name__, TGet)
        setattr(cls, TScan.__name__, TScan)
        setattr(cls, TPut.__name__, TPut)
        setattr(cls, TColumnValue.__name__, TColumnValue)
        return cls