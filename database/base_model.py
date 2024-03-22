from sqlalchemy import event, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.ddl import DDL


class BaseModel:
    __instance = None
    base = None
    archive_base = None

    def __init__(self):
        if BaseModel.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            BaseModel.__instance = self

    @staticmethod
    def get_instance():
        if BaseModel.__instance is None:
            BaseModel()
        return BaseModel.__instance

    @staticmethod
    def init_base(config):
        if 'schema' in config['default']:
            BaseModel.base = declarative_base(metadata=MetaData(schema=config['default']['schema']))
            event.listen(BaseModel.base.metadata, 'before_create',
                         DDL(BaseModel.get_schema_create_query(config['default']['schema'], config['default']['driver'])))
        else:
            BaseModel.base = declarative_base()
        event.listen(BaseModel.base.metadata, 'after_create', BaseModel.after_table_creation)
        try:
            archive_config = config['archive_sp']['schema']
        except:
            archive_config = None

        if archive_config:
            BaseModel.archive_base = declarative_base(metadata=MetaData(schema=config['archive_sp']['schema']))
            event.listen(BaseModel.archive_base.metadata, 'before_create',
                         DDL(BaseModel.get_schema_create_query(archive_config, config['archive_sp']['driver'])))
            event.listen(BaseModel.archive_base.metadata, 'after_create', BaseModel.after_table_creation)

    @staticmethod
    def get_base(schema=None, driver=None):
        if BaseModel.base is None:
            BaseModel.init_base(schema, driver)

        return BaseModel.base

    @staticmethod
    def get_schema_create_query(schema, driver):
        if driver == 'postgresql':
            return 'CREATE SCHEMA IF NOT EXISTS ' + schema

    @staticmethod
    def after_table_creation(target, connection, **kw):
        print('after table creation')
