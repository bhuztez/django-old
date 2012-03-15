from django.db import DEFAULT_DB_ALIAS
from django.db.backends import BaseDatabaseWrapper
from django.db.backends.sqlite3.creation import DatabaseCreation


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'sqlite'

    def __init__(self, settings_dict, alias=DEFAULT_DB_ALIAS):
        super(DatabaseWrapper, self).__init__(settings_dict, alias)
        self.creation = DatabaseCreation(self)

    def _connection_arguments(self):
        settings_dict = self.settings_dict
        if not settings_dict['NAME']:
            from django.core.exceptions import ImproperlyConfigured
            raise ImproperlyConfigured("Please fill out the database NAME in the settings module before using the database.")
        
        return 'sqlite:///' + settings_dict['NAME']

    def close(self):
        # If database is in memory, closing the connection destroys the
        # database. To prevent accidental data loss, ignore close requests on
        # an in-memory db.
        if self.settings_dict['NAME'] != ":memory:":
            BaseDatabaseWrapper.close(self)

