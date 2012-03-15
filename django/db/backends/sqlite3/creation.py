import os

from django.db.backends.creation import BaseDatabaseCreation


class DatabaseCreation(BaseDatabaseCreation):

    def _get_test_db_name(self):
        test_database_name = self.connection.settings_dict['TEST_NAME']
        if test_database_name and test_database_name != ':memory:':
            return test_database_name
        return ':memory:'

    def _destroy_test_db(self, test_database_name, verbosity):
        if test_database_name and test_database_name != ":memory:":
            # Remove the SQLite database file
            os.remove(test_database_name)
        
    def test_db_signature(self):
        """
        Returns a tuple that uniquely identifies a test database.

        This takes into account the special cases of ":memory:" and "" for
        SQLite since the databases will be distinct despite having the same
        TEST_NAME. See http://www.sqlite.org/inmemorydb.html
        """
        settings_dict = self.connection.settings_dict
        test_dbname = self._get_test_db_name()
        sig = [self.connection.settings_dict['NAME']]
        if test_dbname == ':memory:':
            sig.append(self.connection.alias)
        return tuple(sig)

