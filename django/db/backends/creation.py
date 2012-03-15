from django.db.utils import load_backend

# The prefix to put on the default database name when creating
# the test database.
TEST_DATABASE_PREFIX = 'test_'


class BaseDatabaseCreation(object):
    """
    This class encapsulates all backend-specific differences that pertain to
    database *creation*, such as the column types to use for particular Django
    Fields, the SQL used to create and destroy tables, and the creation and
    destruction of test databases.
    """

    def __init__(self, connection):
        self.connection = connection

    def _get_test_db_name(self):
        """
        Internal implementation - returns the name of the test DB that will be
        created. Only useful when called from create_test_db() and
        _create_test_db() and when no external munging is done with the 'NAME'
        or 'TEST_NAME' settings.
        """
        if self.connection.settings_dict['TEST_NAME']:
            return self.connection.settings_dict['TEST_NAME']
        return TEST_DATABASE_PREFIX + self.connection.settings_dict['NAME']


    def create_test_db(self, verbosity=1, autoclobber=False):
        """
        Creates a test database, prompting the user for confirmation if the
        database already exists. Returns the name of the test database created.
        """
        # Don't import django.core.management if it isn't needed.
        from django.core.management import call_command
        test_database_name = self._get_test_db_name()

        if verbosity >= 1:
            test_db_repr = ''
            if verbosity >= 2:
                test_db_repr = " ('%s')" % test_database_name
            print "Creating test database for alias '%s'%s..." % (
                self.connection.alias, test_db_repr)

        self.connection.close()
        self.connection.settings_dict["NAME"] = test_database_name

        call_command('syncdb',
            database=self.connection.alias)

        return test_database_name


    def destroy_test_db(self, old_database_name, verbosity=1):
        """
        Destroy a test database, prompting the user for confirmation if the
        database already exists.
        """
        self.connection.close()
        test_database_name = self.connection.settings_dict['NAME']
        if verbosity >= 1:
            test_db_repr = ''
            if verbosity >= 2:
                test_db_repr = " ('%s')" % test_database_name
            print "Destroying test database for alias '%s'%s..." % (
                self.connection.alias, test_db_repr)

        # Temporarily use a new connection and a copy of the settings dict.
        # This prevents the production database from being exposed to potential
        # child threads while (or after) the test database is destroyed.
        # Refs #10868 and #17786.
        settings_dict = self.connection.settings_dict.copy()
        settings_dict['NAME'] = old_database_name
        backend = load_backend(settings_dict['ENGINE'])
        new_connection = backend.DatabaseWrapper(
                             settings_dict,
                             alias='__destroy_test_db__')
        new_connection.creation._destroy_test_db(test_database_name, verbosity)



    def test_db_signature(self):
        """
        Returns a tuple with elements of self.connection.settings_dict (a
        DATABASES setting value) that uniquely identify a database
        accordingly to the RDBMS particularities.
        """
        settings_dict = self.connection.settings_dict
        return (
            settings_dict['HOST'],
            settings_dict['PORT'],
            settings_dict['ENGINE'],
            settings_dict['NAME']
        )

