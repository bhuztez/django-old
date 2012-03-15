from django.conf import settings

from django.db import DEFAULT_DB_ALIAS
from django.db.backends.creation import BaseDatabaseCreation

from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool


class BaseDatabaseWrapper(object):
    """
    Represents a database connection.
    """
    ops = None
    vendor = 'unknown'

    def __init__(self, settings_dict, alias=DEFAULT_DB_ALIAS):
        # `settings_dict` should be a dictionary containing keys such as
        # NAME, USER, etc. It's called `settings_dict` instead of `settings`
        # to disambiguate it from Django settings modules.
        self._engine = None
        self.connection = None
        self.settings_dict = settings_dict
        self.alias = alias

        # Transaction related attributes
        self.transaction_state = []
        self._dirty = None
        
        self.creation = BaseDatabaseCreation(self)
        
    def _connection_arguments(self):
        raise NotImplementedError

    def _create_connection(self):
        if self._engine is None:
            self._engine =  create_engine(
                self._connection_arguments(),
                poolclass=NullPool)
        self.connection = self._engine.connect().execution_options(
            autocommit=not self.is_managed())

    def _commit(self):
        if self.connection is not None:
            return self.connection._commit_impl()

    def _rollback(self):
        if self.connection is not None:
            return self.connection._rollback_impl()


    def _enter_transaction_management(self, managed):
        """
        A hook for backend-specific changes required when entering manual
        transaction handling.
        """
        if self.connection is not None:
            self.connection = self.connection.execution_options(autocommit=not managed)


    def _leave_transaction_management(self, managed):
        """
        A hook for backend-specific changes required when leaving manual
        transaction handling. Will usually be implemented only when
        _enter_transaction_management() is also required.
        """
        if self.connection is not None:
            self.connection = self.connection.execution_options(autocommit=managed)

    
    def enter_transaction_management(self, managed=True):
        """
        Enters transaction management for a running thread. It must be balanced with
        the appropriate leave_transaction_management call, since the actual state is
        managed as a stack.

        The state and dirty flag are carried over from the surrounding block or
        from the settings, if there is no surrounding block (dirty is always false
        when no current block is running).
        """
        if self.transaction_state:
            self.transaction_state.append(self.transaction_state[-1])
        else:
            self.transaction_state.append(settings.TRANSACTIONS_MANAGED)

        if self._dirty is None:
            self._dirty = False
        self._enter_transaction_management(managed)

    def leave_transaction_management(self):
        """
        Leaves transaction management for a running thread. A dirty flag is carried
        over to the surrounding block, as a commit will commit all changes, even
        those from outside. (Commits are on connection level.)
        """
        self._leave_transaction_management(self.is_managed())
        if self.transaction_state:
            del self.transaction_state[-1]
        else:
            raise TransactionManagementError("This code isn't under transaction "
                "management")
        if self._dirty:
            self.rollback()
            raise TransactionManagementError("Transaction managed block ended with "
                "pending COMMIT/ROLLBACK")
        self._dirty = False

        
    def is_dirty(self):
        """
        Returns True if the current transaction requires a commit for changes to
        happen.
        """
        return self._dirty

    def set_dirty(self):
        """
        Sets a dirty flag for the current thread and code streak. This can be used
        to decide in a managed block of code to decide whether there are open
        changes waiting for commit.
        """
        if self._dirty is not None:
            self._dirty = True
        else:
            raise TransactionManagementError("This code isn't under transaction "
                "management")

    def set_clean(self):
        """
        Resets a dirty flag for the current thread and code streak. This can be used
        to decide in a managed block of code to decide whether a commit or rollback
        should happen.
        """
        if self._dirty is not None:
            self._dirty = False
        else:
            raise TransactionManagementError("This code isn't under transaction management")
        self.clean_savepoints()

    def clean_savepoints(self):
        self.connection.__savepoint_seq = 0


    def is_managed(self):
        """
        Checks whether the transaction manager is in manual or in auto state.
        """
        if self.transaction_state:
            return self.transaction_state[-1]
        return settings.TRANSACTIONS_MANAGED

    def managed(self, flag=True):
        """
        Puts the transaction manager into a manual state: managed transactions have
        to be committed explicitly by the user. If you switch off transaction
        management and there is a pending commit/rollback, the data will be
        commited.
        """
        top = self.transaction_state
        if top:
            top[-1] = flag
            if not flag and self.is_dirty():
                self._commit()
                self.set_clean()
        else:
            raise TransactionManagementError("This code isn't under transaction "
                "management")

    def commit_unless_managed(self):
        """
        Commits changes if the system is not in managed transaction mode.
        """
        if not self.is_managed():
            self._commit()
            self.clean_savepoints()
        else:
            self.set_dirty()

    def rollback_unless_managed(self):
        """
        Rolls back changes if the system is not in managed transaction mode.
        """
        if not self.is_managed():
            self._rollback()
        else:
            self.set_dirty()

    def commit(self):
        """
        Does the commit itself and resets the dirty flag.
        """
        self._commit()
        self.set_clean()

    def rollback(self):
        """
        This function does the rollback itself and resets the dirty flag.
        """
        self._rollback()
        self.set_clean()

    def savepoint(self):
        """
        Creates a savepoint (if supported and required by the backend) inside the
        current transaction. Returns an identifier for the savepoint that will be
        used for the subsequent rollback or commit.
        """
        return self.connection.begin_nested()

    def savepoint_rollback(self, transaction):
        """
        Rolls back the most recent savepoint (if one exists). Does nothing if
        savepoints are not supported.
        """
        transaction.rollback()

    def savepoint_commit(self, transaction):
        """
        Commits the most recent savepoint (if one exists). Does nothing if
        savepoints are not supported.
        """
        transaction.commit()
        
    def __getattr__(self, name):
        if self.connection is None:
            self._create_connection()

        return getattr(self.connection, name)


    def close(self):
        if self.connection is not None:
            print self.connection, 'closed'
            self.connection.close()
            self.connection = None



