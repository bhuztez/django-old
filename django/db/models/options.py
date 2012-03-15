from bisect import bisect

from django.db.models.fields import FieldDoesNotExist, AutoField
from django.utils.datastructures import SortedDict
from django.utils.translation import string_concat

from sqlalchemy import MetaData, Table
from sqlalchemy.util import OrderedProperties



metadata = MetaData()


DEFAULT_NAMES = ('verbose_name', 'verbose_name_plural', 'db_table', 'ordering',
                 'unique_together', 'permissions', 'get_latest_by',
                 'order_with_respect_to', 'app_label', 'db_tablespace',
                 'abstract', 'managed', 'proxy', 'auto_created')


class Options(object):

    def __init__(self, meta, app_label=None):
        self.local_fields, self.local_many_to_many = [], []
        self.virtual_fields = []
        self.module_name, self.verbose_name = None, None
        self.verbose_name_plural = None
        self.db_table = ''
        self.ordering = []
        self.unique_together =  []
        self.permissions =  []
        self.object_name, self.app_label = None, app_label
        self.get_latest_by = None
        self.order_with_respect_to = None
        # self.db_tablespace = settings.DEFAULT_TABLESPACE
        self.admin = None
        self.meta = meta
        self.pk = None
        self.has_auto_field, self.auto_field = False, None
        self.abstract = False
        self.managed = True
        self.proxy = False
        # For any class which is a proxy (including automatically created
        # classes for deferred object loading) the proxy_for_model tells
        # which class this model is proxying. Note that proxy_for_model
        # can create a chain of proxy models. For non-proxy models the
        # variable is always None.
        self.proxy_for_model = None
        # For any non-abstract class the concrete class is the model
        # in the end of the proxy_for_model chain. In particular, for
        # concrete models the concrete_model is always the class itself.
        self.concrete_model = None
        self.parents = SortedDict()
        self.duplicate_targets = {}
        self.auto_created = False

        # To handle various inheritance situations, we need to track where
        # managers came from (concrete or abstract base classes).
        self.abstract_managers = []
        self.concrete_managers = []

        # List of all lookups defined in ForeignKey 'limit_choices_to' options
        # from *other* models. Needed for some admin checks. Internal use only.
        self.related_fkey_lookups = []


    def contribute_to_class(self, cls, name):
        setattr(cls, name, self)

        self.object_name = cls.__name__
        self.module_name = self.object_name.lower()
        
        if self.meta:
            meta_attrs = self.meta.__dict__.copy()
            for name in self.meta.__dict__:
                # Ignore any private attributes that Django doesn't care about.
                # NOTE: We can't modify a dictionary's contents while looping
                # over it, so we loop over the *original* dictionary instead.
                if name.startswith('_'):
                    del meta_attrs[name]
            for attr_name in DEFAULT_NAMES:
                if attr_name in meta_attrs:
                    setattr(self, attr_name, meta_attrs.pop(attr_name))
                elif hasattr(self.meta, attr_name):
                    setattr(self, attr_name, getattr(self.meta, attr_name))

            # unique_together can be either a tuple of tuples, or a single
            # tuple of two strings. Normalize it to a tuple of tuples, so that
            # calling code can uniformly expect that.
            ut = meta_attrs.pop('unique_together', self.unique_together)
            if ut and not isinstance(ut[0], (tuple, list)):
                ut = (ut,)
            self.unique_together = ut

            # verbose_name_plural is a special case because it uses a 's'
            # by default.
            if self.verbose_name_plural is None:
                self.verbose_name_plural = string_concat(self.verbose_name, 's')

            # Any leftover attributes must be invalid.
            if meta_attrs != {}:
                raise TypeError("'class Meta' got invalid attribute(s): %s" % ','.join(meta_attrs.keys()))
        else:
            self.verbose_name_plural = string_concat(self.verbose_name, 's')
        del self.meta
        

        if not self.db_table:
            self.db_table = "%s_%s" % (self.app_label, self.module_name)


    def _prepare(self, model):
        if self.pk is None:
            if self.parents:
                # Promote the first parent link in lieu of adding yet another
                # field.
                field = self.parents.value_for_index(0)
                # Look for a local field with the same name as the
                # first parent link. If a local field has already been
                # created, use it instead of promoting the parent
                already_created = [fld for fld in self.local_fields if fld.name == field.name]
                if already_created:
                    field = already_created[0]
                field.primary_key = True
                self.setup_pk(field)
            else:
                auto = AutoField(verbose_name='ID', primary_key=True,
                        auto_created=True)
                model.add_to_class('id', auto)

        columns = OrderedProperties()
        
        columns = [field.db_type() for field in self.local_fields ]

        self.table = Table(self.db_table, metadata, *columns)
        model.c = self.table.c



    def add_field(self, field):
        self.local_fields.insert(bisect(self.local_fields, field), field)
        self.setup_pk(field)
        if hasattr(self, '_field_cache'):
            del self._field_cache
            del self._field_name_cache

    def setup_pk(self, field):
        if not self.pk and field.primary_key:
            self.pk = field
            field.serialize = False

    def _fields(self):
        """
        The getter for self.fields. This returns the list of field objects
        available to this model (including through parent models).

        Callers are not permitted to modify this list, since it's a reference
        to this instance (not a copy).
        """
        try:
            self._field_name_cache
        except AttributeError:
            self._fill_fields_cache()
        return self._field_name_cache
    fields = property(_fields)

    def get_fields_with_model(self):
        """
        Returns a sequence of (field, model) pairs for all fields. The "model"
        element is None for fields on the current model. Mostly of use when
        constructing queries so that we know which model a field belongs to.
        """
        try:
            self._field_cache
        except AttributeError:
            self._fill_fields_cache()
        return self._field_cache

    def _fill_fields_cache(self):
        cache = []
        for parent in self.parents:
            for field, model in parent._meta.get_fields_with_model():
                if model:
                    cache.append((field, model))
                else:
                    cache.append((field, parent))
        cache.extend([(f, None) for f in self.local_fields])
        self._field_cache = tuple(cache)
        self._field_name_cache = [x for x, _ in cache]

    def get_field(self, name, many_to_many=True):
        """
        Returns the requested field by name. Raises FieldDoesNotExist on error.
        """
        to_search = self.fields
        for f in to_search:
            if f.name == name:
                return f
        raise FieldDoesNotExist('%s has no field named %r' % (self.object_name, name))


    def __repr__(self):
        return '<Options for %s>' % self.object_name

    def __str__(self):
        return "%s.%s" % (smart_str(self.app_label), smart_str(self.module_name))

