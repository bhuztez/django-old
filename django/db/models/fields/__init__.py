from django.utils.encoding import smart_unicode, force_unicode, smart_str
from django.utils.translation import ugettext_lazy as _


from sqlalchemy import Column
from sqlalchemy.types import Integer, String, Date, DateTime

class NOT_PROVIDED:
    pass
    
class FieldDoesNotExist(Exception):
    pass

# A guide to Field parameters:
#
#   * name:      The name of the field specifed in the model.
#   * attname:   The attribute to use on the model object. This is the same as
#                "name", except in the case of ForeignKeys, where "_id" is
#                appended.
#   * db_column: The db_column specified in the model (or None).
#   * column:    The database column for this field. This is the same as
#                "attname", except if db_column is specified.
#
# Code that introspects values, or does other dynamic things, should use
# attname. For example, this gets the primary key value of object "obj":
#
#     getattr(obj, opts.pk.attname)

class Field(object):
    """Base class for all field types"""

    # These track each time a Field instance is created. Used to retain order.
    # The auto_creation_counter is used for fields that Django implicitly
    # creates, creation_counter is used for all user-specified fields.
    creation_counter = 0
    auto_creation_counter = -1

    # Generic field type description, usually overriden by subclasses
    def _description(self):
        return _(u'Field of type: %(field_type)s') % {
            'field_type': self.__class__.__name__
        }
    description = property(_description)


    def __init__(self, verbose_name=None, name=None, primary_key=False,
            max_length=None, unique=False, blank=False, null=False,
            db_index=False, rel=None, default=NOT_PROVIDED, editable=True,
            serialize=True, unique_for_date=None, unique_for_month=None,
            unique_for_year=None, choices=None, help_text='', db_column=None,
            db_tablespace=None, auto_created=False, validators=[],
            error_messages=None):
        self.name = name
        self.verbose_name = verbose_name
        self.primary_key = primary_key
        self.max_length, self._unique = max_length, unique
        self.blank, self.null = blank, null
        # # Oracle treats the empty string ('') as null, so coerce the null
        # # option whenever '' is a possible value.
        # if (self.empty_strings_allowed and
        #     connection.features.interprets_empty_strings_as_nulls):
        #     self.null = True
        self.rel = rel
        self.default = default
        self.editable = editable
        self.serialize = serialize
        self.unique_for_date, self.unique_for_month = (unique_for_date,
                                                       unique_for_month)
        self.unique_for_year = unique_for_year
        self._choices = choices or []
        self.help_text = help_text
        self.db_column = db_column
        # self.db_tablespace = db_tablespace or settings.DEFAULT_INDEX_TABLESPACE
        self.auto_created = auto_created

        # Set db_index to True if the field has a relationship and doesn't
        # explicitly set db_index.
        self.db_index = db_index

        if auto_created:
            self.creation_counter = Field.auto_creation_counter
            Field.auto_creation_counter -= 1
        else:
            self.creation_counter = Field.creation_counter
            Field.creation_counter += 1


    def __cmp__(self, other):
        # This is needed because bisect does not take a comparison function.
        return cmp(self.creation_counter, other.creation_counter)


    def db_type(self):
        # return a sqlalchemy column instead
        return Column(
            self.column,
            self.get_internal_type(),
            key = self.name,
            primary_key = self.primary_key,
            index = self.db_index,
            nullable = self.null,
            unique = self._unique)


    @property
    def unique(self):
        return self._unique or self.primary_key


    def set_attributes_from_name(self, name):
        if not self.name:
            self.name = name
        self.attname, self.column = self.get_attname_column()
        if self.verbose_name is None and self.name:
            self.verbose_name = self.name.replace('_', ' ')

    def contribute_to_class(self, cls, name):
        self.set_attributes_from_name(name)
        self.model = cls
        cls._meta.add_field(self)

    def get_attname(self):
        return self.name

    def get_attname_column(self):
        attname = self.get_attname()
        column = self.db_column or attname
        return attname, column

    def get_internal_type(self):
        raise NotImplementedError

    def pre_save(self, model_instance, add):
        """
        Returns field's value just before saving.
        """
        return getattr(model_instance, self.attname)


    def has_default(self):
        """
        Returns a boolean of whether this field has a default value.
        """
        return self.default is not NOT_PROVIDED

    def get_default(self):
        """
        Returns the default value for this field.
        """
        if self.has_default():
            if callable(self.default):
                return self.default()
            return force_unicode(self.default, strings_only=True)



    def __repr__(self):
        """
        Displays the module, class and name of the field.
        """
        path = '%s.%s' % (self.__class__.__module__, self.__class__.__name__)
        name = getattr(self, 'name', None)
        if name is not None:
            return '<%s: %s>' % (path, name)
        return '<%s>' % path



class AutoField(Field):
    description = _("Integer")

    def __init__(self, *args, **kwargs):
        assert kwargs.get('primary_key', False) is True, \
               "%ss must have primary_key=True." % self.__class__.__name__
        kwargs['blank'] = True
        Field.__init__(self, *args, **kwargs)


    def get_internal_type(self):
        return Integer


    def contribute_to_class(self, cls, name):
        assert not cls._meta.has_auto_field, \
               "A model can't have more than one AutoField."
        super(AutoField, self).contribute_to_class(cls, name)
        cls._meta.has_auto_field = True
        cls._meta.auto_field = self


class DateField(Field):
    description = _("Date (without time)")

    def __init__(self, verbose_name=None, name=None, auto_now=False,
                 auto_now_add=False, **kwargs):
        self.auto_now, self.auto_now_add = auto_now, auto_now_add
        if auto_now or auto_now_add:
            kwargs['editable'] = False
            kwargs['blank'] = True
        Field.__init__(self, verbose_name, name, **kwargs)


    def get_internal_type(self):
        return Date


class DateTimeField(DateField):
    description = _("Date (with time)")

    def get_internal_type(self):
        return DateTime



class CharField(Field):

    def get_internal_type(self):
        return String(self.max_length)


