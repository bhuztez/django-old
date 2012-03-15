import sys
from itertools import izip

import django.db.models.manager     # Imported to register signal handler.
from django.core.exceptions import (ObjectDoesNotExist,
    MultipleObjectsReturned)
from django.db import router, transaction
from django.db.models.fields import AutoField
from django.db.models.options import Options
from django.db.models import signals
from django.db.models.loading import register_models, get_model
from django.utils.functional import Promise
from django.utils.encoding import smart_str, force_unicode



class ModelBase(type):

    def __new__(cls, name, bases, attrs):
        super_new = super(ModelBase, cls).__new__
        parents = [b for b in bases if isinstance(b, ModelBase)]
        if not parents:
            # If this isn't a subclass of Model, don't do anything special.
            return super_new(cls, name, bases, attrs)

        # Create the class.
        module = attrs.pop('__module__')
        new_class = super_new(cls, name, bases, {'__module__': module})
        attr_meta = attrs.pop('Meta', None)
        abstract = getattr(attr_meta, 'abstract', False)
        if not attr_meta:
            meta = getattr(new_class, 'Meta', None)
        else:
            meta = attr_meta
        base_meta = getattr(new_class, '_meta', None)

        if getattr(meta, 'app_label', None) is None:
            # Figure out the app_label by looking one level up.
            # For 'django.contrib.sites.models', this would be 'sites'.
            model_module = sys.modules[new_class.__module__]
            kwargs = {"app_label": model_module.__name__.split('.')[-2]}
        else:
            kwargs = {}

        new_class.add_to_class('_meta', Options(meta, **kwargs))

        if not abstract:
            new_class.add_to_class('DoesNotExist', subclass_exception('DoesNotExist',
                    tuple(x.DoesNotExist
                            for x in parents if hasattr(x, '_meta') and not x._meta.abstract)
                                    or (ObjectDoesNotExist,), module))
            new_class.add_to_class('MultipleObjectsReturned', subclass_exception('MultipleObjectsReturned',
                    tuple(x.MultipleObjectsReturned
                            for x in parents if hasattr(x, '_meta') and not x._meta.abstract)
                                    or (MultipleObjectsReturned,), module))
            if base_meta and not base_meta.abstract:
                # Non-abstract child classes inherit some attributes from their
                # non-abstract parent (unless an ABC comes before it in the
                # method resolution order).
                if not hasattr(meta, 'ordering'):
                    new_class._meta.ordering = base_meta.ordering
                if not hasattr(meta, 'get_latest_by'):
                    new_class._meta.get_latest_by = base_meta.get_latest_by

        m = get_model(new_class._meta.app_label, name,
                      seed_cache=False, only_installed=False)
        if m is not None:
            return m

        for obj_name, obj in attrs.items():
            new_class.add_to_class(obj_name, obj)


        new_class._prepare()
        register_models(new_class._meta.app_label, new_class)

        # Because of the way imports happen (recursively), we may or may not be
        # the first time this model tries to register with the framework. There
        # should only be one class for each model, so we always return the
        # registered version.
        return get_model(new_class._meta.app_label, name,
                         seed_cache=False, only_installed=False)
        
    def add_to_class(cls, name, value):
        if hasattr(value, 'contribute_to_class'):
            value.contribute_to_class(cls, name)
        else:
            setattr(cls, name, value)

    def _prepare(cls):
        """
        Creates some methods once self._meta has been populated.
        """
        opts = cls._meta
        opts._prepare(cls)

        signals.class_prepared.send(sender=cls)


class ModelState(object):
    """
    A class for storing instance state
    """
    def __init__(self, db=None):
        self.db = db
        # If true, uniqueness validation checks will consider this a new, as-yet-unsaved object.
        # Necessary for correct validation of new instances of objects with explicit (non-auto) PKs.
        # This impacts validation only; it has no effect on the actual save.
        self.adding = True

class Model(object):
    __metaclass__ = ModelBase
    _deferred = False


    def __init__(self, *args, **kwargs):
        # Set up the storage for instance state
        self._state = ModelState()

        # There is a rather weird disparity here; if kwargs, it's set, then args
        # overrides it. It should be one or the other; don't duplicate the work
        # The reason for the kwargs check is that standard iterator passes in by
        # args, and instantiation for iteration is 33% faster.
        args_len = len(args)
        if args_len > len(self._meta.fields):
            # Daft, but matches old exception sans the err msg.
            raise IndexError("Number of args exceeds number of fields")

        fields_iter = iter(self._meta.fields)
        if not kwargs:
            # The ordering of the izip calls matter - izip throws StopIteration
            # when an iter throws it. So if the first iter throws it, the second
            # is *not* consumed. We rely on this, so don't change the order
            # without changing the logic.
            for val, field in izip(args, fields_iter):
                if isinstance(val, Promise):
                    val = force_unicode(val)
                setattr(self, field.attname, val)
        else:
            # Slower, kwargs-ready version.
            for val, field in izip(args, fields_iter):
                if isinstance(val, Promise):
                    val = force_unicode(val)
                setattr(self, field.attname, val)
                kwargs.pop(field.name, None)
                # # Maintain compatibility with existing calls.
                # if isinstance(field.rel, ManyToOneRel):
                #     kwargs.pop(field.attname, None)

        # Now we're left with the unprocessed fields that *must* come from
        # keywords, or default.

        for field in fields_iter:
            is_related_object = False
            # # This slightly odd construct is so that we can access any
            # # data-descriptor object (DeferredAttribute) without triggering its
            # # __get__ method.
            # if (field.attname not in kwargs and
            #         isinstance(self.__class__.__dict__.get(field.attname), DeferredAttribute)):
            #     # This field will be populated on request.
            #     continue
            if kwargs:
                # if isinstance(field.rel, ManyToOneRel):
                #     try:
                #         # Assume object instance was passed in.
                #         rel_obj = kwargs.pop(field.name)
                #         is_related_object = True
                #     except KeyError:
                #         try:
                #             # Object instance wasn't passed in -- must be an ID.
                #             val = kwargs.pop(field.attname)
                #         except KeyError:
                #             val = field.get_default()
                #     else:
                #         # Object instance was passed in. Special case: You can
                #         # pass in "None" for related objects if it's allowed.
                #         if rel_obj is None and field.null:
                #             val = None
                # else:
                try:
                    val = kwargs.pop(field.attname)
                except KeyError:
                    # This is done with an exception rather than the
                    # default argument on pop because we don't want
                    # get_default() to be evaluated, and then not used.
                    # Refs #12057.
                    val = field.get_default()
            else:
                val = field.get_default()
            if is_related_object:
                # If we are passed a related instance, set it using the
                # field.name instead of field.attname (e.g. "user" instead of
                # "user_id") so that the object gets properly cached (and type
                # checked) by the RelatedObjectDescriptor.
                setattr(self, field.name, rel_obj)
            else:
                if isinstance(val, Promise):
                    val = force_unicode(val)
                setattr(self, field.attname, val)

        if kwargs:
            for prop in kwargs.keys():
                try:
                    if isinstance(getattr(self.__class__, prop), property):
                        setattr(self, prop, kwargs.pop(prop))
                except AttributeError:
                    pass
            if kwargs:
                raise TypeError("'%s' is an invalid keyword argument for this function" % kwargs.keys()[0])
        super(Model, self).__init__()

    def __repr__(self):
        try:
            u = unicode(self)
        except (UnicodeEncodeError, UnicodeDecodeError):
            u = '[Bad Unicode data]'
        return smart_str(u'<%s: %s>' % (self.__class__.__name__, u))

    def __str__(self):
        if hasattr(self, '__unicode__'):
            return force_unicode(self).encode('utf-8')
        return '%s object' % self.__class__.__name__

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self._get_pk_val() == other._get_pk_val()

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self._get_pk_val())


    def _get_pk_val(self, meta=None):
        if not meta:
            meta = self._meta
        return getattr(self, meta.pk.attname)

    def _set_pk_val(self, value):
        return setattr(self, self._meta.pk.attname, value)

    pk = property(_get_pk_val, _set_pk_val)

    def save(self, force_insert=False, force_update=False, using=None):
        """
        Saves the current instance. Override this in a subclass if you want to
        control the saving process.

        The 'force_insert' and 'force_update' parameters can be used to insist
        that the "save" must be an SQL insert or update (or equivalent for
        non-SQL backends), respectively. Normally, they should not be set.
        """
        if force_insert and force_update:
            raise ValueError("Cannot force both insert and updating in model saving.")
        self.save_base(using=using, force_insert=force_insert, force_update=force_update)

    save.alters_data = True

    def save_base(self, raw=False, cls=None, origin=None, force_insert=False,
            force_update=False, using=None):
        """
        Does the heavy-lifting involved in saving. Subclasses shouldn't need to
        override this method. It's separate from save() in order to hide the
        need for overrides of save() to pass around internal-only parameters
        ('raw', 'cls', and 'origin').
        """
        using = using or router.db_for_write(self.__class__, instance=self)
        assert not (force_insert and force_update)
        if cls is None:
            cls = self.__class__
            meta = cls._meta
            if not meta.proxy:
                origin = cls
        else:
            meta = cls._meta

        # if origin and not meta.auto_created:
        #     signals.pre_save.send(sender=origin, instance=self, raw=raw, using=using)

        # If we are in a raw save, save the object exactly as presented.
        # That means that we don't try to be smart about saving attributes
        # that might have come from the parent class - we just save the
        # attributes we have been given to the class we have been given.
        # We also go through this process to defer the save of proxy objects
        # to their actual underlying model.
        if not raw or meta.proxy:
            if meta.proxy:
                org = cls
            else:
                org = None
            for parent, field in meta.parents.items():
                # At this point, parent's primary key field may be unknown
                # (for example, from administration form which doesn't fill
                # this field). If so, fill it.
                if field and getattr(self, parent._meta.pk.attname) is None and getattr(self, field.attname) is not None:
                    setattr(self, parent._meta.pk.attname, getattr(self, field.attname))

                self.save_base(cls=parent, origin=org, using=using)

                if field:
                    setattr(self, field.attname, self._get_pk_val(parent._meta))
            if meta.proxy:
                return

        if not meta.proxy:
            non_pks = [f for f in meta.local_fields if not f.primary_key]

            # First, try an UPDATE. If that doesn't update anything, do an INSERT.
            pk_val = self._get_pk_val(meta)
            pk_set = pk_val is not None
            record_exists = True
            manager = cls._base_manager
            if pk_set:
                # Determine whether a record with the primary key already exists.
                if (force_update or (not force_insert and
                        manager.using(using).filter(pk=pk_val).exists())):
                    # It does already exist, so do an UPDATE.
                    if force_update or non_pks:
                        values = [(f, None, (raw and getattr(self, f.attname) or f.pre_save(self, False))) for f in non_pks]
                        if values:
                            rows = manager.using(using).filter(pk=pk_val)._update(values)
                            if force_update and not rows:
                                raise DatabaseError("Forced update did not affect any rows.")
                else:
                    record_exists = False
            if not pk_set or not record_exists:
                if meta.order_with_respect_to:
                    # If this is a model with an order_with_respect_to
                    # autopopulate the _order field
                    field = meta.order_with_respect_to
                    order_value = manager.using(using).filter(**{field.name: getattr(self, field.attname)}).count()
                    self._order = order_value

                fields = meta.local_fields
                if not pk_set:
                    if force_update:
                        raise ValueError("Cannot force an update in save() with no primary key.")
                    fields = [f for f in fields if not isinstance(f, AutoField)]

                record_exists = False

                update_pk = bool(meta.has_auto_field and not pk_set)
                result = manager._insert([self], fields=fields, return_id=update_pk, using=using, raw=raw)

                if update_pk:
                    setattr(self, meta.pk.attname, result[0])
            transaction.commit_unless_managed(using=using)

        # Store the database on which the object was saved
        self._state.db = using
        # Once saved, this is no longer a to-be-added instance.
        self._state.adding = False

        # Signal that the save is complete
        # if origin and not meta.auto_created:
        #     signals.post_save.send(sender=origin, instance=self,
        #         created=(not record_exists), raw=raw, using=using)


    save_base.alters_data = True


    def delete(self, using):
        pass



def subclass_exception(name, parents, module):
    return type(name, parents, {'__module__': module})



