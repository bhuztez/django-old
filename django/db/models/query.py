import datetime

from sqlalchemy.sql import extract, exists, select, and_, or_, alias, literal, literal_column
from sqlalchemy.types import Integer

from django.db import router, connections
from django.db.models.fields import FieldDoesNotExist



CHUNK_SIZE = 100
ITER_CHUNK_SIZE = CHUNK_SIZE


class QuerySet(object):
    def __init__(self, model=None, query=None, using=None):
        self.model = model
        # EmptyQuerySet instantiates QuerySet with model as None
        self._db = using
        self.query = query
        self._result_cache = None
        self._iter = None
        self._for_write = False
        self._offset = None
        self._limit = None
        self._ordering = None
        self._extras = []


    def __or__(self, other):
        assert other.__class__ == self.__class__
        assert other.model == self.model
        assert other.db == self.db
        if self.query is not None and other.query is not None:
            query = or_(self.query, other.query)
        else:
            query = None
        return self.__class__(self.model, query, self._db)
    

    def __and__(self, other):
        assert self._offset is None and self._limit is None, "Cannot combine queries once a slice has been taken."
    
        assert other.__class__ == self.__class__
        assert other.model == self.model
        assert other.db == self.db
        if self.query is not None and other.query is not None:
            query = and_(self.query, other.query)
        elif self.query is not None:
            query = self.query
        elif other.query is not None:
            query = other.query
        else:
            query = None
        return self.__class__(self.model, query, self._db)


    def __getitem__(self, k):
        """
        Retrieves an item or slice from the set of results.
        """
        if not isinstance(k, (slice, int, long)):
            raise TypeError
        assert ((not isinstance(k, slice) and (k >= 0))
                or (isinstance(k, slice) and (k.start is None or k.start >= 0)
                    and (k.stop is None or k.stop >= 0))), \
                "Negative indexing is not supported."

        if self._result_cache is not None:
            if self._iter is not None:
                # The result cache has only been partially populated, so we may
                # need to fill it out a bit more.
                if isinstance(k, slice):
                    if k.stop is not None:
                        # Some people insist on passing in strings here.
                        bound = int(k.stop)
                    else:
                        bound = None
                else:
                    bound = k + 1
                if len(self._result_cache) < bound:
                    self._fill_cache(bound - len(self._result_cache))
            return self._result_cache[k]

        if isinstance(k, slice):
            qs = self._clone()
            if k.start is not None:
                start = int(k.start)
            else:
                start = None
            if k.stop is not None:
                stop = int(k.stop)
            else:
                stop = None
            qs._offset = start
            qs._limit = stop-(start or 0) if stop else None
            if self._offset is not None:
                qs._offset = self._offset + (start or 0)
            if self._limit is not None:
                if qs._offset:
                    limit = self._limit - qs._offset - (self._offset or 0)
                    if qs._limit is None or qs._limit > limit:
                        qs._limit = limit
            return k.step and list(qs)[::k.step] or qs

        try:
            qs = self._clone()
            qs._offset = k + (self._offset or 0)
            qs._limit = 1
            return list(qs)[0]
        except self.model.DoesNotExist, e:
            raise IndexError(e.args)


    def __len__(self):
        # Since __len__ is called quite frequently (for example, as part of
        # list(qs), we make some effort here to be as efficient as possible
        # whilst not messing up any existing iterators against the QuerySet.
        if self._result_cache is None:
            if self._iter:
                self._result_cache = list(self._iter)
            else:
                self._result_cache = list(self.iterator())
        elif self._iter:
            self._result_cache.extend(self._iter)
        # if self._prefetch_related_lookups and not self._prefetch_done:
        #     self._prefetch_related_objects()
        return len(self._result_cache)


    def _clone(self, klass=None, setup=False, **kwargs):
        if klass is None:
            klass = self.__class__
        # query = self.query.clone()
        # if self._sticky_filter:
        #     query.filter_is_sticky = True
        c = klass(model=self.model, query=self.query, using=self._db)
        c._for_write = self._for_write
        c._offset = self._offset
        c._limit = self._limit
        c._ordering = self._ordering
        c._extras = list(self._extras)
        # c._prefetch_related_lookups = self._prefetch_related_lookups[:]
        # c.__dict__.update(kwargs)
        # if setup and hasattr(c, '_setup_query'):
        #     c._setup_query()
        return c


    def filter(self, *args, **kwargs):
        """
        Returns a new QuerySet instance with the args ANDed to the existing
        set.
        """
        assert not kwargs or self._offset is None and self._limit is None, "Cannot filter a query once a slice has been taken."

        assert not args
        clone = self._clone()
        LOOKUP_SEP = '__'

        for k,v in kwargs.items():
            parts = k.split(LOOKUP_SEP)
            if len(parts) == 1:
                part = parts[0]
                if part == 'pk':
                    part = self.model._meta.pk.attname
                
                clause = clone.model.c[part] == v

            elif len(parts) == 2:
                part = parts[0]
                if part == 'pk':
                    part = self.model._meta.pk.attname
                        
                if parts[1] == 'exact':
                    clause = clone.model.c[part] == v
                elif parts[1] in ['year', 'month', 'day', 'week_day']:
                # http://lucumr.pocoo.org/2011/7/19/sqlachemy-and-you/
                    if parts[1] == 'week_day':
                        v = v-1
                    clause = extract(
                        {
                            'year': 'year',
                            'month': 'month', 
                            'day': 'day',
                            'week_day': 'dow'}[parts[1]],
                        clone.model.c[part]) == v
                elif parts[1] == 'startswith':
                    clause = clone.model.c[part].startswith(v)
                elif parts[1] == 'in':
                    clause = clone.model.c[part].in_(v)
                elif parts[1] == 'lte':
                    clause = clone.model.c[part] <= v
                else:
                    raise TypeError((parts, v))
            else:
                raise TypeError(parts)
                
            if clone.query is None:
                clone.query = clause
            else:
                clone.query = and_(clone.query, clause)
        
        return clone


    def get(self, *args, **kwargs):
        """
        Performs the query and returns a single object matching the given
        keyword arguments.
        """
        clone = self.filter(*args, **kwargs)
        # if self.query.can_filter():
        #     clone = clone.order_by()
        num = len(clone)
        if num == 1:
            return clone._result_cache[0]
        if not num:
            raise self.model.DoesNotExist("%s matching query does not exist."
                    % self.model._meta.object_name)
        raise self.model.MultipleObjectsReturned("get() returned more than one %s -- it returned %s! Lookup parameters were %s"
                % (self.model._meta.object_name, num, kwargs))


    def create(self, **kwargs):
        """
        Creates a new object with the given kwargs, saving it to the database
        and returning the created object.
        """
        obj = self.model(**kwargs)
        self._for_write = True
        obj.save(force_insert=True, using=self.db)
        return obj


    @property
    def db(self):
        "Return the database that will be used if this query is executed now"
        if self._for_write:
            return self._db or router.db_for_write(self.model)
        return self._db or router.db_for_read(self.model)


    def order_by(self, *fields):
        assert self._offset is None and self._limit is None, "Cannot reorder a query once a slice has been taken."
        self._ordering = fields

    def iterator(self):
        """
        An iterator over the results from applying this QuerySet to the
        database.
        """
        # Cache db and model outside the loop
        db = self.db
        model = self.model
        
        # ordering = [model.c[c].asc() for c in (self._ordering or model._meta.ordering) ] or None

        # query = model._meta.table.select(order_by=ordering, offset=self._offset, limit=self._limit)
        # if self.query is not None:
        #     query = query.where(self.query)
        
        # for extra in self._extras:
        #     query = query.append_column(extra)

        ordering = [model.c[c].asc() for c in (self._ordering or model._meta.ordering) ] or None

        query = select(list(self.model.c) + self._extras, self.query, [self.model._meta.table], order_by=ordering, offset=self._offset, limit=self._limit)


        for row in connections[self.db].execute(query):
            obj = model(**dict((k,v) for k,v in row.items() if k in model.c))
            for k,v in row.items():
                if k not in model.c:
                    setattr(obj, k, v)
            
            obj._state.db = db
            obj._state.adding = False
            yield obj
 
    def using(self, alias):
        """
        Selects which database this QuerySet should excecute its query against.
        """
        clone = self._clone()
        clone._db = alias
        return clone

    def exists(self):
        if self._result_cache is None:
            assert self.query is not None
            query = select((exists().where(self.query),))
            return connections[self.db].execute(query).scalar()
            
        return bool(self._result_cache)


    def extra(self, select=None, where=None, params=None, tables=None,
              order_by=None, select_params=None):
        """
        Adds extra SQL fragments to the query.
        """
        clone = self._clone()
        for k,v in select.items():
           # http://stackoverflow.com/questions/3576382/select-as-in-sqlalchemy
           clone._extras.append(literal_column(v).label(k))

        return clone


    def __iter__(self):
        if self._result_cache is None:
            self._iter = self.iterator()
            self._result_cache = []
        if self._iter:
            return self._result_iter()
        # Python's list iterator is better than our version when we're just
        # iterating over the cache.
        return iter(self._result_cache)


    def _result_iter(self):
        pos = 0
        while 1:
            upper = len(self._result_cache)
            while pos < upper:
                yield self._result_cache[pos]
                pos = pos + 1
            if not self._iter:
                raise StopIteration
            if len(self._result_cache) <= pos:
                self._fill_cache()

    def _fill_cache(self, num=None):
        """
        Fills the result cache with 'num' more entries (or until the results
        iterator is exhausted).
        """
        if self._iter:
            try:
                for i in range(num or ITER_CHUNK_SIZE):
                    self._result_cache.append(self._iter.next())
            except StopIteration:
                self._iter = None


    def _update(self, values):
        """
        A version of update that accepts field objects instead of field names.
        Used primarily for model saving and not intended for use by general
        code (it requires too much poking around at model internals to be
        useful at that level).
        """
        model = self.model

        statement = model._meta.table.update().where(
            self.query).values(
            dict((model.c[f.attname],v) for (f,g,v) in values))
        
        return connections[self.db].execute(statement)

    _update.alters_data = True
    
    
    def dates(self, field, kind, order='ASC'):        
        return DateQuerySet(
            field = field,
            kind = kind,
            order = order,
            model = self.model,
            query = self.query,
            using = self._db)


    def delete(self):
        model = self.model
        query = model._meta.table.delete()
        if self.query is not None:
            query = query.where(self.query)   
        
        connections[self.db].execute(query)
        
    
    def values(self, *fields):
        values = []
        for field in fields:
            if field in self.model.c:
                values.append(self.model.c[field])
            else:
                for extra in self._extras:
                    if field == extra.name:
                        values.append(extra)
                        break
                else:
                    raise Exception("field not found")
        
        
        qs = ValuesQuerySet(values, model=self.model, query=self.query, using=self._db)
        
        qs._for_write = self._for_write
        qs._offset = self._offset
        qs._limit = self._limit
        qs._ordering = self._ordering
        qs._extras = list(self._extras)
        
        return qs


class ValuesQuerySet(QuerySet):

    def __init__(self, fields, model=None, query=None, using=None):
        super(ValuesQuerySet, self).__init__(model, query, using)
        self._fields = fields
    
    def iterator(self):
        """
        An iterator over the results from applying this QuerySet to the
        database.
        """
        # Cache db and model outside the loop
        db = self.db
        model = self.model
        
        ordering = [model.c[c].asc() for c in (self._ordering or model._meta.ordering) ] or None

        query = select(self._fields, self.query, [self.model._meta.table], order_by=ordering, offset=self._offset, limit=self._limit)
                    

        for row in connections[self.db].execute(query):
            yield dict(row)

   


def _create_date(year, month=1, day=1, *args):
    return datetime.datetime(year, month, day, *args)


class DateQuerySet(QuerySet):

    def __init__(self, field, kind, order, model, query, using):
        if field not in model.c:
            raise FieldDoesNotExist("%s has no field named '%s'"%(model.__name__, field) )

        assert kind in ['year', 'month', 'day'], "'kind' must be one of 'year', 'month' or 'day'."
        assert order in ['ASC', 'DESC'], "'order' must be either 'ASC' or 'DESC'."

        super(DateQuerySet, self).__init__(model, query, using)
        self.field = field
        self.kind = kind
        self.order = order



    def _clone(self, klass=None, setup=False, **kwargs):
        if klass is None:
            klass = self.__class__
        c = klass(
            field = field,
            kind = kind,
            order = order,
            model = self.model,
            query = self.query,
            using = self._db)
        return c
        
    def iterator(self):
        db = self.db
        model = self.model
        field = self.model.c[self.field]

        columns = []
        
        columns.append(extract('year', field))
        if self.kind != 'year':
            columns.append(extract('month', field))
            if self.kind != 'month':
                columns.append(extract('day', field))

        query = select(columns, self.query, [self.model._meta.table], distinct=True)
        if self.query is not None:
            query = query.where(self.query)
        
        if self.order == 'ASC':
            query = query.order_by(field.asc())
        elif self.order == 'DESC':
            query = query.order_by(field.desc())
        else:
            raise Exception()

        for row in connections[self.db].execute(query):
            yield _create_date(*row)




class EmptyQuerySet(object):
    pass


class RawQuerySet(object):
    pass



def insert_query(model, objs, fields, return_id=False, raw=False, using=None):
    result = connections[using].execute(
        model._meta.table.insert(),
        [ dict((f.column, f.pre_save(obj, True)) for f in fields if f.pre_save(obj, True)) 
          for obj in objs ])
    return result.inserted_primary_key




