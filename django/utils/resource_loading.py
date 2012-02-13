import os.path
import pkgutil
try:
    import cStringIO as StringIO
except:
    import StringIO

from django.utils._os import safe_join
from django.utils.importlib import import_module

class ResourceLoader(object):

    def isdir(self, path=None):
        raise NotImplementedError

    def get_data(self, path):
        raise NotImplementedError

    def get_stream(self, path):
        raise NotImplementedError

    def get_filename(self, path):
        raise NotImplementedError



class FileSystemResourceLoader(ResourceLoader):

    def __init__(self, path):
        self.path = path

    def isdir(self, path=None):
        if path is None:
            return os.path.isdir(self.path)

        return os.path.isdir(self.get_filename(path))

    def get_data(self, path):
        fp = self.get_stream(path)
        try:
            return fp.read()
        finally:
            fp.close()

    def get_stream(self, path):
        return open(self.get_filename(path), 'rb')

    def get_filename(self, path):
        return safe_join(self.path, path)




class AppPackageResourceLoader(ResourceLoader):

    def __init__(self, package, path):
        self.package = package
        self.path = path

    def isdir(path=None):    
        return True

    def get_data(self, path):
        return pkgutil.get_data(self.package, safe_join('/'+self.path, path)[1:])

    def get_stream(self, path):
        data = self.get_data(path)
        if data is not None:
            return StringIO.StringIO(data)

    def get_filename(self, path):
        if isinstance(pkgutil.get_loader(self.package), pkgutil.ImpLoader):
            mod = import_module(self.package)
            return safe_join(mod.__path__[0], self.path, path)

        raise Exception("sorry")
        
    def __unicode__(self):
        return u'%s:%s'%(self.package, self.path)






