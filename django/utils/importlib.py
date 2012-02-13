# Taken from Python 2.7 with permission from/by the original author.
import os
import sys
import imp
import pkgutil
import warnings

def _resolve_name(name, package, level):
    """Return the absolute name of the module to be imported."""
    if not hasattr(package, 'rindex'):
        raise ValueError("'package' not set to a string")
    dot = len(package)
    for x in xrange(level, 1, -1):
        try:
            dot = package.rindex('.', 0, dot)
        except ValueError:
            raise ValueError("attempted relative import beyond top-level "
                              "package")
    return "%s.%s" % (package[:dot], name)


def import_module(name, package=None):
    """Import a module.

    The 'package' argument is required when performing a relative import. It
    specifies the package to use as the anchor point from which to resolve the
    relative import to an absolute import.

    """
    if name.startswith('.'):
        if not package:
            raise TypeError("relative imports require the 'package' argument")
        level = 0
        for character in name:
            if character != '.':
                break
            level += 1
        name = _resolve_name(name[level:], package, level)
    __import__(name)
    return sys.modules[name]


def find_package_path(name, path=None):
    """Finds search path for package with given name.

    The 'path' argument defaults to ``sys.path``.

    Raises ImportError if no search path could be found.
    """
    if path is None:
        path = sys.path

    results = []

    for path_item in path:
        importer = get_importer(path_item)

        if importer is None:
            continue

        try:
            loader = importer.find_module(name)

            if loader is not None:

                if not hasattr(loader, 'is_package'):
                    warnings.warn(
                        "Django cannot find search path for package '%s' ",
                        "under '%s', because the loader returned by '%s' does ",
                        "not implement 'is_package' method."%(
                            name,
                            path_item,
                            importer.__class__.__name__))
                    continue

                if not hasattr(loader, 'get_filename'):
                    warnings.warn(
                        "Django cannot find search path for package '%s' ",
                        "under '%s', because the loader returned by '%s' does ",
                        "not implement 'get_filename' method."%(
                            name,
                            path_item,
                            importer.__class__.__name__))
                    continue

                if loader.is_package(name):
                    results.append(os.path.dirname(loader.get_filename(name)))
        except ImportError:
            pass

    if not results:
        raise ImportError("No package named %s" % name)

    return results


get_importer = pkgutil.get_importer

try:
    import zipimport

    if hasattr(zipimport.zipimporter, 'get_filename'):
        class ZipImporter(zipimport.zipimporter):
            def get_filename(self, fullname):
                archivepath = os.path.join(self.archive, self.prefix)
                if self.is_package(fullname):
                    return os.path.join(archivepath, fullname, '__init__.py')

                return os.path.join(archivepath, fullname + '.py')

        def get_importer(path_item):
            importer = pkgutil.get_importer(path_item)

            if isinstance(importer, zipimport.zipimporter):
                archivepath = os.path.join(importer.archive, importer.prefix)
                importer = ZipImporter(os.path.dirname(archivepath))

            return importer

except ImportError:
    pass


