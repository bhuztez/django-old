"""
Wrapper for loading templates from "templates" directories in INSTALLED_APPS
packages.
"""

import pkgutil

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.template.base import TemplateDoesNotExist
from django.template.loader import BaseLoader
from django.utils._os import safe_join
from django.utils.importlib import import_module
from django.utils.resource_loading import AppPackageResourceLoader


app_template_loaders = None


def _refresh_app_template_loaders():
    global app_template_loaders
    loaders = []

    for app in settings.INSTALLED_APPS:
        try:
            mod = import_module(app)
        except ImportError, e:
            raise ImproperlyConfigured('ImportError %s: %s' % (app, e.args[0]))

        loader = AppPackageResourceLoader(app, 'templates')
        if loader.isdir():
            loaders.append(loader)

    # It won't change, so convert it to a tuple to save memory.
    app_template_loaders = tuple(loaders)

_refresh_app_template_loaders()


class Loader(BaseLoader):
    is_usable = True

    def load_template_source(self, template_name):
        if template_name.startswith('/'):
            template_name = template_name[1:]
        for loader in app_template_loaders:
            try:
                data = loader.get_data(template_name)
                if data is not None:
                    return data, u'app:%s/%s'%(loader, template_name)
            except Exception, e:
                pass

        raise TemplateDoesNotExist(template_name)

_loader = Loader()
