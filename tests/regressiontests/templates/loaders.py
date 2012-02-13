"""
Test cases for the template loaders
"""

from django.conf import settings

if __name__ == '__main__':
    settings.configure()

import sys
import imp
import StringIO
import os.path

from django.template import TemplateDoesNotExist, Context
from django.template.loaders.app_directories import _refresh_app_template_loaders
from django.template.loaders.app_directories import Loader as EggLoader
from django.template import loader
from django.utils import unittest


class EggLoaderTest(unittest.TestCase):
    def setUp(self):
        self.old_path = sys.path[:]
        self.old_apps = settings.INSTALLED_APPS
        self.egg_dir = '%s/eggs' % os.path.dirname(__file__)
        egg_name = '%s/templatesegg.egg' % self.egg_dir
        sys.path.append(egg_name)

    def tearDown(self):
        settings.INSTALLED_APPS = self.old_apps
        _refresh_app_template_loaders()
        sys.path = self.old_path

    def test_empty(self):
        "Loading any template on an empty egg should fail"
        settings.INSTALLED_APPS = ['egg_empty']
        _refresh_app_template_loaders()
        egg_loader = EggLoader()
        self.assertRaises(TemplateDoesNotExist, egg_loader.load_template_source, "not-existing.html")

    def test_non_existing(self):
        "Template loading fails if the template is not in the egg"
        settings.INSTALLED_APPS = ['egg_1']
        _refresh_app_template_loaders()
        egg_loader = EggLoader()
        self.assertRaises(TemplateDoesNotExist, egg_loader.load_template_source, "not-existing.html")

    def test_existing(self):
        "A template can be loaded from an egg"
        settings.INSTALLED_APPS = ['egg_1']
        _refresh_app_template_loaders()
        egg_loader = EggLoader()
        contents, template_name = egg_loader.load_template_source("y.html")
        self.assertEqual(contents, "y")
        self.assertEqual(template_name, u"app:egg_1:templates/y.html")

    def test_not_installed(self):
        "Loading an existent template from an egg not included in INSTALLED_APPS should fail"
        settings.INSTALLED_APPS = []
        _refresh_app_template_loaders()
        egg_loader = EggLoader()
        self.assertRaises(TemplateDoesNotExist, egg_loader.load_template_source, "y.html")

class CachedLoader(unittest.TestCase):
    def setUp(self):
        self.old_TEMPLATE_LOADERS = settings.TEMPLATE_LOADERS
        settings.TEMPLATE_LOADERS = (
            ('django.template.loaders.cached.Loader', (
                    'django.template.loaders.filesystem.Loader',
                )
            ),
        )
    def tearDown(self):
        settings.TEMPLATE_LOADERS = self.old_TEMPLATE_LOADERS

    def test_templatedir_caching(self):
        "Check that the template directories form part of the template cache key. Refs #13573"
        # Retrive a template specifying a template directory to check
        t1, name = loader.find_template('test.html', (os.path.join(os.path.dirname(__file__), 'templates', 'first'),))
        # Now retrieve the same template name, but from a different directory
        t2, name = loader.find_template('test.html', (os.path.join(os.path.dirname(__file__), 'templates', 'second'),))

        # The two templates should not have the same content
        self.assertNotEqual(t1.render(Context({})), t2.render(Context({})))

class RenderToStringTest(unittest.TestCase):

    def setUp(self):
        self._old_TEMPLATE_DIRS = settings.TEMPLATE_DIRS
        settings.TEMPLATE_DIRS = (
            os.path.join(os.path.dirname(__file__), 'templates'),
        )

    def tearDown(self):
        settings.TEMPLATE_DIRS = self._old_TEMPLATE_DIRS

    def test_basic(self):
        self.assertEqual(loader.render_to_string('test_context.html'), 'obj:')

    def test_basic_context(self):
        self.assertEqual(loader.render_to_string('test_context.html',
                                                 {'obj': 'test'}), 'obj:test')

    def test_existing_context_kept_clean(self):
        context = Context({'obj': 'before'})
        output = loader.render_to_string('test_context.html', {'obj': 'after'},
                                         context_instance=context)
        self.assertEqual(output, 'obj:after')
        self.assertEqual(context['obj'], 'before')

    def test_empty_list(self):
        self.assertRaisesRegexp(TemplateDoesNotExist,
                                'No template names provided$',
                                loader.render_to_string, [])


    def test_select_templates_from_empty_list(self):
        self.assertRaisesRegexp(TemplateDoesNotExist,
                                'No template names provided$',
                                loader.select_template, [])
