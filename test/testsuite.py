#!/usr/bin/env python
"""Run unittests in the `tests` directory."""

from optparse import OptionParser
import sys

try:
    import unittest2 as unittest
except ImportError:
    import unittest

def main():
    parser = OptionParser()
    parser.add_option("-x", "--xml", action="store_true", dest="xml",
                  help="write test report in xunit file format (requires xmlrunner==1.7.4)")
    parser.add_option("-s", "--suffix", dest="suffix",
                  help="append suffix to test class names")
    (options, args) = parser.parse_args(sys.argv)
    loader = unittest.defaultTestLoader
    names = args[1:]
    if names:
        suite = loader.loadTestsFromNames(names)
    else:
        suite = loader.discover('test')

    if options.suffix:
        def rename_test_classes(suite_or_test):
            if isinstance(suite_or_test, unittest.TestSuite):
                for test in suite_or_test:
                    rename_test_classes(test)
            elif isinstance(suite_or_test, unittest.TestCase):
                cls = suite_or_test.__class__
                if options.suffix not in cls.__name__:
                    cls.__name__ = f"{cls.__name__}_{options.suffix}"
        
        rename_test_classes(suite)

    if options.xml:
        import xmlrunner
        runner = xmlrunner.XMLTestRunner(output='build/test-reports')
    else:
        runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    if result.wasSuccessful():
        return 0
    else:
        return 1
    
if __name__ == '__main__':
    sys.exit(main())
