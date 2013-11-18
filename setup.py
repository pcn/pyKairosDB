import sys
import os

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup
    from distutils.util import convert_path

    def _find_packages(where='.', exclude=()):
        """Return a list all Python packages found within directory 'where'

        'where' should be supplied as a "cross-platform" (i.e. URL-style) path; it
        will be converted to the appropriate local path syntax.  'exclude' is a
        sequence of package names to exclude; '*' can be used as a wildcard in the
        names, such that 'foo.*' will exclude all subpackages of 'foo' (but not
        'foo' itself).
        """
        out = []
        stack = [(convert_path(where), '')]
        while stack:
            where, prefix = stack.pop(0)
            for name in os.listdir(where):
                fn = os.path.join(where, name)
                if ('.' not in name and os.path.isdir(fn) and
                        os.path.isfile(os.path.join(fn, '__init__.py'))):
                    out.append(prefix+name)
                    stack.append((fn, prefix + name + '.'))
        for pat in list(exclude)+['ez_setup', 'distribute_setup']:
            from fnmatch import fnmatchcase
            out = [item for item in out if not fnmatchcase(item, pat)]
        return out

    find_packages = _find_packages

PUBLISH_CMD = "python setup.py register sdist upload"
TEST_CMD = 'nosetests'

if 'test' in sys.argv:
    try:
        __import__('nose')
    except ImportError:
        print('nose required.')
        sys.exit(1)

    os.system(TEST_CMD)
    sys.exit()

if 'publish' in sys.argv:
    os.system(PUBLISH_CMD)
    sys.exit()


def cheeseshopify(rst):
    '''Since PyPI doesn't support the RST `code-block` directive or the
    :code: role, replace all `code-block` directives with `::` and
    `:code:` with "".
    '''
    ret = rst.replace(".. code-block:: python", "::").replace(":code:", "")
    return ret

# with open('README.rst') as fp:
#     long_desc = cheeseshopify(fp.read())
#
with open('README.md') as fp:
    long_desc = fp.read()
with open('LICENSE') as fp:
    license = fp.read()

setup(
    name='pyKairosDB',
    version='0.1.0',
    description='Python library to interface with KairosDB',
    long_description=long_desc,
    license=license,
    author='Peter C. Norton',
    author_email='null@void.com',
    url='https://github.com/pcn/pyKairosDB',
    install_requires=['PyYAML', 'requests'],
    packages=find_packages(exclude=('test',)),
    package_data={
        "text": ["*.txt", "*.xml"],
    },
    classifiers=(
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
    ),
    tests_require=['nose'],
)
