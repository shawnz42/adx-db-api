import os
from setuptools import setup, find_packages, Command

NAME = 'adx_db'
DESCRIPTION = 'Python DB-API and SQLAlchemy interface for ADX.'
URL = ''
EMAIL = ''
AUTHOR = ''


class CleanCommand(Command):
    """Custom clean command to tidy up the project root."""
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        os.system('rm -vrf ./build ./dist ./*.pyc ./*.tgz ./*.egg-info')


REQUIRED = [
    'requests>=2.20.0',
    'moz_sql_parser==3.32.20026',
    'pyparsing==2.3.1',
    'azure-kusto-data>=2.3.1',
    'mo-future>=3.31.20024'
]

sqlalchemy_extras = [
    'sqlalchemy',
]

# -----------------------------------------------------------------
here = os.path.abspath(os.path.dirname(__file__))

long_description = ''

# Load the package's __version__.py module as a dictionary.
about = {}
with open(os.path.join(here, NAME, '__version__.py')) as f:
    exec(f.read(), about)

setup(
    name=NAME,
    version=about['__version__'],
    description=DESCRIPTION,
    long_description=long_description,
    author=AUTHOR,
    author_email=EMAIL,
    url=URL,
    license='MIT License',
    packages=find_packages(),
    include_package_data=True,
    platforms='any',
    entry_points={
        'sqlalchemy.dialects': [
            'adx = adx_db.dialect:AdxDialect',
        ],
    },
    install_requires=REQUIRED,
    extras_require={
        'sqlalchemy': sqlalchemy_extras,
    },
    cmdclass={
        'clean': CleanCommand,
    }
)
