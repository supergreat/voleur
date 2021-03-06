import setuptools
import voleur

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='voleur',
    version=voleur.__version__,
    author='Supergreat Engineering',
    author_email='alexmic@supergreat.reviews',
    description=(
        'A tool for extracting, anonymizing and restoring data from/to '
        'PostgreSQL databases'
    ),
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/supergreat/voleur',
    packages=setuptools.find_packages(),
    install_requires=['boto3', 'docopt'],
    include_package_data=True,
    classifiers=[
        'Programming Language :: Python :: 3',
        'Development Status :: 2 - Pre-Alpha',
        'Operating System :: POSIX :: Linux',
        'Operating System :: MacOS :: MacOS X',
    ],
    python_requires='>=3.7',
    scripts=['bin/voleur'],
)
