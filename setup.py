import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='voleur',
    version='0.0.1',
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
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: POSIX :: Linux',
        'Operating System :: MacOS :: MacOS X',
    ],
    python_requires='>=3.7',
)
