from distutils.core import setup

setup(
    name = 'pyPiper',
    packages = ['pyPiper'],
    version = '0.2.0',
    description = 'A pipelining framework designed for data analysis but can be useful to other applications',
    author = 'daniyall',
    author_email = 'daniyal.l@outlook.com',
    url = 'https://github.com/daniyall/pyPiper',
    download_url = 'https://github.com/daniyall/pyPiper/archive/0.2.0.tar.gz',
    keywords = ['data-science', 'pipelining', 'stream-processing', "data-analysis"],
    classifiers = [],
    python_requires=">=3",
    license="LICENSE.txt",
    long_description=open('README.md', 'rt').read()
)