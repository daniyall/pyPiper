from distutils.core import setup

try:
    from pypandoc import convert
    read_md = lambda f: convert(f, 'rst')
except ImportError:
    print("warning: pypandoc module not found, could not convert Markdown to RST")
    read_md = lambda f: open(f, 'r').read()

setup(
    name = 'pyPiper',
    packages = ['pyPiper'],
    version = '0.3.4',
    description = 'A pipelining framework designed for data analysis but can be useful to other applications',
    author = 'daniyall',
    author_email = 'daniyal.l@outlook.com',
    url = 'https://github.com/daniyall/pyPiper',
    download_url = 'https://github.com/daniyall/pyPiper/archive/0.3.3.tar.gz',
    keywords = ['data-science', 'pipelining', 'stream-processing', "data-analysis"],
    classifiers = [],
    python_requires=">=3",
    license="LICENSE",
    long_description=read_md('README.md')
)