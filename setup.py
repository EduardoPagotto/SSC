#!./venv/bin/python3
'''
Created on 20220926
Update on 20220926
@author: Eduardo Pagotto
'''

from setuptools import setup, find_packages

from SSC.server.__init__ import __version__ as VERSION

PACKAGE = "SSC"

# listar os packages
#python -c "from setuptools import setup, find_packages; print(find_packages())"

setup(
    name="SSC",
    version=VERSION,
    author="Eduardo Pagotto",
    author_email="edupagotto@gmail.com",
    description="Simple Flow Control",
    long_description="not know",#long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/EduardoPagotto/SSC.git",
    packages=find_packages(),
    license="MIT",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ],
    install_requires=['certifi',
                      'charset-normalizer',
                      'click',
                      'Flask',
                      'idna',
                      'itsdangerous',
                      'Jinja2',
                      'MarkupSafe',
                      'requests',
                      'types-requests',
                      'setuptools',
                      'tinydb',
                      'tomli',
                      'typing_extensions',
                      'urllib3',
                      'Werkzeug',
                      'wheel'])
