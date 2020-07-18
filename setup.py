# root/setup.py

from setuptools import setup
with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='pyParallelMR',
      version='0.2',
      description='A python package to convert CPU-bound tasks ' +
      'to parallel mapReduce jobs.',
      long_description=long_description,
      long_description_content_type="text/markdown",
      url='https://github.com/k4rth33k/pyMR',
      author='k4rth33k',
      author_email='kartheek2000mike@gmail.com',
      license='MIT',
      python_requires='>=3.2S',
      packages=['pyMR'],
      zip_safe=False)
