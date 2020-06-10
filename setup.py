# root/setup.py

from setuptools import setup
setup(name='pyParallelMR',
      version='0.1',
      description='A python package to convert CPU-bound tasks ' +
      'to parallel mapReduce jobs.',
      url='https://github.com/k4rth33k/pyMR',
      author='k4rth33k',
      author_email='kartheek2000mike@gmail.com',
      license='MIT',
      python_requires='>=3.2S',
      packages=['pyMR'],
      zip_safe=False)
