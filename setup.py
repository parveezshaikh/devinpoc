from setuptools import setup, find_packages

setup(name='pyspark',
      version='0.0.1',
      description='A sample PySpark application',
      author='Bilal',
      py_moudles=['__main__'],
      packages=['src/etl', 'src/factory','src/filter','src/input','src/inputtable','src/join','src/lib','src/output','src/sort','src/util','src/workflow'],
      )


#python setup.py bdist_eg