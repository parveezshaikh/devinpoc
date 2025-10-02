import os
import shutil
from setuptools import setup


OUTPUT_DIR = 'output'


if __name__ == "__main__":
    setup(
        name="my_module",
        packages=['src/etl', 'src/factory', 'src/filter', 'src/input', 'src/inputtable', 'src/join', 'src/lib',
                  'src/output', 'src/sort', 'src/util', 'src/workflow'],
        version="1.0",
        script_args=['--quiet', 'bdist_egg'], # to create egg-file only
    )

    egg_name = os.listdir('dist')[0]

    os.rename(
        os.path.join('dist', egg_name),
        os.path.join(OUTPUT_DIR, egg_name)
    )

    shutil.rmtree('build')
    shutil.rmtree('dist')
    shutil.rmtree('my_module.egg-info')