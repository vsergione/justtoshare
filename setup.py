from setuptools import setup

setup(
    name='zabbix-tagger',
    version='1.0',
    py_modules=['main'],
    install_requires=[
        'pandas',
        'requests',
    ],
    entry_points={
        'console_scripts': [
            'zabbix-tagger=main:main',
        ],
    },
) 