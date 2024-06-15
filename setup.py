from setuptools import setup, find_packages

setup(
    name='ftp_to_s3',
    version='0.0.1',
    package_dir= {'':'src'},
    install_requires=[
        'boto3'
    ],
)