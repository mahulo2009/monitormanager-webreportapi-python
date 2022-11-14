import setuptools

# Load the long_description from README.md
with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='mmwebreportapi',
    version='0.0.5',
    author='Manuel Huertas Lopez',
    author_email='manuel.huertas.lopez@gmail.com',
    description='Monitor Manager Web Rerpot API',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mahulo2009/monitormanager-webreportapi-python.git",
    packages=['mmwebreport', 'mmwebreport.core', 'mmwebreport.retrieve','mmwebreport.main'],
    install_requires=['pandas', 'requests'],
    entry_points={
        'console_scripts': [
            'mm = mmwebreport.main.main:Main'
            ],
        },
    download_url="https://github.com/mahulo2009/monitormanager-webreportapi-python/archive/refs/tags/v1.0.5-alpha.zip"
)