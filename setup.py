import setuptools

setuptools.setup(
    name='mmwebreportapi',
    packages=['mmwebreport','mmwebreport.core','mmwebreport.retrieve'],
    version='0.0.1',
    description='Monitor Manager Web Rerpot API',
    author='Manuel Huertas Lopez',
    author_email='manuel.huertas.lopez@gmail.com',
    install_requires=['pandas', 'requests'],
    download_url="https://github.com/mahulo2009/monitormanager-webreportapi-python/archive/refs/tags/v1.0.0-alpha.tar.gz"
)