# Monitor Manager Web Report Python Api

### Table of Contents

1. [Installation](#installation)
2. [Project Motivation](#motivation)
3. [Main features](#features)
 
## Installation <a name="installation"></a>

The source code is currently hosted on GitHub at: https://github.com/mahulo2009/monitormanager-webreportapi-python.git

Binary installers for the latest released version are available at the Python Package Index (PyPI).

```sh
pip install mmwebreportapi
```

## Project Motivation <a name="motivation"></a>

Monitor Manager Web Report Api is a python package that provides a set of tools to easily make request to the Grantecan 
Monitor Manager Service. The Monitor Manager service collects, persists and propagates
the samples for all the Grantecan Control System components: dome, optics, main axis etc..., produces
several thousand of data per second making some time difficult to efficiently and easily extract this data for analysis.

This Api is fully integrated with Pandas, the most powerful and flexible open source data analysis tool in any 
language.

## Main features <a name="features"></a>

* Simple interface to extract with the minimum requery information complex data.
* Cache management to optimize the time necessary to obtain the data.
* Pagination of the result to treat with large volume of data.
* Filtering similar data values base on epsilon value.
* Treatment of all monitor data type in a uniform way: n-dimension monitor and enumerates.
* Integration with Pandas to generate Data Frame to easily do data science with tabular data.
* Integrity sanity check of the request.



