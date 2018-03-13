# CPacket-Common-Modules

## Prerequisites

You need to install the packages first before using the project. You can use pip:

    * pip install numpy
    * - pip install pandas
    * - pip install scipy
    * - pip install scikit-learn
    * - pip install matplotlib 
    * - pip install numpy
    * - pip install --upgrade git+git://github.com/influxdata/influxdb-python.git
    * - pip install influxdb
    * - pip install coverage
    * - pip install coveralls
    * - pip install nose
    * - pip install progressbar2

Or you can install the packages with PyCharm.

## Before any Commit

To make sure that Travis CI works correctly and do not fail when the new package added it is important to edit .travis.yml which contatins configuration parameters for Travis.   
For this, before commiting the code that uses new package you also need to make sure this package will be added to Travis.    
Usually the package is added in the same manner as you add it to the project. For example, if you added NumPy to the project Travis will need to use this command:

    * pip install numpy
    
If you are using a package that comes from GitHub repository you will need to use this commands:

    * pip install --upgrade git+git://github.com/username/package_name.git
    * pip install package_name

When you are having conflicts with making Travis work, please make sure to contact with QA Lead.
