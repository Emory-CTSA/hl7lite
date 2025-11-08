from setuptools import setup
from Cython.Build import cythonize
import numpy

setup(
    name="waveform",
    packages=["hl7lite"],
    ext_modules=cythonize("hl7lite/parse_time_tz.pyx"),
    include_dirs=[numpy.get_include()],
    zip_safe=False
)
