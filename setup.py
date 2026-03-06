from setuptools import setup
from Cython.Build import cythonize
import numpy

# Metadata is defined in pyproject.toml.
# This file exists solely to configure Cython extension modules,
# which cannot yet be fully expressed in pyproject.toml.
setup(
    ext_modules=cythonize([
        "hl7lite/parse_time_tz.pyx",
        "hl7lite/sanitize_unicode.pyx",
    ]),
    include_dirs=[numpy.get_include()],
    zip_safe=False,
)
