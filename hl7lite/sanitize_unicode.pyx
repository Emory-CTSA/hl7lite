# cython: language_level=3
from libc.string cimport strchr
from cpython.unicode cimport PyUnicode_READ_CHAR
from cpython.unicode cimport PyUnicode_GET_LENGTH
from cpython.unicode cimport PyUnicode_AsUTF8String
from cpython.set cimport PySet_Add
from cpython.dict cimport PyDict_GetItem

def c_sanitize_unicode(str input_str, dict replacements):
    cdef Py_ssize_t i, length
    cdef set non_ascii_chars = set()
    cdef list output = []
    cdef str ch
    cdef Py_UCS4 codepoint
    length = PyUnicode_GET_LENGTH(input_str)

    if input_str.isascii():
        return input_str, set()

    for i in range(length):
        codepoint = PyUnicode_READ_CHAR(input_str, i)
        if codepoint > 127:
            ch = chr(<int>codepoint)
            PySet_Add(non_ascii_chars, ch)
            if ch in replacements:
                output.append(replacements[ch])
            else:
                output.append('?')
        else:
            output.append(chr(codepoint))

    return "".join(output), non_ascii_chars