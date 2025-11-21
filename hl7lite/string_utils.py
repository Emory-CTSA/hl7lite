#%%
import base64
import zlib
# import zstandard as zstd
import lz4.frame

from hl7lite.sanitize_unicode import c_sanitize_unicode



utf8_to_ascii_map = {
    '\u2018': "'",
    '\u2019': "'",
    '\u201a': "'",
    '\u2032': "'",
    '\u201c': '"',
    '\u201d': '"',
    '\u201e': '"',
    '\u2033': '"',
    '\u00ae': '(R)',
    '\u00a9': '(C)',
    '\u2122': '(TM)',
    '\u2026': '...',
    '\ufffd': '?',
    '\u2013': '-',
    '\u2014': '-',
    '\u2039': '<',
    '\u203a': '>',
    '\u02dc': '~',
    '\u02c6': '^',
}

def sanitize_nonascii_python(message: str) -> tuple[str, set[str] | None]:
    # replace smartquotes, also experiencing U+FFFD REPLACEMENT CHARACTER in messages, possibly from other invalid bytes.        
    non_ascii_chars = set(c for c in message if ord(c) > 127)
    if non_ascii_chars:

        # replace all non-ascii characters with closest ascii equivalent or ?
        for c in non_ascii_chars:
            newc = utf8_to_ascii_map.get(c, '?')  # if non-ascii char is not in map, replace with ?
            message = message.replace(c, newc)
                    
        # should not have any additional non-ascii characters.
        return message, non_ascii_chars 
    else:
        return message, None

def sanitize_nonascii(message: str, replacement_map: dict = utf8_to_ascii_map) -> tuple[str, set[str] | None]:
    return c_sanitize_unicode(message, replacement_map)


    
# compress string, and base64 encode and return as string
def compress_string(input_str: str, compression: str = 'zlib') -> str:
    # compression ratios vs time:  choose zlib level 1.   
    
    if compression.lower() == 'none':    
        # raw - 35 sec , 437mb
        return input_str
    elif compression.lower() == 'zlib':
        # zlib compressed + base64 encoded.  level 1: 35s, 116mb, level3: 37s, 116mb, level 4: 38s 92mb , level5: 38s, 86mb , level6: 41s 83MB, level 9: 45s 78MB
        compressed = zlib.compress(input_str.encode('utf-8'), level=1)
        encoded = base64.b64encode(compressed).decode('utf-8')
        return encoded
    # elif compression.lower() == 'zstd':
    #     # zstd compression + base64: -1 fastest:  34s, 138mb
    #     compressor = zstd.ZstdCompressor(level=-1)
    #     # # Compress the data
    #     compressed = compressor.compress(input_str.encode('utf-8'))
    #     encoded = base64.b64encode(compressed).decode('utf-8')
    #     return encoded
    elif compression.lower() == 'lz4':
        # lz4 compressed + base64 encoded: level 0: 33s, 177mb, level 16: 86s, 107mb. level 4 36.7s, 117mb . level 2: 35s 168mb.  level1 35s, 174mb, level3: 36s, 126mb
        compressed = lz4.frame.compress(input_str.encode('utf-8'), compression_level=0)
        encoded = base64.b64encode(compressed).decode('utf-8')  # Convert to string
        return encoded
    else:
        raise ValueError(f"Unsupported compression type: {compression}")
    
    

    
# compress string, and base64 encode and return as string
def decompress_string(input_str: str, compression: str = 'zlib') -> str:
    # compression ratios vs time:  choose zlib level 1.   
    
    if compression.lower() == 'none':    
        # raw - 35 sec , 437mb
        return input_str
    elif compression.lower() == 'zlib':
        # zlib compressed + base64 encoded.  level 1: 35s, 116mb, level3: 37s, 116mb, level 4: 38s 92mb , level5: 38s, 86mb , level6: 41s 83MB, level 9: 45s 78MB
        decoded = base64.b64decode(input_str.encode('utf-8'))
        decompressed = zlib.decompress(decoded)
        return decompressed.decode('utf-8')
    # elif compression.lower() == 'zstd':
    #     # zstd compression + base64: -1 fastest:  34s, 138mb
    #     compressor = zstd.ZstdCompressor(level=-1)
    #     # # Compress the data
    #     decoded = base64.b64decode(input_str.encode('utf-8'))
    #     decompressed = compressor.decompress(decoded)
    #     return decompressed.decode('utf-8')
    elif compression.lower() == 'lz4':
        # lz4 compressed + base64 encoded: level 0: 33s, 177mb, level 16: 86s, 107mb. level 4 36.7s, 117mb . level 2: 35s 168mb.  level1 35s, 174mb, level3: 36s, 126mb
        decoded = base64.b64decode(input_str.encode('utf-8'))
        decompressed = lz4.frame.decompress(decoded)
        return decompressed.decode('utf-8')
    else:
        raise ValueError(f"Unsupported compression type: {compression}")