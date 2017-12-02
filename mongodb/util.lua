
local hasffi, ffi     = pcall(require, "ffi")
local hasposix, posix = pcall(require, "posix")
local ngx             = ngx
local assert          = assert
local unpack          = unpack
local floor           = math.floor
local strbyte         = string.byte
local strchar         = string.char
local strsub          = string.sub
local t_insert        = table.insert
local t_concat        = table.concat
local tonumber        = tonumber
local strdump         = string.dump
local strformat       = string.format
local loadstring      = loadstring
local io_popen        = io.popen

local util = {}

-- little-endian functions

local le_uint_to_num = function( s , i , j )
    i , j = i or 1 , j or #s
    local b = { strbyte( s , i , j ) }
    local n = 0
    for i=#b , 1 , -1 do
        n = n*2^8 + b[ i ]
    end
    return n
end
local le_int_to_num = function( s , i , j )
    i , j = i or 1 , j or #s
    local n = le_uint_to_num( s , i , j )
    local overflow = 2^(8*(j-i) + 7)
    if n > 2^overflow then
        n = - ( n % 2^overflow )
    end
    return n
end

local num_to_le_uint = function( n , bytes )
    bytes = bytes or 4
    local b = { }
    for i=1 , bytes do
        b[ i ] , n = n % 2^8 , floor(n / 2^8)
    end
    assert( n == 0 )
    return strchar( unpack(b) )
end

local num_to_le_int = function( n , bytes )
    bytes = bytes or 4
    if n < 0 then -- Converted to unsigned.
        n = 2^(8*bytes) + n
    end
    return num_to_le_uint(n , bytes)
end

-- Look at ith bit in given string (indexed from 0)
-- Returns boolean
local le_bpeek = function( s , bitnum )
    local byte = floor( bitnum / 8 ) + 1
    local bit = bitnum % 8
    local char = strbyte( s , byte )
    return floor( ( char % 2^(bit+1) ) / 2^bit ) == 1
end

-- big-edian unpack function

local be_uint_to_num = function( s , i , j )
    i , j = i or 1 , j or #s
    local b = { strbyte ( s , i , j ) }
    local n = 0
    for i=1 , #b do
        n = n*2^8 + b [ i ]
    end
    return n
end
local num_to_be_uint = function( n , bytes )
    bytes = bytes or 4
    local b = { }
    for i=bytes , 1 , -1 do
        b [ i ] , n = n % 2^8 , floor( n / 2^8 )
    end
    assert ( n == 0 )
    return strchar( unpack( b ) )
end

-- Returns (as a number); bits i to j (indexed from 0)
local extract_bits = function( s , i , j )
    j = j or i
    local i_byte = floor( i / 8 ) + 1
    local j_byte = floor( j / 8 ) + 1

    local n = be_uint_to_num( s , i_byte , j_byte )
    n = n % 2^( j_byte*8 - i )
    n = floor( n / 2^( (-(j+1) ) % 8 ) )
    return n
end


local be_bpeek = function( s , bitnum )
    local byte = floor( bitnum / 8 ) + 1
    local bit = 7-bitnum % 8
    local char = strbyte ( s , byte )
    return floor( ( char % 2^(bit+1) ) / 2^bit ) == 1
end

local to_double , from_double
do
    local s , e , d
    if hasffi then
        d = ffi.new ( "double[1]" )
    else
        d = strdump ( loadstring ( [[return 523123.123145345]] ) )
        s , e = d:find ( "\3\54\208\25\126\204\237\31\65" )
        s = d:sub ( 1 , s )
        e = d:sub ( e+1 , -1 )
    end
    function to_double( n )
        if hasffi then
            d [ 0 ] = n
            return ffi.string ( d , 8 )
        else
            local str = strdump ( loadstring ( [[return ]] .. n ) )
            local loc , en , mat = str:find ( "\3(........)" , str:find ( "\3" ) + 1 )
            return mat
        end
    end
    function from_double( str )
        assert ( #str == 8 )
        if hasffi then
            ffi.copy ( d , str , 8 )
            return d [ 0 ]
        else
            str = s .. str .. e
            return loadstring ( str ) ( )
        end
    end
end

local function new_str_buffer(s,i)
    i = i or 1
    return function( n )
        if not n then -- Rest of string
            n = #s - i + 1
        end
        i = i + n
        assert ( i-1 <= #s , "Unable to read enough characters" )
        return strsub( s , i-n , i-1 )
    end , function ( new_i )
        if new_i then i = new_i end
        return i
    end
end

local function string_to_array_of_chars(s)
    local t = { }
    for i = 1 , #s do
        t[ i ] = strsub(s , i , i)
    end
    return t
end

-- read from string buffer until got the terminators
local function read_terminated_string(strbuf , terminators)
    local terminators = string_to_array_of_chars( terminators or "\0" )
    local str = { }
    local found = 0
    while found < #terminators do
        local c = strbuf(1)
        if c == terminators[ found + 1 ] then
            found = found + 1
        else
            found = 0
        end
        t_insert( str , c )
    end
    return t_concat( str , "" , 1 , #str - #terminators )
end

local function slice_le_uint(buf,num)
    local t = {}
    local i = 1
    for j=1,num do
        t[j] = le_uint_to_num(buf,i,i+3)
        i = i+4
    end
    return unpack(t)
end

local function extract_flag_bits(flag, bits)
    local t = {}
    for i=1, bits do
        t[i] = le_bpeek(flag,i-1)
    end
    return unpack(t)
end

local function machineid()
    if hasposix then
        return posix.uname ( "%n" )
    else
        return assert ( io_popen ( "uname -n" ) ):read ( "*l" )
    end
end

local function getpid()
    if ngx then
        return ngx.worker.pid()
    end
    if hasposix then
        return posix.getpid().pid
    else
        return assert( tonumber( assert( io_popen ( "ps -o ppid= -p $$") ):read ( "*a" ) ) )
    end
end

local function split(s,sep)
        local sep, fields = sep or ":", {}
        local pattern = strformat("([^%s]+)", sep)
        s:gsub(pattern, function(c) fields[#fields+1] = c end)
        return fields
end


return {
    le_uint_to_num         = le_uint_to_num,
    le_int_to_num          = le_int_to_num,
    num_to_le_uint         = num_to_le_uint,
    num_to_le_int          = num_to_le_int,
    slice_le_uint          = slice_le_uint,
    
    be_uint_to_num         = be_uint_to_num,
    num_to_be_uint         = num_to_be_uint,
    
    extract_bits           = extract_bits,
    extract_flag_bits      = extract_flag_bits,
    
    le_bpeek               = le_bpeek,
    be_bpeek               = be_bpeek,
    
    to_double              = to_double,
    from_double            = from_double,
    new_str_buffer         = new_str_buffer,
    read_terminated_string = read_terminated_string,
    
    split                  = split,
    
    machineid              = machineid,
    getpid                 = getpid,
    time                   = ngx.time,
    md5                    = ngx.md5,
    socket                 = ngx.socket,
}