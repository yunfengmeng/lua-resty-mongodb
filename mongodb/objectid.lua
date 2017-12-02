
local util           = require "resty.mongodb.util"
local setmetatable   = setmetatable
local strbyte        = string.byte
local strchar        = string.char
local tonumber       = tonumber
local strformat      = string.format
local t_insert       = table.insert
local t_concat       = table.concat
local assert         = assert
local md5_bin        = ngx.md5_bin
local re_match       = ngx.re.match
local num_to_le_uint = util.num_to_le_uint
local num_to_be_uint = util.num_to_be_uint


local function str2hex(str)
    local t = { }
    for i = 1 , 12 do
        t_insert( t , strformat ( "%02x" , strbyte( str , i , i ) ) )
    end
    return t_concat ( t )
end


-- hexadecimal string convert to 12-bytes
local function hex2str(hex)
    local m, err = re_match(hex, "^[0-9A-F]{24}$",'ij')
    if m then
        local str = ""
        for i = 1, #hex, 2 do
            str = str .. strchar(tonumber(hex:sub(i, i + 1), 16))
        end
        hex = str
    end
    return hex
end


local oid_mt = {
    __tostring = function( ob )
        return "ObjectId(" .. str2hex(ob.id) .. ")"
    end,
    
    __eq = function( a , b ) return a.id == b.id end,
    
    __index = {    
        valueof = function( self ) -- return objectid value
            return str2hex(self.id)
        end,
    },
}

local machineid = md5_bin( util.machineid() ):sub(1,3)

local pid = util.getpid() % 0xffff
pid = num_to_le_uint(pid,2)

local inc = 0

local function generate_id()
    inc = inc + 1
    return num_to_be_uint( util.time(), 4 ) .. machineid .. pid .. num_to_be_uint( inc , 3 )
end

local function new_object_id( str )
    if str then
        str = hex2str(str)
        assert( #str == 12 )
    else
        str = generate_id()
    end
    return setmetatable( { id = str } , oid_mt )
end

return {
    new       = new_object_id,
    metatable = oid_mt,
}
