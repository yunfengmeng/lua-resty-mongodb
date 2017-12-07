
local collection   = require "resty.mongodb.collection"
local cursor       = require "resty.mongodb.cursor"
local t_ordered    = require "resty.mongodb.orderedtable"
local protocol     = require "resty.mongodb.protocol"
local NS           = protocol.NS
local setmetatable = setmetatable

local database = {}
local database_mt = { __index = database }

database.name = nil
database.conn = nil


function database:get_collection(name)
    return collection.new(name,self)
end

function database:drop()
    return self:run_command({ dropDatabase = true })
end

function database:drop_collection( coll )
    local ok =  self:run_command({ drop = coll })
    return ok.ok == 1 or ok.ok == true
end

function database:get_last_error(options)
    options = options or {}
    local w = options.w or self.conn.w
    local wtimeout = options.wtimeout or self.conn.wtimeout
    local cmd = t_ordered({"getlasterror",true, "w",w,"wtimeout",wtimeout})
    if options.fsync then cmd.fsync = true end
    if options.j then cmd.j = true end
    return self:run_command(cmd)
end

function database:run_command(cmd)
    local cursor = cursor.new(self, NS.SYSTEM_COMMAND_COLLECTION,cmd)
    local result = cursor:limit(-1):all()
    if not result[1] then
        return { ok = 0, errmsg = cursor.last_error_msg }
    end
    return result[1]
end

local function new(name, conn)
    local obj = { name = name, conn = conn }
    return setmetatable(obj, database_mt)
end

return {
    new = new,
}
