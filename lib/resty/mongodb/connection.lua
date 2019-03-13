local util = require "resty.mongodb.util"
local bson = require "resty.mongodb.bson"
local database = require "resty.mongodb.database"
local tcp = util.socket.tcp
local split = util.split
local substr = string.sub
local assert = assert
local error = error
local setmetatable = setmetatable

local connection = {}
local connection_mt = { __index = connection }

connection.host = "127.0.0.1"
connection.port = 27017
connection.w = 1
connection.wtimeout = 1000
connection.user_name = nil
connection.password = nil
connection.db_name = 'admin'
connection.query_timeout = 1000
connection.max_bson_size = 4 * 1024 * 1024
connection.find_master = false;
connection.sock = nil

connection.hosts = {}
connection.arbiters = {}
connection.passives = {}

local string_sub = string.sub

function connection:connect(host, port)
    self.host = host or self.host
    self.port = port or self.port
    local sock = self.sock

    if string_sub(host, 1, 5) == "unix:" then
        return sock:connect(self.host)
    end
    return sock:connect(self.host, self.port)
end

function connection:set_timeout(timeout)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    return sock:settimeout(timeout)
end

function connection:set_keepalive(...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    return sock:setkeepalive(...)
end

function connection:get_reused_times()
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:getreusedtimes()
end

function connection:close()
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    return sock:close()
end

function connection:database_names()
    local r = self:get_database("admin"):run_command({ listDatabases = true })
    if r.ok == 1 then
        return r.databases
    end
    error("failed to get database names:" .. r.errmsg)
end

--[[ todo

function connection:get_master()
end
--]]


function connection:get_database(name)
    return database.new(name, self)
end

--[[ todo
function connection:auth(dbname,user,password,is_digest)

end
--]]

function connection:get_max_bson_size()
    local buildinfo = self:get_database("admin"):run_command({ buildinfo = true })
    if buildinfo then
        return buildinfo.maxBsonObjectSize or 4194304
    end
    return 4194304
end


function connection:new()
    local sock = tcp()
    return setmetatable({ sock = sock }, connection_mt)
end


return connection

