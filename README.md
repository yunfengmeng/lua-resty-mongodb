# Name

lua-resty-sphinx - lua mongodb client driver implement for the ngx_lua based on the cosocket API
# Status

This library is considered production ready.
# Description

This Lua library is a mongodb client driver implement for the ngx_lua nginx module:
# Synopsis

```
local conf = {
    host          = "127.0.0.1",
    port          = 27017,
    timeout       = 3000,
    idle_timeout  = 1000 * 60,
    max_keepalive = 10,
}
local cjson = require "cjson"
local mongo = require "resty.mongo"
local mong = mongo:connect(conf)

local db = mong:get_database("d")
local coll = db:get_collection("c")
local cursor = coll:find({})

for i, item in cursor:next() do
    for k,v in pairs(item) do
        ngx.say(cjson.encode(v))
    end
end

```
# Requires


# TODO

# See Also

