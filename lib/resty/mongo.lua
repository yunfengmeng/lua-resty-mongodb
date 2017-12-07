-- Copyright (C) 2015 Yunfeng Meng

local mongodb      = require "resty.mongodb"
local setmetatable = setmetatable
local get_instance = get_instance


local _M = { _VERSION = '0.01' }
local mt = { __index = _M }


function _M.connect(self, config)
    local mongo = mongodb:new()

    mongo:set_timeout(config.timeout)

    local ok, err = mongo:connect(config.host, config.port)
    if not ok then
        get_instance().debug:log_error("failed to connect mongodb: ", err)
        return
    end

    return setmetatable({ conn = mongo, config = config }, mt)
end


function _M.close(self)
    local conn = self.conn
    local ok, err = conn:close()
    if not ok then
        get_instance().debug:log_error("failed to close mongodb: ", err)
        return
    end
end


function _M.is_closed(self)
    local conn = self.conn
    local count, err = conn:get_reused_times()
    if count == nil and err == "closed" then
        return true
    end
    return false
end


function _M.keepalive(self)
    local conn   = self.conn
    local config = self.config
    
    if config.idle_timeout >= 0 and config.max_keepalive > 0 then
        local ok, err = conn:set_keepalive(config.idle_timeout, config.max_keepalive)
        if not ok then
            get_instance().debug:log_error("failed to set mongodb keepalive: ", err)
            return
        end
    end
end


-- 元表
local class_mt = {
    __index = function (table, key)
        return function (self, ...)
            local conn = self.conn
            return conn[key](conn, ...)
        end
    end
}


setmetatable(_M, class_mt)


return _M

