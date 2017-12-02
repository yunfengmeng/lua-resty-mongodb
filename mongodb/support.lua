
local getmetatable = getmetatable
local pairs        = pairs


local _M = {}

function _M.iterator(t)
    local mt = getmetatable(t)
    if mt then
        local f = mt.__pairs
        if f then
            return f(t)
        end
    end
    return pairs(t)
end


return _M

