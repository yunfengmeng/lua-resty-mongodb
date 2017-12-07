-- https://docs.mongodb.org/manual/reference/mongodb-wire-protocol/

local bson                             = require "resty.mongodb.bson"
local object_id                        = require "resty.mongodb.objectid"
local util                             = require "resty.mongodb.util"
local t_ordered                        = require "resty.mongodb.orderedtable"
local to_bson,from_bson,from_bson_buf  = bson.to_bson,bson.from_bson,bson.from_bson_buf
local t_concat,t_insert                = table.concat, table.insert
local num_to_le_uint,num_to_le_int     = util.num_to_le_uint,util.num_to_le_int
local new_str_buffer                   = util.new_str_buffer
local le_bpeek                         = util.le_bpeek
local slice_le_uint, extract_flag_bits = util.slice_le_uint,util.extract_flag_bits
local assert                           = assert
local ipairs                           = ipairs

local ns = {
    SYSTEM_NAMESPACE_COLLECTION = "system.namespaces",
    SYSTEM_INDEX_COLLECTION     = "system.indexes",
    SYSTEM_PROFILE_COLLECTION   = "system.profile",
    SYSTEM_USER_COLLECTION      = "system.users",
    SYSTEM_JS_COLLECTION        = "system.js",
    SYSTEM_COMMAND_COLLECTION   = "$cmd",
}


-- opcodes

local op_codes = {
    OP_REPLY        = 1,    -- Reply to a client request. responseTo is set.
    OP_MSG          = 1000, -- Generic msg command followed by a string.
    OP_UPDATE       = 2001, -- Update document.
    OP_INSERT       = 2002, -- Insert new document.
    RESERVED        = 2003, -- Formerly used for OP_GET_BY_OID.
    OP_QUERY        = 2004, -- Query a collection.
    OP_GETMORE      = 2005, -- Get more data from a query. See Cursors.
    OP_DELETE       = 2006, -- Delete documents.
    OP_KILL_CURSORS = 2007, -- Notify database that the client has finished with the cursor.
}

-- message header size

local STANDARD_HEADER_SIZE = 16
local RESPONSE_HEADER_SIZE = 20

-- place holder

local ZERO32 = "\0\0\0\0"
local ZEROID = "\0\0\0\0\0\0\0\0"

-- flag bit constant

local flags = {
    update = {
        Upsert      = 1,
        MultiUpdate = 2,
        -- 2-31 reserved
    },
    insert = {
        ContinueOnError = 1,
    },
    -- used in query message
    query = {
        TailableCursor  = 2,
        SlaveOk         = 4,
        OplogReplay     = 8,
        NoCursorTimeout = 16,
        AwaitData       = 32,
        Exhaust         = 64,
        Partial         = 128,
    },
    -- used in delete message
    delete           = {
        SingleRemove = 1,
    },
    -- used in reponse message
    reply            = {
        REPLY_CURSOR_NOT_FOUND   = 1,
        REPLY_QUERY_FAILURE      = 2,
        REPLY_SHARD_CONFIG_STALE = 4,
        REPLY_AWAIT_CAPABLE      = 8,
        -- Reserved 4-31
    },
}

local ERR = {
    CURSOR_NOT_FOUND = 1,
    QUERY_FAILURE = 2,
}

local  current_request_id = 0;

local function with_header(opcode, message, response_to)
    current_request_id = current_request_id+1
    local request_id = num_to_le_uint(current_request_id)
    response_to = response_to or ZERO32
    opcode = num_to_le_uint(assert(op_codes[opcode]))
    return current_request_id, num_to_le_uint (#message + STANDARD_HEADER_SIZE)
        .. request_id .. response_to .. opcode .. message
end

local function query_message(full_collection_name, query, fields, numberToReturn, numberToSkip, options)
    numberToSkip = numberToSkip or 0
    local flag = 0
    if options then
        flag = (options.tailable and flags.query.TailableCursor or 0)
            + (options.slave_ok and flags.query.SlaveOk or 0 )
            + (options.oplog_replay and flags.query.OplogReplay or 0)
            + (options.immortal and flags.query.NoCursorTimeout or 0)
            + (options.await_data and flags.query.AwaitData or 0)
            + (options.exhaust and flags.query.Exhaust or 0)
            + (options.partial and flags.query.Partial or 0)
    end
    query = to_bson(query)
    if fields then
        fields = to_bson(fields)
    else
        fields = ""
    end
    return with_header("OP_QUERY",
        num_to_le_uint(flag) .. full_collection_name .. num_to_le_uint(numberToSkip) .. num_to_le_int(numberToReturn)
         .. query .. fields
        )
end

local function get_more_message(full_collection_name, cursor_id, numberToReturn)
    return with_header("OP_GETMORE", ZERO32 .. full_collection_name .. num_to_le_int(numberToReturn or 0) .. cursor_id )
end

local function delete_message(full_collection_name, selector, singleremove)
    local flags = (singleremove and flags.delete.SingleRemove or 0)
    selector = to_bson(selector)
    return with_header('OP_DELETE', ZERO32 .. full_collection_name .. num_to_le_uint(flags) .. selector)
end

local function update_message(full_collection_name, selector, update, upsert, multiupdate)
    local flags = (upsert and flags.update.Upsert or 0) + ( multiupdate and flags.update.MultiUpdate or 0)
    selector = to_bson(selector)
    update = to_bson(update)
    return with_header('OP_UPDATE',ZERO32 .. full_collection_name .. num_to_le_uint(flags) .. selector .. update)
end

local function insert_message(full_collection_name, docs, continue_on_error, no_ids)
    local flags = ( continue_on_error and flags.insert.ContinueOnError or 0 )
    local r = {}
    for i,v in ipairs(docs) do
        local _id = v._id
        if not _id and not no_ids then
            _id = object_id.new()
            v._id = _id
        end
        r[i] = to_bson(v)
    end
    return with_header("OP_INSERT", num_to_le_uint(flags) .. full_collection_name .. t_concat(r))
end

local function kill_cursors_message(cursor_ids)
    local n = #cursor_ids
    cursor_ids = t_concat(cursor_ids)
    return with_header('OP_KILL_CURSORS',ZERO32 .. num_to_le_uint(n) .. cursor_ids )
end

local function recv_message(sock, request_id)
    -- msg header
    local header = assert(sock:receive(STANDARD_HEADER_SIZE))
    local msg_length,req_id,response_to,opcode = slice_le_uint(header,4)
    assert(request_id == response_to, "response_to:".. response_to .. " should:" .. request_id)
    assert(opcode == op_codes.OP_REPLY,"invalid response opcode")
    -- read message data
    local msg_data = assert(sock:receive(msg_length-STANDARD_HEADER_SIZE))
    local msg_buf = new_str_buffer(msg_data)
    -- response header,20 bytes
    local response_flags,cursor_id = msg_buf(4), msg_buf(8)
    local starting_from,number_returned = slice_le_uint(msg_buf(8),2)
    local err = {}
    -- parse reponse flags
    local cursor_not_found,query_failure,shard_config_stale,await_capable = extract_flag_bits(response_flags,4)

    if cursor_not_found then
        err.CURSOR_NOT_FOUND = true
    end
    if query_failure then
        err.QUERY_FAILURE = true
    end
    -- client should ignore this flag
    -- assert(not shard_config_stale,'shard confi is stale')
    local docs = {}
    -- documents
    if not cursor_not_found then
        for i=1,number_returned do
            docs[i] = from_bson_buf(msg_buf)
        end
    end
    return cursor_id,number_returned,err,docs
end

local function db_ns(db,name )
    return db .. "." .. name .."\0"
end

local function send_message( sock, message )
    return sock:send(message)
end

local function send_message_with_safe(sock, message, dbname, opts)
    local cmd = t_ordered({"getlasterror",true, "w",opts.w,"wtimeout",opts.wtimeout})
    if opts.fsync then cmd.fsync = true end
    if opts.j then cmd.j =  true end
    local req_id,last_error_msg = query_message(db_ns(dbname,ns.SYSTEM_COMMAND_COLLECTION),cmd,nil,-1,0)
    sock:send(message .. last_error_msg)
    local _, number,err, docs = recv_message(sock,req_id)
    if number == 1 and ( docs[1]['err'] or docs[1]['errmsg'] ) then
        return false, docs[1]
    end
    return docs[1]
end


return {
    OPCODES                = op_codes,
    NS                     = ns,
    FLAGS                  = flags,
    ZERO32                 = ZERO32,
    ZEROID                 = ZEROID,
    ERR                    = ERR,
    db_ns                  = db_ns,
    update_message         = update_message,
    get_more_message       = get_more_message,
    delete_message         = delete_message,
    query_message          = query_message,
    insert_message         = insert_message,
    kill_cursors_message   = kill_cursors_message,
    recv_message           = recv_message,
    send_message           = send_message,
    send_message_with_safe = send_message_with_safe,
}
