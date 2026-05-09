# HTTP transport layer — stateless RPC calls

function _rpc_call_http(client::SurrealClient{<:RemoteHTTPConnection}, method::String, params::Vector{Any};
                      session=nothing, txn=nothing)
    conn = client.connection
    lock(conn.lock)
    conn.request_id += 1
    rid = conn.request_id
    url = conn.http_base_url * "/rpc"

    # For HTTP, auto-prepend USE NS/DB since it is a stateless protocol
    ns = client.namespace
    db = client.database
    ns_db_prefix = (ns !== nothing && db !== nothing) ? "USE NS $ns DB $db;\n" : ""

    # Auto-convert CRUD methods to SurrealQL for HTTP (so USE NS/DB applies)
    effective_method, effective_params = _http_adapt_method(method, params, ns_db_prefix)

    msg = Dict("id" => rid, "method" => effective_method, "params" => effective_params)
    if session !== nothing
        msg["session"] = session isa UUIDs.UUID ? string(session) : string(session)
    end
    if txn !== nothing
        msg["txn"] = txn isa UUIDs.UUID ? string(txn) : string(txn)
    end
    headers = ["Content-Type" => "application/json", "Accept" => "application/json"]
    tok = client.token
    if tok !== nothing
        push!(headers, "Authorization" => "Bearer $tok")
    end
    unlock(conn.lock)

    @debug "SurrealDB http RPC →" rid=rid method=effective_method
    resp = nothing  # JET noticed it could be undefined if HTTP.post throws
    try
        resp = HTTP.post(url, headers, JSON.json(msg))
        if resp.status != 200
            throw(ConnectionError("HTTP $(resp.status): $(String(resp.body))"))
        end
        response = JSON.parse(String(resp.body))
        @debug "SurrealDB http RPC ←" rid=rid status=resp.status has_error=(response isa AbstractDict && haskey(response, "error"))
        if response isa AbstractDict && haskey(response, "error")
            err = response["error"]
            if err isa AbstractDict
                throw(_parse_rpc_error(err))
            else
                throw(RPCError(-1, string(err)))
            end
        end
        return get(response, "result", nothing)
    catch e
        if e isa SurrealDBError
            rethrow()
        end
        throw(ConnectionError("HTTP request failed: $e", e))
    end
end

function _http_adapt_method(method::String, params::Vector{Any}, prefix::String)
    isempty(prefix) && return method, params

    if method == "query"
        sql = params[1]
        vars = length(params) > 1 ? params[2] : Dict{String, Any}()
        return "query", Any[prefix * sql, vars]
    elseif method == "select"
        what = _to_string(params[1])
        return "query", Any[prefix * "SELECT * FROM $what", Dict{String, Any}()]
    elseif method == "create"
        what = _to_string(params[1])
        data = length(params) > 1 ? params[2] : Dict{String, Any}()
        data_json = JSON.json(data)
        return "query", Any[prefix * "CREATE $what CONTENT \$data", Dict("data" => data)]
    elseif method == "update"
        what = _to_string(params[1])
        data = length(params) > 1 ? params[2] : Dict{String, Any}()
        return "query", Any[prefix * "UPDATE $what MERGE \$data", Dict("data" => data)]
    elseif method == "delete"
        what = _to_string(params[1])
        return "query", Any[prefix * "DELETE FROM $what", Dict{String, Any}()]
    elseif method == "insert"
        table = _to_string(params[1])
        data = length(params) > 1 ? params[2] : Dict{String, Any}()
        return "query", Any[prefix * "INSERT INTO $table \$data", Dict("data" => data)]
    elseif method == "upsert"
        what = _to_string(params[1])
        data = length(params) > 1 ? params[2] : Dict{String, Any}()
        return "query", Any[prefix * "UPSERT $what CONTENT \$data", Dict("data" => data)]
    elseif method == "merge"
        what = _to_string(params[1])
        data = length(params) > 1 ? params[2] : Dict{String, Any}()
        return "query", Any[prefix * "UPDATE $what MERGE \$data", Dict("data" => data)]
    elseif method == "relate"
        rel_in = _to_string(params[1])
        relation = _to_string(params[2])
        rel_out = _to_string(params[3])
        data = length(params) > 3 ? params[4] : nothing
        data_json = data !== nothing ? " CONTENT \$data" : ""
        extra_vars = data !== nothing ? Dict("data" => data) : Dict{String, Any}()
        return "query", Any[prefix * "RELATE $rel_in->$relation->$rel_out$data_json", extra_vars]
    elseif method == "insert_relation"
        relation = _to_string(params[1])
        payload = length(params) > 1 ? params[2] : Dict{String, Any}()
        return "query", Any[prefix * "INSERT INTO $relation \$data", Dict("data" => payload)]
    elseif method == "live"
        throw(UnsupportedFeatureError(:live, :http))
    else
        # Non-data methods (signin, use, info, version, etc.) pass through unchanged
        return method, params
    end
end
