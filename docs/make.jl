using Documenter
using SurrealDB

makedocs(;
    sitename = "SurrealDB.jl",
    modules = [SurrealDB],
    authors = "SurrealDB.jl contributors",
    pages = [
        "Home" => "index.md",
        "API Reference" => "api.md",
    ],
    format = Documenter.HTML(;
        prettyurls = get(ENV, "CI", "false") == "true",
        canonical = "https://danvinci.github.io/surrealdb/",
        edit_link = "main",
        assets = String[],
    ),
    warnonly = true,
)

deploydocs(;
    repo = "github.com/danvinci/surrealdb",
    devbranch = "main",
    push_preview = false,
)
