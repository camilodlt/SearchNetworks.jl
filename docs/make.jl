using SearchNetworks
using Documenter

DocMeta.setdocmeta!(SearchNetworks, :DocTestSetup, :(using SearchNetworks); recursive=true)

makedocs(;
    modules=[SearchNetworks],
    authors="Camilo De La Torre <camilo.de-la-torre@ut-capitole.fr> and contributors",
    repo="https://github.com/camilo/SearchNetworks.jl/blob/{commit}{path}#{line}",
    sitename="SearchNetworks.jl",
    format=Documenter.HTML(;
        prettyurls=get(ENV, "CI", "false") == "true",
        canonical="https://camilo.github.io/SearchNetworks.jl",
        edit_link="main",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/camilo/SearchNetworks.jl",
    devbranch="main",
)
