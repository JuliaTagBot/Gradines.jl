using DataTables
using Gradines
using Estuaries
using JLD

const nrows = 100
const filename = "testfile.jld"

function writefile(filename::String)
    isfile(filename) && rm(filename)
    f = jldopen(filename, "w")

    g = Gradine(f, A=rand(nrows), B=[randstring(5) for i ∈ 1:nrows],
                C=NullableArray(rand(Int64, nrows)))

    f, g
end

# f, g = writefile(filename)

g = Gradine(filename, mode="r+")
E = Estuary(g)

