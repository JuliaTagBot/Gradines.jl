__precompile__(true)

module Gradines

using JLD
using NullableArrays
using DataStreams

import HDF5

import Base: length, size
import Base.delete!
import Base.eltype
import Base.isnull

import Base: getindex, setindex!

include("abstracts.jl")
include("utils.jl")
include("gradinecolumn.jl")
include("nullablegradinecolumn.jl")
include("gradine.jl")

end # module
