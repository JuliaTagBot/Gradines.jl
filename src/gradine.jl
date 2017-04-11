
struct Gradine <: AbstractGradine
    grp::GrdGroup
    columns::Vector{AbstractGradineColumn}

    names::Vector{Symbol}

    function Gradine(grp::GrdGroup, cols::Vector, names::Vector{Symbol})
        !JLD.exists(grp, "columns") && JLD.g_create(grp, "columns")
        new(grp, cols, names)
    end
end
export Gradine


#=========================================================================================
    <interface>
=========================================================================================#
ncol(g::Gradine) = length(g.columns)
nrow(g::Gradine) = (ncol(g) < 1) ? 0 : length(g.columns[1])
Base.size(g::Gradine) = (nrow(g), ncol(g))
Base.size(g::Gradine, idx::Integer) = size(g)[idx]

eltypes(g::Gradine) = DataType[eltype(c) for c ∈ g.columns]
export eltypes

Base.names(g::Gradine) = g.names

columns_group(g::Gradine) = g.grp["columns"]

Base.copy(g::Gradine) = Gradine(copy(g.grp), copy(g.columns), copy(g.names))

Base.getindex(g::Gradine, i::Integer) = g.columns[i]
#=========================================================================================
    <interface>
=========================================================================================#


#=========================================================================================
    <DataStreams Source Interface>
=========================================================================================#
function Data.isdone(g::Gradine, row::Integer, col::Integer)
    m,n = size(g)
    (1 ≤ col ≤ n) && (1 ≤ row ≤ m)
end

function Data.schema(g::Gradine, ::Type{Data.Column})
    Data.Schema(string.(names(g)), typeof.(g.columns), size(g,1))
end
function Data.schema(g::Gradine, ::Type{Data.Field})
    Data.Schema(string.(names(g)), eltypes(g), size(g,1))
end
Data.schema(g::Gradine) = Data.schema(g, Data.Field)

# # Columns
Data.streamtype(::Gradine, ::Type{Data.Column}) = true

function Data.streamfrom{T}(g::Gradine, ::Type{Data.Column}, ::Type{T}, col::Integer)
    convert(Vector{T}, g.columns[col][:])
end
function Data.streamfrom{T}(g::Gradine, ::Type{Data.Column}, ::Type{Nullable{T}},
                            col::Integer)
    convert(Vector{T}, g.columns[col][:])
end


# # Fields
Data.streamtype(::Gradine, ::Type{Data.Field}) = true

function Data.streamfrom{T}(g::Gradine, ::Type{Data.Field}, ::Type{T}, row, col::Integer)::T
    convert(T, g.columns[col][row])
end
function Data.streamfrom{T}(g::Gradine, ::Type{Data.Field}, ::Type{Nullable{T}},
                            row, col::Integer)::Nullable{T}
    convert(Nullable{T}, g.columns[col][row])
end
#=========================================================================================
    </DataStreams Source Interface>
=========================================================================================#



#=========================================================================================
    <DataStreams Sink Interface>
=========================================================================================#
Data.streamtypes(::Gradine) = [Data.Field, Data.Column]

# # Columns
# note that when calling this, columns would already have to be constructed
function Data.streamto!{T}(g::Gradine, ::Type{Data.Column}, column::T, row::Integer,
                           col::Integer)
    g.columns[col][:] = column
end
function Data.streamto!{T}(g::Gradine, ::Type{Data.Column}, column::T, row::Integer,
                           col::Integer, sch::Data.Schema)
    Data.streamto!(g, Data.Column, column, row, col)
end

# # Fields
function Data.streamto!{T}(g::Gradine, ::Type{Data.Field}, val::T, row, col)
    g.columns[col][row] = val
end
function Data.streamto!{T}(g::Gradine, ::Type{Data.Field}, val::T, row, col,
                           sch::Data.Schema)
    Data.streamto!(g, Data.Field, val, row, col)
end
#=========================================================================================
    </DataStreams Sink Interface>
=========================================================================================#



#=========================================================================================
    <constructors>
=========================================================================================#
# initialize a new, blank Gradine.  one should ensure group is free
function Gradine{T<:Data.StreamType}(sch::Data.Schema, ::Type{T}=Data.Field,
                                     append::Bool=false, ref::Vector{UInt8}=UInt8[];
                                     group::Union{GrdGroup,Void}=nothing)
    if group isa Void
        error("Specify an HDF5 group when constructing a Gradine.")
    end
    m, n = size(sch)
    m = max(0, T <: Data.Column ? 0 : m) # don't pre-allocate for column streaming
    types = Data.types(sch)
    colnames = Symbol.(Data.header(sch))

    columns = Vector{AbstractGradineColumn}(n)

    !JLD.exists(group, "columns") && JLD.g_create(group, "columns")

    for i ∈ 1:n
        columns[i] = gradinecolumn!(group["columns"], colnames[i], types[i], m)
    end

    Gradine(group, columns, colnames)
end
Gradine(grp::GrdGroup, sch::Data.Schema) = Gradine(sch, group=grp)

# given an existing sink, make any necessary changes for streaming source
# with Data.Schema `sch` to it
function Gradine(g::Gradine, sch::Data.Schema, ::Type{Data.Column}, append::Bool,
                 ref::Vector{UInt8}=UInt8[])
    m, n = size(sch)

    if m ≠ size(g,1)
        error("New columns must be same length as old. Appending not supported.")
    end

    if append
        error("JLD package currently does not allow support of appending to columns.")
    end

    types = Data.types(sch)
    colnames = Symbol.(Data.header(sch))

    for i ∈ 1:n
        push!(g.columns, gradinecolumn!(g.grp, colnames[i], types[i], m))
    end

    g
end

# constructors for blank gradines
# note that provided types should be eltypes
function Gradine(grp::GrdGroup, colnames::Vector{Symbol}, coltypes::Vector{DataType},
                 nrows::Integer)
    sch = Data.Schema(string.(colnames), coltypes, nrows)
    Gradine(grp, sch)
end
function Gradine(grdfile::GrdFile, group_name::String, colnames::Vector{Symbol},
                 coltypes::Vector{DataType}, nrows::Integer)
    Gradine(grdfile[group_name], colnames, coltypes, nrows)
end
function Gradine(grdfile::GrdFile, colnames::Vector{Symbol}, coltypes::Vector{DataType},
                 nrows::Integer)
    Gradine(grdfile, "/", colnames, coltypes, nrows)
end

# create a blank gradine from a Dict
function Gradine(grp::GrdGroup, nrows::Integer; kwargs::DataType...)
    colnames = Symbol[kw[1] for kw ∈ kwargs]
    coltypes = Symbol[kw[2] for kw ∈ kwargs]
    Gradine(grp, colnames, coltypes, nrows)
end
function Gradine(grdfile::GrdFile, group_name::String, nrows::Integer; kwargs::DataType...)
    Gradine(grdfile[group_name], nrows; kwargs...)
end
function Gradine(grdfile::GrdFile, nrows::Integer; kwargs::DataType...)
    Gradine(grdfile, "/", nrows; kwargs...)
end


function _gradine_load_existing(grp::GrdGroup)
    if "columns" ∈ names(grp)
        colnames = names(grp["columns"])
        cols = AbstractGradineColumn[gradinecolumn(grp["columns"][n]) for n ∈ colnames]
        return Gradine(grp, cols, Symbol.(colnames))
    else
        return Gradine(grp, AbstractGradineColumn[], Symbol[])
    end
end


# create a Gradine from existing data
function Gradine(grp::GrdGroup; kwargs::AbstractVector...)
    if length(kwargs) == 0
        return _gradine_load_existing(grp)
    end

    colnames = Symbol[kw[1] for kw ∈ kwargs]
    cols = Any[kw[2] for kw ∈ kwargs]
    coltypes = eltype.(cols)
    nrows = length(cols[1]) # WARNING we don't check all lengths
    sch = Data.Schema(string.(colnames), coltypes, nrows)
    g = Gradine(grp, sch)

    for (i, col) ∈ enumerate(cols)
        Data.streamto!(g, Data.Column, col, 0, i)
    end

    g
end
function Gradine(grdfile::GrdFile, group_name::String; kwargs::AbstractVector...)
    Gradine(grdfile[group_name]; kwargs...)
end
function Gradine(grdfile::GrdFile; kwargs::AbstractVector...)
    Gradine(grdfile, "/"; kwargs...)
end

# create a Gradine from an existing DataTable
function Gradine(grp::GrdGroup, data::DataTables.DataTable)
    cols = names(data)
    Gradine(grp, [data[col] for col ∈ cols])
end

function Gradine(filename::String, group_name::String; mode::String="r")
    f = jldopen(filename, mode)
    Gradine(f[group_name])
end
Gradine(filename::String; mode::String="r") = Gradine(filename, "/", mode=mode)
#=========================================================================================
    </constructors>
=========================================================================================#
