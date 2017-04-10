
struct GradineColumn{T} <: AbstractGradineColumn{T}
    values::GrdDataset

    GradineColumn{T}(values::GrdDataset) where T = new(values)
end

#=========================================================================================
    <interface>
=========================================================================================#
Base.length(gc::AbstractGradineColumn) = length(gc.values)
Base.size(gc::AbstractGradineColumn) = size(gc.values)
Base.eltype(gc::AbstractGradineColumn{T}) = T

# parent is assumed to be the same for all datasets for other types
parent(gc::AbstractGradineColumn) = parent(gc.values)

name(gc::AbstractGradineColumn) = Symbol(string(split(name(parent(gc)), '/')[end]))

Base.delete!(gc::GradineColumn) = delete!(gc.values)
#=========================================================================================
    </interface>
=========================================================================================#


#=========================================================================================
    <constructors>
=========================================================================================#
GradineColumn(values::GrdDataset) = GradineColumn{eltype(values)}(values)

function GradineColumn!{T}(grp::GrdGroup, name::Symbol, values::AbstractVector{T})
    path = path_values(grp, name)
    grp[path] = values
    GradineColumn{T}(grp[path])
end

function GradineColumn!{T}(grdfile::GrdFile, group_name::String, name::Symbol,
                           values::AbstractVector{T})
    GradineColumn!(grdfile[group_name], name, values)
end
#=========================================================================================
    </constructors>
=========================================================================================#


#=========================================================================================
    <getindex>
=========================================================================================#
Base.getindex(gc::GradineColumn, idx::Union{AbstractVector,Colon}) = gc.values[idx]
Base.getindex(gc::GradineColumn, idx) = gc.values[idx][1]

# HDF5 datasets don't support vector indices
function Base.getindex(gc::GradineColumn, idx::AbstractVector{<:Integer})
    [gc.values[i][1] for i ∈ idx]
end
function Base.getindex(gc::GradineColumn, idx::AbstractVector{Bool})
    getindex(gc, find(idx))
end
#=========================================================================================
    </getindex>
=========================================================================================#


#=========================================================================================
    <setindex>
=========================================================================================#
setindex!(gc::GradineColumn, v, idx) = setindex!(gc.values, v, idx)
#=========================================================================================
    </setindex>
=========================================================================================#


#=========================================================================================
    <general constructors>
    These are constructors which infer what type of AbstractGradineColumn to use.
=========================================================================================#
function gradinecolumn!(grp::GrdGroup, name::Symbol, v::AbstractVector)
    GradineColumn!(grp, name, v)
end
function gradinecolumn!(f::GrdFile, group_name::String, name::Symbol, v::AbstractVector)
    GradineColumn!(f, group_name, name, v)
end

function gradinecolumn!(grp::GrdGroup, name::Symbol, v::NullableVector)
    NullableGradineColumn!(grp, name, v)
end
function gradinecolumn!(f::GrdFile, group_name::String, name::Symbol, v::NullableVector)
    NullableGradineColumn!(f, group_name, name, v)
end

# for loading from file
function gradinecolumn(grp::GrdGroup)
    colname = group_name(grp)
    leaves = Set(names(grp))

    # TODO this is a quick and dirty way of determining eltype of columns.
    # There should be a way to get it properly from JLD metadata.
    # As far as I can tell, this should be ok except in cases where columns have mixed types.

    if Set(["values"]) == leaves
        dset = grp["values"]
        dtype = eltype(dset[1])
        return GradineColumn{dtype}(dset)
    elseif Set(["values", "isnull"]) == leaves
        dset = grp["values"]
        dset_isnull = grp["isnull"]
        dtype = eltype(dset[1])
        return NullableGradineColumn{dtype}(dset, dset_isnull)
    else
        throw(ArgumentError("Attempted to construct column from invalid HDF5 group."))
    end
end
gradinecolumn(f::GrdFile, group_name::String) = gradinecolumn(f[group_name])
#=========================================================================================
    </general constructors>
=========================================================================================#



