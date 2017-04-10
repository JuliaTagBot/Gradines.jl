
struct Gradine <: AbstractGradine
    grp::GrdGroup
    columns::Vector{AbstractGradineColumn}

    names::Vector{Symbol}

    function Gradine(grp::GrdGroup, cols::Vector, names::Vector{Symbol})
        !exists(grp, "columns") && g_create(grp, "columns")
        new(grp, cols, names)
    end
end
export Gradine


#=========================================================================================
    <interface>
=========================================================================================#
ncol(g::Gradine) = length(g.columns)
nrow(g::Gradine) = (ncol < 1) ? 0 : length(g.columns[1])
Base.size(g::Gradine) = (nrow(g), ncol(g))
Base.size(g::Gradine, idx::Integer) = size(g)[idx]

eltypes(g::Gradine) = eltype.(g.columns)
export eltypes

Base.names(g::Gradine) = g.names

columns_group(g::Gradine) = g.grp["columns"]

Base.copy(g::Gradine) = Gradine(copy(g.grp), copy(g.columns), copy(g.names))
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

function Data.streamfrom(g::Gradine, ::Type{Data.Field}, ::Type{T}, row, col::Integer)::T
    convert(T, g.columns[col][row])
end
function Data.streamfrom(g::Gradine, ::Type{Data.Field}, ::Type{Nullable{T}},
                         row, col::Integer)::Nullable{T}
    convert(Nullable{T}, g.columns[col][row])
end
#=========================================================================================
    </DataStreams Source Interface>
=========================================================================================#
