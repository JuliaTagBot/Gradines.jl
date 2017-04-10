
abstract type AbstractGradine end
abstract type AbstractGradineColumn{T} <: AbstractVector{T} end

# aliases
const GrdFile = JLD.JldFile
const GrdGroup = JLD.JldGroup
const GrdDataset = JLD.JldDataset

