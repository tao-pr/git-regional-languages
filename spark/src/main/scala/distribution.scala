package gitdist

/**
 * Representation of an exact geospatial location
 */
case class GeoLocation(lat: Double, lng: Double)

/**
 * Representation of a geospatial location with the amount
 * of code committed at the underlying location.
 */
case class CodeDistribution(codeAmount: Long, centroid: GeoLocation)

/**
 * Representation of a language code distribution over 
 * geospatial regions
 */
case class LanguageCodeDistributions(language: String, dists: List[CodeDistribution] )
