// Package taskstate defines versioned, canonical task-definition and adaptive
// state payloads. Approved definitions and adaptive state deliberately use
// separate DTOs so an automatic learning write cannot acquire a field that
// changes a user's approved intent or side-effect boundary.
package taskstate
