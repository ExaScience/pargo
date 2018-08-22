// Package pargo provides functions and data structures for expressing parallel
// algorithms. While Go is primarily designed for concurrent programming, it is
// also usable to some extent for parallel programming, and this library
// provides convenience functionality to turn otherwise sequential algorithms
// into parallel algorithms, with the goal to improve performance.
//
// For documentation that provides a more structured overview than is possible
// with Godoc, see the wiki at https://github.com/exascience/pargo/wiki
//
// Pargo provides the following subpackages:
//
// pargo/parallel provides simple functions for executing series of thunks or
// predicates, as well as thunks, predicates, or reducers over ranges in
// parallel. See also https://github.com/ExaScience/pargo/wiki/TaskParallelism
//
// pargo/speculative provides speculative implementations of most of the
// functions from pargo/parallel. These implementations not only execute in
// parallel, but also attempt to terminate early as soon as the final result is
// known. See also https://github.com/ExaScience/pargo/wiki/TaskParallelism
//
// pargo/sequential provides sequential implementations of all functions from
// pargo/parallel, for testing and debugging purposes.
//
// pargo/sort provides parallel sorting algorithms.
//
// pargo/sync provides an efficient parallel map implementation.
//
// pargo/pipeline provides functions and data structures to construct and
// execute parallel pipelines.
//
// Pargo has been influenced to various extents by ideas from Cilk, Threading
// Building Blocks, and Java's java.util.concurrent and java.util.stream
// packages. See http://supertech.csail.mit.edu/papers/steal.pdf for some
// theoretical background, and the sample chapter at
// https://mitpress.mit.edu/books/introduction-algorithms for a more practical
// overview of the underlying concepts.
package pargo
