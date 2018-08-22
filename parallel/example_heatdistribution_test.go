package parallel_test

// This is a simplified version of a heat distribution simulation, based on an
// implementation by Wilfried Verachtert.
//
// See https://en.wikipedia.org/wiki/Heat_equation for some theoretical
// background.

import (
	"fmt"
	"math"

	"gonum.org/v1/gonum/mat"

	"github.com/exascience/pargo/parallel"
)

const ε = 0.001

func maxDiff(m1, m2 *mat.Dense) (result float64) {
	rows, cols := m1.Dims()
	result = parallel.RangeReduceFloat64(
		1, rows-1, 0,
		func(low, high int) (result float64) {
			for row := low; row < high; row++ {
				r1 := m1.RawRowView(row)
				r2 := m2.RawRowView(row)
				for col := 1; col < cols-1; col++ {
					result = math.Max(result, math.Abs(r1[col]-r2[col]))
				}
			}
			return
		},
		math.Max,
	)
	return
}

func HeatDistributionStep(u, v *mat.Dense) {
	rows, cols := u.Dims()
	parallel.Range(1, rows-1, 0,
		func(low, high int) {
			for row := low; row < high; row++ {
				uRow := u.RawRowView(row)
				vRow := v.RawRowView(row)
				vRowUp := v.RawRowView(row - 1)
				vRowDn := v.RawRowView(row + 1)
				for col := 1; col < cols-1; col++ {
					uRow[col] = (vRowUp[col] + vRowDn[col] + vRow[col-1] + vRow[col+1]) / 4.0
				}
			}
		},
	)
}

func HeatDistributionSimulation(M, N int, init, t, r, b, l float64) {
	// ensure a border
	M += 2
	N += 2

	// set up the input matrix
	data := make([]float64, M*N)
	for i := range data {
		data[i] = init
	}
	u := mat.NewDense(M, N, data)

	// set up the border for the input matrix
	for i := 0; i < N; i++ {
		u.Set(0, i, t)
		u.Set(M-1, i, b)
	}
	for i := 0; i < M; i++ {
		u.Set(i, 0, l)
		u.Set(i, N-1, r)
	}

	// create a secondary working matrix
	v := mat.NewDense(M, N, nil)
	v.Copy(u)

	// run the simulation
	for δ, iterations := ε+1.0, 0; δ >= ε; {
		for step := 0; step < 1000; step++ {
			HeatDistributionStep(v, u)
			HeatDistributionStep(u, v)
		}
		iterations += 2000
		δ = maxDiff(u, v)
		fmt.Printf("iterations: %6d, δ: %08.6f, u[8][8]: %10.8f\n", iterations, δ, u.At(8, 8))
	}
}

func Example_heatDistributionSimulation() {
	HeatDistributionSimulation(1024, 1024, 75, 0, 100, 100, 100)

	// Output:
	// iterations:   2000, δ: 0.009073, u[8][8]: 50.99678108
	// iterations:   4000, δ: 0.004537, u[8][8]: 50.50380048
	// iterations:   6000, δ: 0.003025, u[8][8]: 50.33708179
	// iterations:   8000, δ: 0.002268, u[8][8]: 50.25326869
	// iterations:  10000, δ: 0.001815, u[8][8]: 50.20283493
	// iterations:  12000, δ: 0.001512, u[8][8]: 50.16915148
	// iterations:  14000, δ: 0.001296, u[8][8]: 50.14506197
	// iterations:  16000, δ: 0.001134, u[8][8]: 50.12697847
	// iterations:  18000, δ: 0.001008, u[8][8]: 50.11290381
	// iterations:  20000, δ: 0.000907, u[8][8]: 50.10163797
}
