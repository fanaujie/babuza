package breaker

type Breaker interface {
	Success()
	Fail()
	Ready() bool
	Reset()
}
