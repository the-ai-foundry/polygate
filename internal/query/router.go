package query

import "fmt"

// Router maps engine names to QueryEngine implementations.
type Router struct {
	engines map[string]QueryEngine
}

func NewRouter() *Router {
	return &Router{engines: make(map[string]QueryEngine)}
}

func (r *Router) Register(engine QueryEngine) {
	r.engines[engine.Name()] = engine
}

func (r *Router) Route(name string) (QueryEngine, error) {
	e, ok := r.engines[name]
	if !ok {
		return nil, fmt.Errorf("unknown engine %q", name)
	}
	return e, nil
}

func (r *Router) List() []string {
	names := make([]string, 0, len(r.engines))
	for n := range r.engines {
		names = append(names, n)
	}
	return names
}
