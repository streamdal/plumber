// +build debug

package stream

import "sort"

type ProducersCoordinator = environmentCoordinator

func (env *Environment) ClientCoordinator() map[string]*ProducersCoordinator {
	return env.producers.producersCoordinator
}

func (env *Environment) Nodes() []string {
	var result []string
	for s, _ := range env.producers.producersCoordinator {
		result = append(result, s)
	}
	sort.Strings(result)
	return result
}

func (env *Environment) ProducerPerStream(streamName string) []*Producer {
	var result []*Producer
	for _, p := range env.producers.producersCoordinator {
		for _, client := range p.getClientsPerContext() {
			for _, prod := range client.coordinator.producers {
				if prod.(*Producer).options.streamName == streamName {
					result = append(result, prod.(*Producer))
				}
			}
		}
	}
	return result
}

func (env *Environment) ClientsPerStream(streamName string) []*Client {
	var result []*Client
	for _, p := range env.producers.producersCoordinator {
		for _, client := range p.getClientsPerContext() {
			for _, prod := range client.coordinator.producers {
				if prod.(*Producer).options.streamName == streamName {
					result = append(result, client)
				}
			}
		}
	}
	return result
}

func (env *Environment) Coordinators() []*Coordinator {
	var result []*Coordinator
	for _, p := range env.producers.producersCoordinator {
		for _, client := range p.getClientsPerContext() {
			result = append(result, client.coordinator)
		}
	}
	return result
}
