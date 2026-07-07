/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package broker

import (
	"fmt"
	"slices"
	"strings"
)

// Direction is the outcome of a Decide call.
type Direction string

const (
	// NoOp means replicas must not change this tick.
	NoOp Direction = "NoOp"
	// ScaleUp means replicas should increase.
	ScaleUp Direction = "ScaleUp"
	// ScaleDown means replicas should decrease.
	ScaleDown Direction = "ScaleDown"
)

// Reason tokens for a NoOp Decision - stable, machine-readable, and reused
// as a status condition's Reason field.
const (
	ReasonNoMetrics      = "NoMetrics"
	ReasonMixedSignals   = "MixedSignals"
	ReasonAtReplicaBound = "AtReplicaBound"
)

// Params bundles the (already-defaulted) autoscaler thresholds a Decide call
// evaluates against. Percent fields are whole numbers in [0, 100]; replica
// fields are the effective bounds/step sizes after BrokerAutoscalerSpec
// defaulting has been applied by the caller.
type Params struct {
	LowerCPUPercent  int32
	HigherCPUPercent int32
	ScaleUpBy        int32
	ScaleDownBy      int32
	MinReplicas      int32
	MaxReplicas      int32 // 0 means unbounded.
	CurrentReplicas  int32
}

// Decision is the result of evaluating a tick's CPU readings against Params.
type Decision struct {
	Direction Direction
	// TargetReplicas is only meaningful when Direction != NoOp.
	TargetReplicas int32
	// Reason is a short, stable machine-readable token suitable for a status
	// condition's Reason field.
	Reason string
	// Message is a human-readable explanation suitable for a status
	// condition's Message field or an Event.
	Message string
}

// Decide applies Pulsar/KAAP's unanimous broker CPU vote: scale up only if
// every broker's CPU percent is strictly above HigherCPUPercent, scale down
// only if every broker's CPU percent is strictly below LowerCPUPercent.
// A broker sitting between the thresholds, a mix of hot and cold brokers, or
// no readings at all, all resolve to NoOp - a single hot or idle broker never
// moves replicas on its own. The result is clamped to [MinReplicas,
// MaxReplicas] (MaxReplicas==0 meaning unbounded); a decision that would not
// actually change the replica count (already at a clamp) also resolves to
// NoOp.
func Decide(cpuPercentByBroker map[string]int32, p Params) Decision {
	if len(cpuPercentByBroker) == 0 {
		return Decision{Direction: NoOp, Reason: ReasonNoMetrics, Message: "no broker CPU readings available"}
	}

	names := sortedKeys(cpuPercentByBroker)

	sawHot, sawCold := false, false
	for _, name := range names {
		percent := cpuPercentByBroker[name]
		switch {
		case percent < p.LowerCPUPercent:
			if sawHot {
				return mixedSignalsDecision(names, cpuPercentByBroker)
			}
			sawCold = true
		case percent > p.HigherCPUPercent:
			if sawCold {
				return mixedSignalsDecision(names, cpuPercentByBroker)
			}
			sawHot = true
		default:
			return Decision{
				Direction: NoOp,
				Reason:    ReasonMixedSignals,
				Message: fmt.Sprintf("broker %s CPU %d%% is between lowerCpuThreshold %d%% and higherCpuThreshold %d%%",
					name, percent, p.LowerCPUPercent, p.HigherCPUPercent),
			}
		}
	}

	switch {
	case sawHot && sawCold:
		// Unreachable given the per-iteration checks above, but keep Decide
		// total rather than panicking if that invariant is ever broken.
		return mixedSignalsDecision(names, cpuPercentByBroker)
	case sawHot:
		return clamped(ScaleUp, p.CurrentReplicas+p.ScaleUpBy, p)
	default:
		return clamped(ScaleDown, p.CurrentReplicas-p.ScaleDownBy, p)
	}
}

func mixedSignalsDecision(names []string, cpuPercentByBroker map[string]int32) Decision {
	return Decision{
		Direction: NoOp,
		Reason:    ReasonMixedSignals,
		Message: fmt.Sprintf("brokers disagree on load (%s) - never scaling on a single hot or idle broker",
			formatReadings(names, cpuPercentByBroker)),
	}
}

func formatReadings(names []string, cpuPercentByBroker map[string]int32) string {
	var b strings.Builder
	for i, name := range names {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s=%d%%", name, cpuPercentByBroker[name])
	}
	return b.String()
}

func clamped(direction Direction, target int32, p Params) Decision {
	clampedTarget := max(target, p.MinReplicas)
	if p.MaxReplicas > 0 {
		clampedTarget = min(clampedTarget, p.MaxReplicas)
	}

	if clampedTarget == p.CurrentReplicas {
		return Decision{
			Direction: NoOp,
			Reason:    ReasonAtReplicaBound,
			Message: fmt.Sprintf("%s would move replicas outside [%d, %d]; already at %d",
				direction, p.MinReplicas, p.MaxReplicas, p.CurrentReplicas),
		}
	}

	verb := "up"
	if direction == ScaleDown {
		verb = "down"
	}
	return Decision{
		Direction:      direction,
		TargetReplicas: clampedTarget,
		Reason:         string(direction),
		Message: fmt.Sprintf("scaling %s from %d to %d replicas (every broker unanimously %s threshold)",
			verb, p.CurrentReplicas, clampedTarget, unanimousVerb(direction)),
	}
}

func unanimousVerb(direction Direction) string {
	if direction == ScaleUp {
		return "above the higher CPU"
	}
	return "below the lower CPU"
}

func sortedKeys(m map[string]int32) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
