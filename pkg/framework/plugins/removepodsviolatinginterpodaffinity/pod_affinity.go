/*
Copyright 2017 The Kubernetes Authors.

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

package removepodsviolatinginterpodaffinity

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/descheduler/pkg/descheduler/evictions"
	podutil "sigs.k8s.io/descheduler/pkg/descheduler/pod"
	frameworktypes "sigs.k8s.io/descheduler/pkg/framework/types"
	"sigs.k8s.io/descheduler/pkg/utils"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

const PluginName = "RemovePodsViolatingInterPodAffinity"

// RemovePodsViolatingInterPodAffinity evicts pods on the node which violate pod affinity
type RemovePodsViolatingInterPodAffinity struct {
	handle    frameworktypes.Handle
	args      *RemovePodsViolatingInterPodAffinityArgs
	podFilter podutil.FilterFunc
}

var _ frameworktypes.DeschedulePlugin = &RemovePodsViolatingInterPodAffinity{}

// New builds plugin from its arguments while passing a handle
func New(args runtime.Object, handle frameworktypes.Handle) (frameworktypes.Plugin, error) {
	InterPodAffinityArgs, ok := args.(*RemovePodsViolatingInterPodAffinityArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type RemovePodsViolatingInterPodAffinityArgs, got %T", args)
	}

	var includedNamespaces, excludedNamespaces sets.Set[string]
	if InterPodAffinityArgs.Namespaces != nil {
		includedNamespaces = sets.New(InterPodAffinityArgs.Namespaces.Include...)
		excludedNamespaces = sets.New(InterPodAffinityArgs.Namespaces.Exclude...)
	}

	podFilter, err := podutil.NewOptions().
		WithNamespaces(includedNamespaces).
		WithoutNamespaces(excludedNamespaces).
		WithLabelSelector(InterPodAffinityArgs.LabelSelector).
		BuildFilterFunc()
	if err != nil {
		return nil, fmt.Errorf("error initializing pod filter function: %v", err)
	}

	return &RemovePodsViolatingInterPodAffinity{
		handle:    handle,
		podFilter: podFilter,
		args:      InterPodAffinityArgs,
	}, nil
}

// Name retrieves the plugin name
func (d *RemovePodsViolatingInterPodAffinity) Name() string {
	return PluginName
}

func (d *RemovePodsViolatingInterPodAffinity) Deschedule(ctx context.Context, nodes []*v1.Node) *frameworktypes.Status {
	pods, err := podutil.ListPodsOnNodes(nodes, d.handle.GetPodsAssignedToNodeFunc(), d.podFilter)
	if err != nil {
		return &frameworktypes.Status{
			Err: fmt.Errorf("error listing all pods: %v", err),
		}
	}

	podsInANamespace := podutil.GroupByNamespace(pods)
	podsOnANode := podutil.GroupByNodeName(pods)
	nodeMap := utils.CreateNodeMap(nodes)

loop:
	for _, node := range nodes {
		klog.V(1).InfoS("Processing node", "node", klog.KObj(node))
		pods := podsOnANode[node.Name]
		// sort the evict-able Pods based on priority, if there are multiple pods with same priority, they are sorted based on QoS tiers.
		podutil.SortPodsBasedOnPriorityLowToHigh(pods)
		totalPods := len(pods)
		for i := 0; i < totalPods; i++ {
			if utils.CheckPodAffinityViolation(pods[i], podsInANamespace, nodeMap) {
				if d.handle.Evictor().Filter(pods[i]) && d.handle.Evictor().PreEvictionFilter(pods[i]) {
					err := d.handle.Evictor().Evict(ctx, pods[i], evictions.EvictOptions{StrategyName: PluginName})
					switch err.(type) {
					case *evictions.EvictionNodeLimitError:
						continue loop
					case *evictions.EvictionTotalLimitError:
						return nil
					default:
						klog.Errorf("eviction failed: %v", err)
					}
				}
			}
		}
	}
	return nil
}
