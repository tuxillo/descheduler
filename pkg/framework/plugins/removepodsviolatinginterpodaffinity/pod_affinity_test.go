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
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/events"
	frameworktypes "sigs.k8s.io/descheduler/pkg/framework/types"

	"sigs.k8s.io/descheduler/pkg/descheduler/evictions"
	podutil "sigs.k8s.io/descheduler/pkg/descheduler/pod"
	frameworkfake "sigs.k8s.io/descheduler/pkg/framework/fake"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/defaultevictor"
	"sigs.k8s.io/descheduler/pkg/utils"
	"sigs.k8s.io/descheduler/test"
)

func TestInterPodAffinity(t *testing.T) {
	node1 := test.BuildTestNode("n1", 2000, 3000, 10, func(node *v1.Node) {
		node.ObjectMeta.Labels = map[string]string{
			"region": "main-region",
		}
	})
	node2 := test.BuildTestNode("n2", 2000, 3000, 10, func(node *v1.Node) {
		node.ObjectMeta.Labels = map[string]string{
			"region": "main-region",
		}
	})
	node3 := test.BuildTestNode("n3", 2000, 3000, 10, func(node *v1.Node) {
		node.ObjectMeta.Labels = map[string]string{
			"region": "main-region",
		}
	})

	node4 := test.BuildTestNode("n4", 2000, 3000, 10, func(node *v1.Node) {
		node.ObjectMeta.Labels = map[string]string{
			"region": "south",
		}
	})

	node5 := test.BuildTestNode("n5", 2000, 3000, 10, func(node *v1.Node) {
		node.Spec = v1.NodeSpec{
			Unschedulable: true,
		}
	})

	p1 := test.BuildTestPod("p1", 100, 0, node1.Name, nil)
	p2 := test.BuildTestPod("p2", 100, 0, node2.Name, nil)
	p3 := test.BuildTestPod("p3", 100, 0, node3.Name, nil)
	p4 := test.BuildTestPod("p4", 100, 0, node4.Name, nil)
	p5 := test.BuildTestPod("p5", 100, 0, node4.Name, nil)
	p6 := test.BuildTestPod("p6", 100, 0, node5.Name, nil)

	criticalPriority := utils.SystemCriticalPriority
	nonEvictablePod := test.BuildTestPod("non-evict", 100, 0, node1.Name, func(pod *v1.Pod) {
		pod.Spec.Priority = &criticalPriority
	})

	// Set Pods labels
	p1.Labels = map[string]string{"foo": "bar"}
	p2.Labels = map[string]string{"foo": "bar"}
	p3.Labels = map[string]string{"foo": "bar"}
	nonEvictablePod.Labels = map[string]string{"foo": "bar"}
	test.SetNormalOwnerRef(p1)
	test.SetNormalOwnerRef(p2)
	test.SetNormalOwnerRef(p3)
	test.SetNormalOwnerRef(p4)
	test.SetNormalOwnerRef(p5)
	test.SetNormalOwnerRef(p6)

	// set pod affinity
	test.SetPodAffinity(p4, "foo", "bar")
	test.SetPodAffinity(p5, "foo", "bar")

	// set pod priority
	test.SetPodPriority(p5, 100)
	test.SetPodPriority(p6, 50)

	//	var uint1 uint = 1
	//	var uint3 uint = 3

	tests := []struct {
		description                    string
		maxPodsToEvictPerNode          *uint
		maxNoOfPodsToEvictPerNamespace *uint
		maxNoOfPodsToEvictTotal        *uint
		pods                           []*v1.Pod
		expectedEvictedPodCount        uint
		nodeFit                        bool
		nodes                          []*v1.Node
	}{
		{
			description:             "Maximum pods to evict - 0",
			pods:                    []*v1.Pod{p1, p2, p3, p4, p5},
			nodes:                   []*v1.Node{node1, node2, node3, node4},
			expectedEvictedPodCount: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var objs []runtime.Object
			for _, node := range test.nodes {
				objs = append(objs, node)
			}
			for _, pod := range test.pods {
				objs = append(objs, pod)
			}
			fakeClient := fake.NewSimpleClientset(objs...)

			sharedInformerFactory := informers.NewSharedInformerFactory(fakeClient, 0)
			podInformer := sharedInformerFactory.Core().V1().Pods().Informer()

			getPodsAssignedToNode, err := podutil.BuildGetPodsAssignedToNodeFunc(podInformer)
			if err != nil {
				t.Errorf("Build get pods assigned to node function error: %v", err)
			}

			sharedInformerFactory.Start(ctx.Done())
			sharedInformerFactory.WaitForCacheSync(ctx.Done())

			eventRecorder := &events.FakeRecorder{}

			podEvictor := evictions.NewPodEvictor(
				fakeClient,
				eventRecorder,
				evictions.NewOptions().
					WithMaxPodsToEvictPerNode(test.maxPodsToEvictPerNode).
					WithMaxPodsToEvictPerNamespace(test.maxNoOfPodsToEvictPerNamespace).
					WithMaxPodsToEvictTotal(test.maxNoOfPodsToEvictTotal),
			)

			defaultevictorArgs := &defaultevictor.DefaultEvictorArgs{
				EvictLocalStoragePods:   false,
				EvictSystemCriticalPods: false,
				IgnorePvcPods:           false,
				EvictFailedBarePods:     false,
				NodeFit:                 test.nodeFit,
			}

			evictorFilter, err := defaultevictor.New(
				defaultevictorArgs,
				&frameworkfake.HandleImpl{
					ClientsetImpl:                 fakeClient,
					GetPodsAssignedToNodeFuncImpl: getPodsAssignedToNode,
					SharedInformerFactoryImpl:     sharedInformerFactory,
				},
			)
			if err != nil {
				t.Fatalf("Unable to initialize the plugin: %v", err)
			}

			handle := &frameworkfake.HandleImpl{
				ClientsetImpl:                 fakeClient,
				GetPodsAssignedToNodeFuncImpl: getPodsAssignedToNode,
				PodEvictorImpl:                podEvictor,
				SharedInformerFactoryImpl:     sharedInformerFactory,
				EvictorFilterImpl:             evictorFilter.(frameworktypes.EvictorPlugin),
			}
			plugin, err := New(
				&RemovePodsViolatingInterPodAffinityArgs{},
				handle,
			)

			plugin.(frameworktypes.DeschedulePlugin).Deschedule(ctx, test.nodes)
			podsEvicted := podEvictor.TotalEvicted()
			if podsEvicted != test.expectedEvictedPodCount {
				t.Errorf("Unexpected no of pods evicted: pods evicted: %d, expected: %d", podsEvicted, test.expectedEvictedPodCount)
			}
		})
	}
}
