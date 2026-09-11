# Polyphonic - The Next Generation Compute Orchestrator

Kubernetes is a great orchestrator for traditional computing workloads but it does not handle FaaS, AI, and edge computing cases very well. In addition to those issues, every disparate technology that orchestrates them independently has to communicate across different boundaries increasing the latency between requests. A new orchestrator would try to accomplish the following:
- handle modern workloads as first class citizens
	- instant scheduling for serverless workloads
	- traditional long running compute
	- lightweight orchestration for edge devices (probably managed as a separate binary that packages and manages these on device, with an API surface available to phone back to a larger network for extended functionality)
	- AI inference (llm-d style but model-type agnostic for routing/scheduling but does not replace inference engines like vLLM)
	- AI post training - make it easy to schedule recurring jobs for training and updating small models affecting larger ones
- central and distributed efficiency
	- strong support for local compute
	- strong primitives to schedule and manage workloads that span regions
		- perhaps it doesn't work with extremely stateful services like databases, but for stateless services, noticing that demand in a given region is high should spin up resources close to that region to reduce request latency
- create zero cost extensions
	- gRPC and sidecars are the mechanism by which k8s does its job which incurs latency and overhead
	- having an ABI makes it possible to extend natively
	- having a WASM interface makes it possible to have cross language support and possibly zero copy support
- making lightweight VMs a first-class concept while retaining a type of container support
	- containers as the main abstraction have issues with isolation
	- lightweight VMs can be serialized easily and live migrated
	- containers within a virtualized environment can still be possible for more efficient resource sharing, and should be a choice and relatively easy to accomplish/support but the native abstraction should be a VM
- first class DX support
	- an entire cloud run locally is not feasible
	- core capabilities can be
	- ring 0 for core targets but ring 3 for DX portability
	- lightweight orchestration to enable local dev and rapid iterations and onboarding cycles with minimal to no cloud
The core selling point is the emergent properties of having a unified orchestrator across workloads. It must be better than the best orchestrators that specialize in their own domain, not necessarily independently, but across workload fabrics specifically. For instance, a FaaS function that calls into AI Inference should be able to understand that that's going to happen ahead of time, reducing latency by having excellent execution and routing properties independently, but also being able to pre-warm and route based on its knowledge of the shared differences between workloads.
