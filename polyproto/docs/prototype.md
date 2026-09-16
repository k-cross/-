This is a prototype of a next generation control plane that could replace Kubernetes.
The focus of this prototype is to collect realistic data on:
1. distributed workloads not local ones
2. three core workload types (traditional compute like web servers, serverless/FaaS, and AI Inference particularly around agents)
3. the cost of performing actions across userspace and kernel boundaries to help inform what to build to optimize the reduction of paying system call taxes and marshalling data

The hypothesis:
Are emergent properties that unlock new use cases when using a unified control plane instead of siloed but specialized ones?

Caveats:
On Mac and Apple silicon, we have access to an entire pool of unified memory but datacenter targets are going to be different and are more important to us, GPU/TPU and other accelerators will likely be working off of a separate pool of HBM where the traditional compute still happens on a seperate pool of DDR.
