# Prototype

A prototype of a next-generation control plane that could replace Kubernetes. It exists to collect
realistic data on:

1. distributed workloads, not local ones;
2. the three core workload types -- traditional compute such as web servers, serverless/FaaS, and
   AI inference, particularly agents;
3. the cost of acting across userspace and kernel boundaries, to show where cutting system-call
   taxes and data marshalling pays.

**The hypothesis.** Does a unified control plane produce emergent properties -- new use cases --
that siloed but specialised control planes cannot?

**Caveat: the host is not the target.** Mac and Apple silicon give one pool of unified memory. The
datacenter target, which matters more, is split: GPUs, TPUs and other accelerators work from their
own HBM, while traditional compute runs in a separate pool of host DDR. The simulator models the
split by default.

## Documents

- [`README.md`](../README.md) -- the direction: what Polyphonic is for.
- [`taxo.md`](taxo.md) -- the workload taxonomy the scheduler has to serve.
- [`owned-and-observed.md`](owned-and-observed.md) -- the design: what the scheduler owns, infers
  and observes, the data path, the system of record, and the phases and what they leave open.
- [`residency-ledger.md`](residency-ledger.md) -- the simulator's model and its current results.
- `phase-N.md` -- one per phase: the plan, the predictions stated before the run, and what was
  measured. These are the historical record and keep their results as measured; every other
  document states only current decisions and results.
