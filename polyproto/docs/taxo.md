# AI Inference Dynamics

The following workload patterns serve as a representative sample used to isolate properties creating a taxonomy for our control plane.

| Pattern                                | Basic shape                                          | External knowledge / tools                     | State and iteration                         | Typical output                         | Example                                        |
| -------------------------------------- | ---------------------------------------------------- | ---------------------------------------------- | ------------------------------------------- | -------------------------------------- | ---------------------------------------------- |
| One-shot generation                    | One input → one model response                       | None, or fixed prompt context                  | No meaningful loop                          | Text, image, code, structured JSON     | “Write a product description”                  |
| Structured extraction / classification | One input → constrained prediction                   | Usually none                                   | No loop; often batchable                    | Label, score, fields, JSON schema      | Extract invoice total and vendor               |
| Conversational assistant               | Repeated turns                                       | May use fixed system context                   | Short-lived conversation history            | Streaming responses                    | General-purpose chat support                   |
| Fixed-pipeline RAG                     | Query → retrieve → generate                          | Read-only search over documents/data           | Usually one bounded retrieval pass          | Grounded answer with citations         | “What is our travel policy?”                   |
| Workflow / tool-using pipeline         | Predetermined sequence of model/tool steps           | APIs, databases, calculators, search           | Fixed control flow                          | Result or completed workflow           | Parse ticket → query CRM → draft response      |
| Agentic workflow                       | Goal → plan → tool calls → observe → revise → finish | Dynamic tool use, often including RAG          | Multi-step and adaptive                     | Answer, recommendation, or action      | Investigate outage and open remediation ticket |
| Long-running / asynchronous agent      | Agentic, but over extended time                      | Multiple systems and event sources             | Durable memory, checkpoints, human approval | Ongoing task execution                 | Monitor a queue and resolve routine cases      |
| Multi-agent system                     | Multiple specialized agents collaborate              | Shared tools and/or delegated roles            | Iterative coordination                      | Composite decision or deliverable      | Research agent + analyst + reviewer            |
| Multimodal real-time inference         | Continuous or latency-sensitive inputs               | Sensors, audio/video streams, vision pipelines | Often stateful over a stream                | Live detection, transcription, control | Voice assistant or factory defect detection    |
| Batch inference                        | Large offline set of independent requests            | Optional retrieval or enrichment               | Little per-item state                       | Enriched records, scores, summaries    | Summarize 10 million support tickets           |

## A Better Taxonomy

Each workload pattern is characterized across four dimensions.
1. Control flow
	- **Single-call:** One LLM invocation. This includes rewriting, translation, basic Q&A, extraction, and simple classification.
	- **Fixed multi-step:** A developer defines the sequence—e.g., retrieve documents, rerank them, call the LLM, validate JSON, retry once.
	- **Dynamic multi-step:** The model decides which next step to take based on intermediate evidence. This is the core “agentic” characteristic.
	- **Parallel / delegated:** Several calls or agents work concurrently, then a final model or deterministic program synthesizes results.
2. Knowledge grounding
	- **Closed-book:** Uses model parameters and the prompt only.
	- **Prompt-grounded:** Uses user-provided documents, a long system prompt, or fixed reference material.
	- **RAG-grounded:** Retrieves from indexes, files, databases, web search, or enterprise sources at inference time.
	- **Tool-grounded:** Calls authoritative systems such as a pricing API, SQL database, calculator, or CRM.
	- **Hybrid:** Combines retrieval, live APIs, structured data, and model reasoning.
3. State and time horizon
	- **Stateless request:** Every request is independent.
	- **Session state:** The system maintains a chat history or temporary working memory for a user session.
	- **Task state:** The system stores a plan, observations, intermediate outputs, and checkpoints while completing a job.
	- **Durable memory:** The system retains selected facts, preferences, task history, and artifacts across sessions.
	- **Event-driven / continuous:** The system wakes on new events, monitors signals, or processes a live stream.
4. Authority to act
	- **Read-only:** Answers, summarizes, extracts, classifies, or recommends.
	- **Draft-only:** Produces an email, code change, report, or proposed transaction for human review.
	- **Human-approved execution:** Prepares an action but requires confirmation before sending, changing, purchasing, or deleting.
	- **Bounded autonomous execution:** Acts automatically within strict scopes—e.g., reset a password after verified identity checks.
	- **High-authority autonomy:** Can make material changes across systems. This should be rare and tightly governed.
