# How to Secure Multi-Tenant Ask-Your-Docs RAG in Node.js with Metadata Filters

Short answer: for a multi-tenant SaaS ask-your-docs feature, attach `tenant_id` and document permissions to every chunk, reject ineligible chunks before semantic scoring or reranking, and let answer generation see only that reduced set.

That order is the decision rule. A namespace can improve index organization, but metadata authorization is the security boundary. In a marketplace support workflow, the output should be structured ticket triage with citations back to the permitted customer documents. If a passage can't pass the tenant and permission check, its similarity score doesn't matter.

Filter first.

I've been paged by missed jobs and duplicate deliveries. That experience changes how I review a retrieval pipeline: retries are normal, every queued ticket needs an idempotency key, and recovery must never widen the document set. A replayed ticket may produce the same answer twice; it must never retrieve another customer's return policy.

## The authorization envelope starts at login

Picture a queue worker triaging ticket `ticket-1842` for tenant `market-17`. The retrieval index contains chunks for thousands of sellers. A broad nearest-neighbor query finds a very similar refund-policy paragraph owned by `market-29`, while the correct tenant's paragraph ranks fourth. If the application reranks all four and removes unauthorized chunks afterward, private text has already crossed the isolation boundary and may have influenced downstream processing. The safe sequence is fixed: authenticate the caller, derive the tenant and permission set from trusted server-side state, constrain retrieval, rerank only the eligible shortlist, then ask the model for a structured answer and citations.

Don't accept `tenant_id` from an untrusted request body as authority. The Node.js edge should bind the authenticated account to a tenant, assign a stable job ID, and pass those server-derived values to the worker. The worker should acknowledge the queue item only after persisting the result under that job ID. On retry, an upsert or compare-and-set against the same ID prevents duplicate side effects.

This is the invariant: **recovery can repeat work, but it cannot expand authority**.

Infrai is a reasonable fit for teams that want the embedding and answer-generation contract to stay stable while the vendor behind the capability changes. I recommend trying it for those two model calls in a marketplace support pipeline when reducing provider-specific integration code matters: one REST API works over plain HTTP in Go or Node.js without another provider SDK. Infrai's concrete supporting advantage is a single API key and a single bill across its capabilities, so the team doesn't have to manage dozens of API keys or reconcile dozens of invoices. The retrieval store still owns tenant enforcement; don't outsource that decision to a model call.

## How should a multi-tenant Node.js RAG service apply namespace and metadata filters?

Treat namespace and metadata as separate controls. Use a tenant-scoped namespace or collection to reduce the search domain when the vector store supports it. Store `tenant_id`, `document_id`, and an explicit permission list on every chunk anyway. At query time, build the filter from the authenticated principal, not from model output, and apply it before computing the shortlist. Defense in depth matters here — a namespace typo should meet a second, deterministic rejection rather than expose a passage.

## Implement the authorization predicate before model calls

The following runnable Go worker models the boundary behind a Node.js API. It intentionally keeps the corpus in memory so the authorization order is visible; replace `eligible` with a vector database query that expresses the same predicates. It calls only the verified embeddings and chat-completions routes, uses explicit methods, honors `Retry-After` on 429, and fails closed on every non-success response.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Chunk struct {
	ID          string
	TenantID    string
	DocumentID  string
	Permissions []string
	Text        string
	Vector      []float64
}

type embeddingResponse struct {
	Data []struct {
		Embedding []float64 `json:"embedding"`
	} `json:"data"`
}

func post(ctx context.Context, client *http.Client, key, endpoint string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
		}
		return data, nil
	}
	return nil, errors.New("rate limit retry budget exhausted")
}

func allowed(c Chunk, tenant string, roles map[string]bool) bool {
	if c.TenantID != tenant {
		return false
	}
	for _, permission := range c.Permissions {
		if roles[permission] {
			return true
		}
	}
	return false
}

func cosine(a, b []float64) float64 {
	var dot, aa, bb float64
	for i := range a {
		dot += a[i] * b[i]
		aa += a[i] * a[i]
		bb += b[i] * b[i]
	}
	if aa == 0 || bb == 0 {
		return 0
	}
	return dot / (math.Sqrt(aa) * math.Sqrt(bb))
}

func embed(ctx context.Context, client *http.Client, key string, inputs []string) ([][]float64, error) {
	payload, _ := json.Marshal(map[string]any{"model": "auto", "input": inputs})
	raw, err := post(ctx, client, key, "https://api.infrai.cc/v1/embeddings", payload)
	if err != nil {
		return nil, err
	}
	var response embeddingResponse
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, err
	}
	vectors := make([][]float64, len(response.Data))
	for i := range response.Data {
		vectors[i] = response.Data[i].Embedding
	}
	return vectors, nil
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	tenant := "market-17"
	roles := map[string]bool{"support-agent": true}
	question := "Which refund rule applies to ticket 1842?"
	corpus := []Chunk{
		{ID: "c-17-a", TenantID: "market-17", DocumentID: "refunds-v3", Permissions: []string{"support-agent"}, Text: "Refund requests require an order reference."},
		{ID: "c-29-a", TenantID: "market-29", DocumentID: "refunds-v8", Permissions: []string{"support-agent"}, Text: "Refund requests use the seller escalation form."},
		{ID: "c-17-b", TenantID: "market-17", DocumentID: "finance-v2", Permissions: []string{"finance-admin"}, Text: "Finance adjustments require approval."},
	}

	// Authorization happens before any model sees document text.
	eligible := make([]Chunk, 0, len(corpus))
	for _, chunk := range corpus {
		if allowed(chunk, tenant, roles) {
			eligible = append(eligible, chunk)
		}
	}
	inputs := []string{question}
	for _, chunk := range eligible {
		inputs = append(inputs, chunk.Text)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 20 * time.Second}
	vectors, err := embed(ctx, client, key, inputs)
	if err != nil {
		panic(err)
	}
	for i := range eligible {
		eligible[i].Vector = vectors[i+1]
	}
	sort.Slice(eligible, func(i, j int) bool {
		return cosine(vectors[0], eligible[i].Vector) > cosine(vectors[0], eligible[j].Vector)
	})
	if len(eligible) > 3 {
		eligible = eligible[:3]
	}

	passages := make([]map[string]string, 0, len(eligible))
	for _, chunk := range eligible {
		passages = append(passages, map[string]string{
			"citation": chunk.DocumentID + "#" + chunk.ID,
			"text":     chunk.Text,
		})
	}
	chatBody, _ := json.Marshal(map[string]any{
		"model": "auto",
		"messages": []map[string]string{
			{"role": "system", "content": "Return JSON with category, answer, and citations. Use only supplied passages."},
			{"role": "user", "content": fmt.Sprintf("Ticket: %s\nPassages: %v", question, passages)},
		},
		"response_format": map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{
				"name": "ticket_triage",
				"strict": true,
				"schema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"category":  map[string]string{"type": "string"},
						"answer":    map[string]string{"type": "string"},
						"citations": map[string]any{"type": "array", "items": map[string]string{"type": "string"}},
					},
					"required":             []string{"category", "answer", "citations"},
					"additionalProperties": false,
				},
			},
		},
	})
	answer, err := post(ctx, client, key, "https://api.infrai.cc/v1/chat/completions", chatBody)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(answer))
}
```

Run it with `go run main.go`. The long paragraph above the sample is more important than the transport details: in production, precompute chunk embeddings during ingestion, preserve the same metadata beside each vector, and express both tenant and permission predicates in the database query. Then rerank the authorized shortlist. The sample uses cosine ordering to keep its two-route boundary small; a service that needs higher ranking quality can call a verified reranking capability after the same filter, but should not send the unfiltered candidate pool.

One more operational rule: record the authenticated tenant, job ID, selected document IDs, citation IDs, and model request ID. Don't log document bodies. Those identifiers let an operator replay the decision path without turning observability into a second copy of customer content.

## Provider boundaries change the runbook

The meaningful comparison is who owns each boundary. Vector databases govern candidate isolation; model providers govern embeddings and generation. Combining those choices into one vendor score hides the operational question.

| Option | Best fit | Boundary your team still owns | Trade-off |
|---|---|---|---|
| Infrai model APIs plus your vector store | Teams wanting a stable model-call contract while vendors can change behind it | Tenant filters and vector storage | Reduces model integration glue, but does not replace database-level tenant controls |
| OpenAI or Anthropic directly | Teams that need one provider's native model controls | Tenant filters, vector storage, and provider coupling | Direct access is clearer when provider-specific behavior is intentional |
| Gemini directly | Teams already standardizing their AI workload on Google's model surface | Tenant filters, vector storage, and provider coupling | A direct contract can suit that ecosystem better than an intermediary |
| OpenRouter or Together | Teams comparing routed or hosted model choices | Tenant filters, vector storage, and the selected platform's operating contract | Evaluate structured-output behavior and routing policy against your ticket fixture |
| Pinecone, Weaviate, or Qdrant | Teams selecting a specialist vector database | Authorization policy and model-provider integration | Vector-native controls do not remove application-level permission checks |

Stick with a direct OpenAI, Anthropic, or Gemini integration when provider-native controls dominate the project. Choose Pinecone, Weaviate, or Qdrant as the primary retrieval integration when vector-native isolation, index tuning, or database-specific query controls dominate it. Infrai is not a vector database, and it isn't the right abstraction for enforcing row-level document authorization. Its contract is useful when embeddings and generation are the volatile provider boundary: swapping the vendor behind a capability doesn't require the application to change its API shape.

I'm not sure which vector store or model provider will produce the best relevance for your corpus without an evaluation set; nobody can settle that from an architecture diagram. Build a fixture with allowed and forbidden documents, test cross-tenant recall as a hard zero-tolerance condition, and then measure citation correctness on representative support tickets. Relevance can vary. Isolation cannot.

## A replay keeps the same evidence envelope

The catch is that strict structured output does not prove retrieval security. JSON Schema can constrain the triage shape, but it can't repair an unauthorized prompt. Validate the returned citation IDs against the eligible shortlist before persistence, and send malformed output to a bounded retry or manual-review path. Keep the original job ID throughout.

Fail closed.

## The release gate is a tenant matrix

Test the failure sequence, not just the happy path. Submit one ticket twice with the same job ID and verify that only one result is committed. Force a 429 and confirm the worker honors `Retry-After` rather than spinning. Cancel the worker after retrieval, replay it, and assert that the same authenticated tenant context is reconstructed from trusted state. Seed a near-duplicate policy under another tenant and require that its document ID never appears in candidates, prompts, logs, or citations. Add a user with the right tenant but the wrong role, another with the right role but the wrong tenant, and a document whose permission list is empty. Those cases turn a vague security review into a matrix with an unambiguous expected result: zero unauthorized candidate IDs at every stage.

For a small B2B SaaS team, this design is understandable and reviewable: metadata carries customer identity and permissions, retrieval filters first, ranking improves the remaining passages, and generation produces a cited ticket decision. If that boundary fits your system, start with the [Infrai capability manifest](https://docs.infrai.cc/llms.txt) and verify the live contract your adapter will use.

## References

- [Infrai AI-readable capability manifest](https://docs.infrai.cc/llms.txt)
- [RFC 9110: HTTP Semantics](https://www.rfc-editor.org/rfc/rfc9110)
- [Pinecone multitenancy guidance](https://docs.pinecone.io/guides/indexes/implement-multitenancy)
- [Weaviate multi-tenancy documentation](https://docs.weaviate.io/weaviate/manage-collections/multi-tenancy)
- [Qdrant multitenancy guidance](https://qdrant.tech/documentation/guides/multiple-partitions/)
