# Module 01: API Paradigms, RESTful Architecture & Idempotency

> **Component Locations:**
> * `src/models/schemas.py`
> * `src/routers/comments.py`
> * `src/main.py`
>
> **Test Suite:** `tests/test_idempotency.py`
>
> **Architectural Scope:** Cross-Service API Protocols (REST, SOAP, GraphQL, gRPC, WebSockets), HTTP Data Passing Channels, Semantic Status Codes, and Idempotent Mutation Guarantees.

## Part 1: Cross-Service API Paradigms (REST, GraphQL, gRPC & WebSockets)

### 1. What Is It?
An **Application Programming Interface (API)** defines the controlled surface area of permitted interactions between decoupled software systems (such as a web frontend communicating with a backend, or two microservices exchanging data) without exposing direct database access.

```text
+-----------------------------------------------------------------------------+
|                        1. REST (Multiple Resource URLs)                     |
|  Client  --- GET /api/v1/posts/10/comments (JSON over HTTP) --->  Backend   |
+-----------------------------------------------------------------------------+
|                        2. GraphQL (Single Query Endpoint)                   |
|  Client  --- POST /graphql { post(id:10) { comments { id } } } -> Backend   |
+-----------------------------------------------------------------------------+
|                        3. gRPC (Binary Protocol Buffers)                    |
|  Service A --- CheckToxicity(CommentProto) (HTTP/2 Binary) ---->  Service B |
+-----------------------------------------------------------------------------+
|                        4. WebSockets (Persistent Bidirectional)             |
|  Client  <== Persistent TCP Duplex Stream (Real-Time Push) ====>  Backend   |
+-----------------------------------------------------------------------------+
```

### 2. Why Do We Need It?
No single communication protocol can optimally serve every distributed systems workload:

1. **Public Browser & Mobile CRUD:** Needs standardized HTTP caching, human-readable payloads, and universal client compatibility (**REST**).
2. **Legacy Enterprise Integration:** Older banking, telecom, and ERP systems mandate strict XML contracts (**SOAP**).
3. **Complex Frontend Data Graphs:** Fetching a post, its nested comments, and author badges across multiple REST endpoints causes round-trip waterfalls or over-fetches unused JSON fields (**GraphQL** solves this with a single query endpoint).
4. **High-Throughput Microservice Mesh:** Parsing text-based JSON or XML across dozens of internal microservices burns CPU and network bandwidth (**gRPC** compiles `.proto` files into compact binary Protocol Buffers).
5. **Real-Time Live Feeds:** Polling a REST endpoint every second to check for new comments wastes server compute (**WebSockets** maintain an open bidirectional connection so the backend pushes events immediately).

### 3. How It Works in PulseStream
* **RESTful Core (`/api/v1/...`):** Serves primary public CRUD operations using FastAPI and Rust-backed `ORJSONResponse` for high-speed JSON serialization.
* **OpenAPI 3.1 Contract (`/openapi.json` & `/docs`):** Automatically generates machine-readable API specifications from our code so frontend and third-party consumers can integrate without manual guesswork.
* **Multi-Protocol Extensibility:** Separates the transport layer (`src/routers/`) from business logic (`src/controllers/`) so GraphQL queries, internal gRPC moderation stubs (`proto/moderation.proto`), and WebSocket live comment broadcasts all invoke the exact same underlying controller methods.

### 4. Comparison of API Paradigms

| Paradigm | Payload Format | Directionality | Primary Use Case | Latency Overhead |
| :--- | :--- | :--- | :--- | :--- |
| **REST** | **JSON** | **Request-Response** | **Public web and mobile APIs** | **Low-Medium** |
| SOAP | XML | Request-Response | Legacy enterprise systems | High |
| GraphQL | JSON | Query-Response | Custom client data graphs | Medium |
| gRPC | Binary Protobuf | Bidirectional stream | Internal microservices | Very Low |
| WebSockets | JSON or Binary | Persistent duplex | Real-time chat and live feeds | Very Low |

Choosing the right paradigm balances client simplicity against wire efficiency: REST leverages native HTTP verbs and status codes, GraphQL shifts field selection to the frontend client, gRPC maximizes cross-language microservice throughput via binary serialization, and WebSockets eliminate polling latency for live events.

### 5. Problem & Solution Summary
* **Problem:** Exposing a database directly to clients destroys security and data integrity, while forcing all external and internal traffic through a single rigid protocol causes either client over-fetching or microservice serialization bottlenecks.
* **Solution:** Expose a clean RESTful JSON interface for standard resource operations while decoupling business orchestration so GraphQL, gRPC, and WebSockets can share the same core domain engine.

### 6. Real-World Applications
* **Stripe & Twilio:** Use versioned REST APIs with JSON payloads and OpenAPI specifications for global developer integrations.
* **GitHub:** Provides both a REST API (v3) for webhook and resource automation and a GraphQL API (v4) for complex repository metadata queries.
* **Netflix & Uber:** Use gRPC with Protocol Buffers for low-latency internal communication across hundreds of polyglot microservices.
* **Slack & Discord:** Use REST for historical message fetches and WebSockets for real-time message and presence push events.

## Part 2: RESTful URL Design, Data Passing Channels & Payload Security (`src/models/schemas.py`)

### 1. What Is It?
A RESTful endpoint combines an **HTTP Method** with a **URI Path**, **Query Parameters**, **HTTP Headers**, and a **JSON Request Body**. PulseStream enforces strict channel separation and validates every incoming payload through **Pydantic v2** data contracts (`src/models/schemas.py`) before any business logic executes:

```text
Incoming HTTP Request
+-----------------------------------------------------------------------+
| Path:   /api/v1/posts/42/comments      (Resource Identity: post_id)   |
| Query:  ?cursor_id=726481&limit=20     (Optional Filter & Pagination) |
| Header: Content-Type: application/json (Metadata Contract)            |
| Body:   {"user_id": 7, "content": "Hi"} (Mutation & Sensitive Data)   |
+-----------------------------------------------------------------------+
                                   |
                                   v
                 +-----------------------------------+
                 |   Pydantic Schema Gatekeeper      |
                 |     (src/models/schemas.py)       |
                 +-----------------------------------+
                    /                             \
        [Invalid Type / Length]           [Valid Schema Match]
                  /                                 \
                 v                                   v
     HTTP 422 Unprocessable Entity         Controller Business Logic
     (Rejected at API Boundary)            (Guaranteed Type-Safe Object)
```

### 2. Why Do We Need It?
1. **Preventing URL Credential & PII Leakage:** Passing sensitive data (such as passwords, tokens, or private message content) in URL paths or query strings (`POST /register?user=alice&pass=1234`) exposes secrets in browser history, bookmarked links, proxy caches, and plaintext server logs. Sensitive and mutation payloads must travel strictly in the HTTPS request body.
2. **Avoiding Deep Nesting Anti-Patterns:** When a comment is created, it receives a globally unique 64-bit Snowflake ID. Forcing clients to nest every subsequent operation under `/posts/{post_id}/comments/{comment_id}/replies/{reply_id}` couples frontend code to parent IDs it does not need.
3. **Balancing Nested Routes vs. Query Filtering:** Simple parent-child lookups work naturally as nested routes (`/posts/{post_id}/comments`), whereas multi-attribute searches across users, posts, and sort orders require a flat query-filtered endpoint (`/comments?post_id=42&user_id=7`).

### 3. How It Works in PulseStream
1. **Shallow Resource Nesting:**
   * Creating or listing top-level comments for a specific post uses `/api/v1/posts/{post_id}/comments`.
   * Operating on a specific comment (`GET`, `PUT`, `PATCH`, `DELETE`) or its replies (`/api/v1/comments/{comment_id}/replies`) addresses `/comments/{comment_id}` directly without requiring `post_id` in the URL path.
2. **Flat Query Collection (`GET /api/v1/comments`):** Supports optional query parameters (`post_id`, `user_id`, `cursor_id`, `sort`, `limit`) bounded by FastAPI `Query(default=20, ge=1, le=100)` so malicious clients cannot request 10,000,000 rows in a single call.
3. **Purpose-Built Pydantic Contracts:**
   * `CommentCreate`: Validates `user_id: int` and `content: str` (1 to 2,000 chars) on `POST`.
   * `CommentReplace`: Requires all mutable fields (`user_id`, `content`, `is_pinned`) for full document replacement on `PUT`.
   * `CommentPatch`: Marks all mutable fields as optional (`None` default) so partial `PATCH` updates only touch keys explicitly present in `payload.model_dump(exclude_unset=True)`.

### 4. Comparison of HTTP Data Passing Channels

| Channel | Example Syntax | URL Visibility | Cache Key Inclusion | PulseStream Usage |
| :--- | :--- | :--- | :--- | :--- |
| URL Path | `/comments/72648101` | Visible in logs | Yes | Unique resource IDs |
| Query String | `?user_id=7&limit=20` | Visible in logs | Yes | Filtering, sorting, cursors |
| HTTP Headers | `Content-Type` | Hidden from URL | Controls cache rules | Auth tokens, MIME metadata |
| **JSON Body** | **`{"content": "..."}`** | **Encrypted in TLS** | **No** | **Mutations and private data** |

Separating data across these four channels ensures that URLs remain clean and cacheable for reads while keeping mutation payloads and sensitive fields protected inside the TLS-encrypted request body.

### 5. Problem & Solution Summary
* **Problem:** Clients need intuitive hierarchical routes for post feeds, flexible multi-attribute filtering for search views, and strict protection against malformed JSON or URL data leakage.
* **Solution:** Pair shallow nested endpoints (`/posts/{id}/comments`) with direct entity addressing (`/comments/{id}`) and a query-filtered root collection (`/comments`), backed by strict Pydantic v2 body validation.

### 6. Real-World Applications
* **YouTube Data API & Reddit API:** Use shallow parent routes for top-level thread retrieval and direct comment IDs for edits, deletions, and reply chains.
* **FastAPI / Pydantic v2 Production Stacks:** Reject invalid request payloads at the C/Rust boundary with `422 Unprocessable Entity` before consuming application worker threads.

## Part 3: HTTP Method Semantics, Idempotency & Status Codes (`src/routers/comments.py`)

### 1. What Is It?
In distributed systems, **Idempotency** guarantees that executing an identical request once or executing it 100 times produces the exact same server state without unintended side effects. PulseStream aligns its router handlers strictly with RFC 9110 HTTP method semantics and three-digit status codes:

```text
1. Non-Idempotent Creation (POST /api/v1/posts/10/comments)
   Request #1  ---> Mints Snowflake ID 72648101 (Returns 201 Created)
   Retry   #2  ---> Mints Snowflake ID 72648109 (Creates duplicate record!)

2. Idempotent Full Replacement (PUT /api/v1/comments/72648101)
   Request #1  ---> Overwrites {content: "Updated", is_pinned: true} (200 OK)
   Retry   #50 ---> Overwrites {content: "Updated", is_pinned: true} (200 OK)
                    (Same comment_id 72648101, same created_at_ms, zero duplicates!)

3. Idempotent Partial Update (PATCH /api/v1/comments/72648101)
   Request #1  ---> Payload {"is_pinned": false} -> Updates only is_pinned (200 OK)
   Retry   #50 ---> Payload {"is_pinned": false} -> State remains identical (200 OK)
```

### 2. Why Do We Need It?
1. **Surviving Flaky Network Retries:** When a mobile client submits a comment edit over an unstable connection and the response drops, the client or load balancer automatically retries the request. Because `PUT` and `PATCH` are idempotent, retrying 10 times is completely safe and never creates duplicate comments.
2. **Full Replacement (`PUT`) vs. Partial Modification (`PATCH`):** Clients often want to toggle a single boolean flag (such as pinning a comment) without resending a 2,000-character text body. `PATCH` updates only the supplied attributes, whereas `PUT` replaces the entire resource representation while preserving its immutable identity (`comment_id`, `post_id`, `created_at_ms`).
3. **Automated Client Error Handling via Status Codes:** Standardized status code categories (`2xx` Success, `3xx` Redirect, `4xx` Client Error, `5xx` Server Error) allow API gateways, CDNs, and frontend SDKs to know automatically whether a request succeeded (`200`, `201`, `204`), failed due to missing data (`404`), or requires a server retry (`503`).

### 3. How It Works in PulseStream
* **`POST` (`201 Created`):** Calls `controller.create_comment()` or `controller.create_reply()`, minting a new 64-bit Snowflake ID on every execution.
* **`GET` (`200 OK` / `404 Not Found`):** Safe, read-only, idempotent retrieval of single comments or paginated lists.
* **`PUT` (`200 OK` / `404 Not Found`):** Calls `controller.replace_comment()`, overwriting mutable fields in place while keeping `comment_id`, `post_id`, `parent_comment_id`, and `created_at_ms` unchanged.
* **`PATCH` (`200 OK` / `404 Not Found`):** Calls `controller.patch_comment()`, applying `payload.model_dump(exclude_unset=True)` so omitted attributes remain untouched.
* **`DELETE` (`204 No Content` / `404 Not Found`):** Pops the target `comment_id` from primary storage and returns an empty `204` response body.

### 4. Comparison of REST HTTP Methods

| HTTP Method | Idempotent | Safe (Read-Only) | Success Status | State Behavior on Repeat Calls |
| :--- | :--- | :--- | :--- | :--- |
| `GET` | Yes | Yes | `200 OK` | Returns resource without mutation |
| `POST` | **No** | No | `201 Created` | Mints a brand-new resource every call |
| **`PUT`** | **Yes** | **No** | **`200 OK`** | **Replaces full document identically** |
| **`PATCH`** | **Yes** | **No** | **`200 OK`** | **Applies partial fields identically** |
| `DELETE` | Yes (State) | No | `204 No Content` | Resource remains absent (`404` on retry) |

Enforcing idempotency on `GET`, `PUT`, `PATCH`, and `DELETE` allows load balancers and client libraries to safely retry failed network requests without risking duplicate creations.

### 5. Problem & Solution Summary
* **Problem:** Unreliable client networks cause duplicate form submissions and retry storms, while using generic `POST` routes for updates prevents safe automatic retries.
* **Solution:** Map resource creation to `POST` (`201`), full replacement to idempotent `PUT` (`200`), partial updates to idempotent `PATCH` (`200`), and removals to `DELETE` (`204`), verified by automated retry assertions in `tests/test_idempotency.py`.

### 6. Real-World Applications
* **Stripe Payment & Billing APIs:** Rely on strict HTTP idempotency so network timeouts never double-update customer subscriptions.
* **Kubernetes API Server:** Uses declarative idempotent `PUT` and `PATCH` operations so controllers can continuously reconcile cluster state without creating duplicate pods.