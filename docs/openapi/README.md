# Admin API: OpenAPI and Swagger UI

[`admin-v1.yaml`](admin-v1.yaml) is the OpenAPI 3.1 contract for the `/v1`
operational API in EmitLane v0.9.1. [`index.html`](index.html) renders it with
Swagger UI 5.33.1. The [operator guide](../ADMIN_API.md) explains authentication,
redaction, retry, replay and audit semantics.

## View locally

From the repository root:

```bash
python3 -m http.server 8083 --bind 127.0.0.1 --directory docs
```

Open [Swagger UI](http://127.0.0.1:8083/openapi/). Serve over HTTP rather than
opening the HTML through `file://`, so the browser can fetch the adjacent YAML.
This only serves documentation; no Relay or database is needed to browse it.
The CLI does not host Swagger UI or the schema on its Admin API listener.

The viewer loads version-pinned CSS/JavaScript from jsDelivr and requires
internet access. For an offline or restricted deployment, vendor those two
`swagger-ui-dist` assets and change the HTML URLs. The schema is loaded locally;
the external Swagger validator is disabled. Request execution and authorization
persistence are disabled in this viewer.

## Use the API

Import the YAML into an API client for requests. Standalone defaults to
`http://127.0.0.1:8081/v1` when enabled. The Compose example uses port 8082 with
its development bearer token; port 8081 belongs to the Orders example.

```bash
curl -sS http://127.0.0.1:8082/v1/stats \
  -H 'Authorization: Bearer emitlane-local-admin'
```

A documentation server and Admin API have different origins. The API supplies
no browser CORS policy; enabling Swagger request execution alone would not make
cross-origin calls work. If you host an interactive viewer, provide an explicit
same-origin proxy and authenticated access in your deployment.

## Maintain the contract

Update the YAML with route, JSON field, status, parameter and redaction changes
in [`internal/admin`](../../internal/admin/). Keep `info.version` aligned with
the documented product snapshot; it is independent of the `/v1` route prefix.
Keep operation IDs unique and update both the success and error responses.

Swagger configuration references:
[installation](https://swagger.io/docs/open-source-tools/swagger-ui/usage/installation/)
and [configuration](https://swagger.io/docs/open-source-tools/swagger-ui/usage/configuration/).
