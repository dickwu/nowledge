# POST /v1/ingest/uploads:sync

Synchronously ingest a multipart file upload and return the completed `IngestTaskResult`.

## Multipart Fields

Same fields as `POST /v1/ingest/uploads`.

## Response

Completed `IngestTaskResult`.

## Rules

- Text files can use `parser_provider=builtin`.
- PDF/DOCX/PPTX/XLSX/image bytes should use `parser_provider=mineru`.
- Invalid MIME strings are rejected before parser dispatch.

```mermaid
flowchart TD
  n1["Read multipart file and metadata"]
  n2["Create task record"]
  n3["Run parser synchronously"]
  n4["Fragment and index"]
  n5["Return completed result"]
  n1 --> n2 --> n3 --> n4 --> n5
```
