import { OperationsClient } from "../api/client";

export const client = new OperationsClient();

export function requestHeaders(): HeadersInit {
  const id = crypto.randomUUID();
  return {
    "content-type": "application/json",
    "x-correlation-id": id,
    "x-idempotency-key": id,
  };
}
