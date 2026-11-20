import type { SourceEvent } from "./generated/SourceEvent.js";

/** Only provider resource identities, never a shared homepage, merge thread work. */
export function sourceWorkResource(value: string): string | undefined {
  let url: URL; try { url = new URL(value); } catch { return; }
  if (url.protocol !== "https:" || url.username || url.password) return;
  if (["fedex.com", "www.fedex.com"].includes(url.hostname) && /^\/fedextrack\/?$/u.test(url.pathname)) {
    const tracking = url.searchParams.get("trknbr");
    if (tracking !== null && /^(?:\d{12}|\d{15}|\d{20}|\d{22})$/u.test(tracking)) return `shipment:fedex:${tracking}`;
    return;
  }
  const match = url.hostname === "github.com" ? /^\/([^/]+)\/([^/]+)\/(?:issues|pull)\/(\d+)(?:\/|$)/u.exec(url.pathname)
    : url.hostname === "api.github.com" ? /^\/repos\/([^/]+)\/([^/]+)\/(?:issues|pulls)\/(\d+)(?:\/|$)/u.exec(url.pathname) : null;
  return match === null ? undefined : `github:${match[1]!.toLowerCase()}/${match[2]!.toLowerCase()}:${match[3]}`;
}

export function sourceWorkResources(event: SourceEvent): string[] {
  return [...new Set([event.source.thread_id, ...(event.text.match(/https:\/\/[^\s<>"'|)]+/gu) ?? [])]
    .flatMap(value => { const resource = sourceWorkResource(value); return resource === undefined ? [] : [resource]; }))];
}

/** GitHub notifications normalize this provider-owned field separately from
 * their subject text, which may mention other unrelated issues. */
export function legacySourceWorkResources(event: SourceEvent): string[] {
  if (event.source.provider !== "github") return [];
  const subject = /^Subject API: (https:\/\/\S+)\s*$/mu.exec(event.text)?.[1];
  const resource = subject === undefined ? undefined : sourceWorkResource(subject);
  return resource === undefined ? [] : [resource];
}
