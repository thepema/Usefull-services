import { Injectable } from '@angular/core';
import { Observable, Subscriber } from 'rxjs';

type SseEventInit = {
  event: 'init';
  data: 'connected';
};

export type SseEvent<K> = SseEventInit | { id: number; event: string; data: K };

export type SseFrame = {
  eventType: string;
  dataLines: string[];
  data?: string;
  id: string;
  retry?: number;
};

type ParserResult = {
  feed: (chunkText: string) => void;
};

function createSseParser(
  parserType: string,
  onEvent: (e: SseFrame) => void,
  onRetry: (ms: number) => void
): { feed: (chunkText: string) => void } {
  let buffer = '';

  let eventType = 'message';
  let dataLines: string[] = [];
  let id = '';
  let retry: number | undefined;

  const dispatch = (): void => {
    if (dataLines.length === 0 && eventType === 'message' && id === '' && retry == null) return;

    const frame: SseFrame = { eventType, dataLines, id, retry };
    if (retry != null) onRetry(retry);

    if (dataLines.length > 0) {
      const dataText = frame.dataLines.join('\n');

      frame.data = dataText;
      try {
        if (frame.eventType === parserType) {
          frame.data = JSON.parse(dataText);
        }
      } catch {
        throw new Error(`Failed to parse SSE data as JSON: ${dataText}`);
      }
      onEvent(frame);
    }

    eventType = 'message';
    dataLines = [];
    retry = undefined;
  };

  const feed = (chunkText: string): void => {
    buffer += chunkText;
    let idx: number;
    while ((idx = buffer.indexOf('\n')) !== -1) {
      let line = buffer.slice(0, idx);
      buffer = buffer.slice(idx + 1);

      if (line.endsWith('\r')) line = line.slice(0, -1);

      if (line === '') {
        dispatch();
        continue;
      }

      if (line.startsWith(':')) continue;

      const colon = line.indexOf(':');
      let field = '';
      let value = '';

      if (colon === -1) {
        field = line;
        value = '';
      } else {
        field = line.slice(0, colon);
        value = line.slice(colon + 1);
        if (value.startsWith(' ')) value = value.slice(1);
      }

      switch (field) {
        case 'event':
          eventType = value || 'message';
          break;
        case 'data':
          dataLines.push(value);
          break;
        case 'id':
          id = value;
          break;
        case 'retry': {
          const ms = Number(value);
          if (Number.isFinite(ms) && ms >= 0) retry = ms;
          break;
        }
      }
    }
  };

  return { feed };
}

@Injectable({
  providedIn: 'root'
})
export class SseService {
  connectSSE<K>(
    url: string,
    parserType: string,
    customHeaders?: Record<string, string>
  ): Observable<SseEvent<K>> {
    const initialRetryMs = 1000;
    const maxRetryMs = 30000;
    const maxRetryAttempts = 10;
    let retryAttempts = 0;

    return new Observable<SseEvent<K>>((subscriber: Subscriber<SseEvent<K>>) => {
      const abort = new AbortController();

      let lastEventId = '';
      let retryMs = initialRetryMs;
      let stopped = false;

      const parser: ParserResult = createSseParser(
        parserType,
        (frame: SseFrame) => {
          if (frame.id) lastEventId = frame.id;

          if (frame.eventType === parserType) {
            subscriber.next({
              id: frame.id ? Number(frame.id) : 0,
              event: frame.eventType,
              data: frame.data as K
            });
          }
        },
        (serverRetry: number) => {
          retryMs = Math.min(serverRetry, maxRetryMs);
        }
      );

      const reqHeaders: Record<string, string> = {
        Accept: 'text/event-stream'
      };
      if (customHeaders) Object.assign(reqHeaders, customHeaders);

      const run = async (): Promise<void> => {
        while (!stopped && !abort.signal.aborted) {
          try {
            if (lastEventId) reqHeaders['Last-Event-ID'] = lastEventId;

            const resp: Response = await fetch(url, {
              method: 'GET',
              headers: reqHeaders,
              credentials: 'same-origin',
              signal: abort.signal
            });

            if (!resp.ok) {
              retryAttempts++;
              throw new Error(`SSE HTTP error ${resp.status}`);
            } else {
              retryAttempts = 0;
            }

            retryMs = initialRetryMs;

            const reader = resp.body?.getReader();
            if (!reader) throw new Error('ReadableStream not supported or empty body');

            const decoder = new TextDecoder('utf-8');

            while (!stopped && !abort.signal.aborted) {
              const { value, done }: { value?: Uint8Array; done: boolean } = await reader.read();
              if (done) {
                stopped = true;
                abort.abort();
                break;
              }

              const text = decoder.decode(value, { stream: true });
              parser.feed(text);
            }
          } catch {
            retryAttempts++;

            if (!stopped && !abort.signal.aborted) {
              await new Promise<void>((res) => setTimeout(res, retryMs));
              retryMs = Math.min(retryMs * 2, maxRetryMs);
            }

            if (retryAttempts >= maxRetryAttempts) {
              break;
            }
            continue;
          }
        }
      };

      void run();

      return () => {
        console.log('SSE connection closed by subscriber');
        stopped = true;
        abort.abort();
      };
    });
  }
}
