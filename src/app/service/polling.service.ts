import { inject, Injectable } from '@angular/core';
import { BehaviorSubject, filter, finalize, firstValueFrom, Observable } from 'rxjs';
import { NotificationType } from '../../models';
import { NotificationService } from '../notification/notification.service';

/**
 * Poll object structure for managing polling state and logic.
 * @template T - Type of the polled data.
 */
type Poll<T> = {
  poll: {
    interval: number;
    close: (result: T) => boolean;
    pollingFN: BehaviorSubject<T>;
    globalNotification?: string;
  };
};

@Injectable({
  providedIn: 'root'
})
export class PollingService {
  private readonly maxOffset: number = 30;
  private readonly maxError: number = 3;

  private readonly notificationService: NotificationService = inject(NotificationService);

  /**
   * Stores active polls by key.
   */
  private actual_polls: Map<string, Poll<unknown>> = new Map();

  /**
   * Tracks the number of observers per poll key.
   */
  private observers: Map<string, number> = new Map();

  /**
   * Emits when a poll finishes.
   */
  // private notification_finish_poll: Subject<void> = new Subject();

  /**
   * Observable that emits when a poll finishes.
   */
  // public get notificationFinishPoll(): Observable<void> {
  //   return this.notification_finish_poll;
  // }

  /**
   * Returns an observable for the polling result of a given key.
   * Increases the observer count for the poll.
   * @template T
   * @param key Unique identifier for the poll.
   * @returns Observable emitting polled values.
   */
  public getPolling<T>(key: string): Observable<T> {
    this.observers.set(key, (this.observers.get(key) || 0) + 1);
    return (this.actual_polls.get(key)?.poll.pollingFN as Observable<T>)?.pipe(
      filter((result: T) => result !== null && result !== undefined),
      finalize(() => {
        const observers: number = this.observers.get(key) || 0;
        if (observers > 0) {
          this.observers.set(key, observers - 1);
        }
      })
    );
  }

  /**
   * Checks if a poll exists for the given key.
   * @param key Unique identifier for the poll.
   * @returns True if the poll exists, false otherwise.
   */
  public hashPolling(key: string): boolean {
    return this.actual_polls.has(key);
  }

  public createPollFactory<T>(
    key: string,
    interval: number,
    close: (result: T) => boolean,
    llamadaFn: () => Observable<T>,
    globalNotification?: string
  ): Observable<T> {
    const poll$: BehaviorSubject<T> = new BehaviorSubject<T>(null as unknown as T);
    const poll: Poll<T> = {
      poll: {
        interval,
        close,
        pollingFN: poll$,
        globalNotification
      }
    };
    this.actual_polls.set(key, poll as Poll<unknown>);
    const pollObservable: Observable<T> = this.getPolling<T>(key);
    let active: boolean = true;

    const pollAsync = async (): Promise<void> => {
      let offset: number = 0;
      let pollingError: number = 0;
      while (active) {
        try {
          if (this.actual_polls.get(key) === undefined) {
            active = false;
            break;
          }
          offset = offset + 1;

          const result: T = await firstValueFrom(llamadaFn());

          if (result) {
            poll$.next(result);
            if (close(result) || offset >= this.maxOffset) {
              const observers: number = this.observers.get(key) || 0;

              if (observers === 0) {
                // this.notification_finish_poll.next();
                if (globalNotification) {
                  this.notificationService.showNotification({
                    detail: globalNotification,
                    severity: NotificationType.SUCCESS
                  });
                }
              }
              this.actual_polls.delete(key);
              break;
            }
          }
        } catch (_e) {
          console.debug('Error en poll', key, _e);
          if (++pollingError >= this.maxError) {
            this.actual_polls.delete(key);
            active = false;
            break;
          }
        }
        await new Promise<void>((res: any) => setTimeout(res, interval));
      }
    };
    pollAsync();
    return pollObservable;
  }

  /**
   * Deletes a poll by its key.
   * @param key Unique identifier for the poll.
   */
  public deletePoll(key: string): void {
    this.actual_polls.delete(key);
  }
}
