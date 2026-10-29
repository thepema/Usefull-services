import { inject, Injectable } from '@angular/core';
import { BehaviorSubject, EMPTY, filter, finalize, firstValueFrom, Observable, Subject, takeUntil } from 'rxjs';
import { NotificationType } from '../../models';
import { NotificationService } from '../notification/notification.service';

type PollState<T> = {
  subject: BehaviorSubject<T>;
  stop$: Subject<void>;
  interval: number;
  close: (result: T) => boolean;
  globalNotification?: string;
  fn: () => Observable<T>;
  active: boolean;
};

@Injectable({
  providedIn: 'root'
})
export class PollingSafeService {
  private readonly maxOffset: number = 30;
  private readonly maxError: number = 3;

  private readonly notificationService: NotificationService = inject(NotificationService);

  private readonly actual_polls: Map<string, PollState<unknown>> = new Map();
  private readonly observers: Map<string, number> = new Map();

  public getPolling<T>(key: string): Observable<T> {
    const poll = this.actual_polls.get(key);
    if (!poll) {
      return EMPTY;
    }

    this.observers.set(key, (this.observers.get(key) || 0) + 1);

    return (poll.subject as BehaviorSubject<T>).asObservable().pipe(
      filter((result: T) => result !== null && result !== undefined),
      takeUntil(poll.stop$),
      finalize(() => {
        const observers = (this.observers.get(key) || 1) - 1;

        if (observers <= 0) {
          this.observers.delete(key);
          this.cleanupPoll(key);
          return;
        }

        this.observers.set(key, observers);
      })
    );
  }

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
    if (this.actual_polls.has(key)) {
      this.cleanupPoll(key);
    }

    const subject = new BehaviorSubject<T>(null as unknown as T);
    const stop$ = new Subject<void>();
    const state: PollState<T> = {
      subject,
      stop$,
      interval,
      close,
      globalNotification,
      fn: llamadaFn,
      active: true
    };

    this.actual_polls.set(key, state as PollState<unknown>);
    this.observers.set(key, 0);

    const pollObservable = this.getPolling<T>(key);
    this.startPollingLoop(key);

    return pollObservable;
  }

  public cancelPoll(key: string): void {
    this.cleanupPoll(key);
  }

  public deletePoll(key: string): void {
    this.cleanupPoll(key);
  }

  private startPollingLoop(key: string): void {
    const poll = this.actual_polls.get(key);
    if (!poll || !poll.active) {
      return;
    }

    void this.pollAsync(key);
  }

  private async pollAsync(key: string): Promise<void> {
    const poll = this.actual_polls.get(key);
    if (!poll) {
      return;
    }

    let offset = 0;
    let pollingError = 0;

    while (poll.active) {
      if (!this.actual_polls.has(key)) {
        return;
      }

      try {
        offset += 1;
        const result = await firstValueFrom(poll.fn());

        if (result !== null && result !== undefined) {
          (poll.subject as BehaviorSubject<unknown>).next(result);

          if (poll.close(result as never) || offset >= this.maxOffset) {
            this.finishPoll(key);
            return;
          }
        }
      } catch (_e) {
        console.debug('Error en poll', key, _e);
        if (++pollingError >= this.maxError) {
          this.finishPoll(key);
          return;
        }
      }

      await new Promise<void>((resolve) => setTimeout(resolve, poll.interval));
    }
  }

  private finishPoll(key: string): void {
    const poll = this.actual_polls.get(key);
    if (!poll) {
      return;
    }

    poll.active = false;
    poll.subject.complete();
    poll.stop$.next();
    poll.stop$.complete();

    const observers = this.observers.get(key) || 0;
    if (observers === 0 && poll.globalNotification) {
      this.notificationService.showNotification({
        detail: poll.globalNotification,
        severity: NotificationType.SUCCESS
      });
    }

    this.actual_polls.delete(key);
    this.observers.delete(key);
  }

  private cleanupPoll(key: string): void {
    const poll = this.actual_polls.get(key);
    if (!poll) {
      return;
    }

    poll.active = false;
    poll.subject.complete();
    poll.stop$.next();
    poll.stop$.complete();
    this.actual_polls.delete(key);
    this.observers.delete(key);
  }
}
