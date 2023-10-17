export type Config = {
    redisUrl: string;
    redisPrefix?: string;
}

export type CreateParallelWatcher = {
    ttl: number;
    count: number;
    type: string;
}


export type TaskResult<T> = {
    groupId: string;
    taskId: string;
    type: string;
    result: T;
}

export type WatcherCallback<T> = (res: TaskResult<T>[]) => void