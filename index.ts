import { Config, CreateParallelWatcher, WatcherCallback } from "./types/config";
import { v4 as uuidv4 } from 'uuid';
import { RedisClientType, RedisFunctions, RedisModules, RedisScripts, createClient } from 'redis';

/**
 * A class that provides functionality for creating and watching parallel tasks.
 */
class ParallelWatcher {
    /**
     * The configuration object for the ParallelWatcher.
     */
    private static _config: Config;

    /**
     * The Redis client instance used for communication with Redis server.
     */
    private static redis: RedisClientType<RedisModules, RedisFunctions, RedisScripts>

    /**
     * An array of callbacks to be executed when a parallel tasks is completed.
     */
    private static callbacks: [string, WatcherCallback<any>][] = [];

    /**
     * Returns the configuration object for the ParallelWatcher.
     * @throws Error if the configuration object is not set.
     */
    private static get config() {
        if (!this._config) {
            throw new Error("Config is not set");
        }
        return this._config;
    }

    /**
     * Sets up the ParallelWatcher with the provided configuration.
     * @param config The configuration object for the ParallelWatcher.
     */
    public static async setup(config: Config) {
        if (!config.redisPrefix) {
            config.redisPrefix = "parallel-watcher"
        }

        this._config = config;
        this.redis = createClient({
            url: config.redisUrl
        });

        await this.redis.connect();
    }

    /**
     * Adds a callback to be executed when a parallel tasks is completed.
     * @param type The type of the parallel task.
     * @param callback The callback function to be executed.
     */
    public static on<T>(type: string, callback: WatcherCallback<T>) {
        this.callbacks.push([type, callback])
    }

    /**
     * Creates a new parallel task with the provided data.
     * @param data The data for the parallel task.
     * @returns The ID of the created parallel task.
     */
    public static async createParallel(data: CreateParallelWatcher): Promise<string> {
        const groupId = uuidv4()

        await this.redis.hSet(`${this.config.redisPrefix}:${groupId}`, "count", data.count)
        await this.redis.hSet(`${this.config.redisPrefix}:${groupId}`, "type", data.type)

        await this.redis.expire(`${this.config.redisPrefix}:${groupId}`, data.ttl)

        return groupId
    }

    /**
     * Confirms the completion of a parallel task with the provided data.
     * @param groupId The ID of the parallel task.
     * @param data The data for the completed task.
     * @param taskId The ID of the completed task.
     */
    public static async confirmParallel(groupId: string, data: any, taskId?: string) {
        if (!taskId)
            taskId = uuidv4()
        await this.redis.hSetNX(`${this.config.redisPrefix}:${groupId}`, taskId, data)
    }

    /**
     * Watches for completed parallel tasks and executes the corresponding callbacks.
     */
    public static async watcher() {
        while (true) {

            const keys = await this.redis.keys(`${this.config.redisPrefix}:*`)

            for (let i = 0; i < keys.length; i++) {
                const groupId = keys[i];
                const res = await this.redis.HGETALL(groupId) as any
                const count = parseInt(res.count);
                const type = res.type;

                if(type === undefined || count === undefined){
                    await this.redis.del(groupId)
                    continue;
                }

                const objectKeys = Object.keys(res).length - 2
                if (objectKeys >= count) {
                    this.callbacks.filter(([type, callback]) => type === type).forEach(([type, callback]) => {
                        const mapped = Object.keys(res).filter(key => key !== "count" && key !== "type").map(key => {
                            return {
                                groupId: groupId,
                                type: type,
                                taskId: key,
                                result: res[key]
                            }
                        })

                        callback(mapped)
                        this.redis.del(groupId)
                    })
                }
            }

            await new Promise((resolve) => setTimeout(resolve, 1000));
        }
    }
}

export default ParallelWatcher;