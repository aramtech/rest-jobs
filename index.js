// @ts-nocheck
import * as prisma from "$/prisma/client/index.js";

const log_util = await import("$/server/utils/log/index.js");
const log = await log_util.local_log_decorator("JOB", "green", true, "info", true);

const client = (await import("../../database/prisma.ts")).default;
const cron_parser = (await import("cron-parser")).default;

const env = (await import("$/server/env.js")).default;
const handlers = (await import("$/server/jobs/handlers.js")).default;

/**
 * @typedef {string} FunctionString
 */

/**
 * @typedef {string} JobHandlerName
 */
/**
 *
 * @typedef {prisma.job_handler_type} JobHandlerType
 */
/**
 *
 * @typedef {string} CronString
 */

/**
 * @typedef {Object} Job
 * @property {CronString} cron_schedule
 * @property {JobHandlerType} handler_type
 * @property {string} title
 * @property {Number} repeat_count
 * @property {JobHandlerName|FunctionString} handler
 *
 *
 */

/**
 *
 * @param {Job} job
 * @param {import("$/server/utils/express/index.ts").Req} request
 * @returns {Promise<void>}
 */
async function insert_job(job, request, b64_args = true) {
    const cron = cron_parser.parseExpression(job.cron_schedule);
    const next_date = cron.next().toDate();

    await client.jobs.create({
        data: {
            created_at: new Date(),
            created_by_user: !request?.user?.user_id
                ? undefined
                : {
                      connect: {
                          user_id: request?.user?.user_id,
                      },
                  },
            created_by_username: request?.user?.full_name,
            created_by_user_full_name: request?.user?.username,
            updated_at: new Date(),
            updated_by_user: !request?.user?.user_id
                ? undefined
                : {
                      connect: {
                          user_id: request?.user?.user_id,
                      },
                  },
            updated_by_user_full_name: request?.user?.full_name,
            updated_by_user_username: request?.user?.username,

            deleted: false,

            designated_date: next_date,
            handler: job.handler,
            argument_json: b64_args ? job.argument_json : Buffer.from(JSON.stringify(job.argument_json || [])).toString("base64"),
            title: job.title,
            status: "PENDING",
            cron_schedule: job.cron_schedule,
            handler_type: job.handler_type,
        },
    });
    return;
}

async function schedule_next(job) {
    if (job.repeat_count == -1) {
        await insert_job(job);
    } else if (job.repeat_count > 1) {
        job.repeat_count = parseInt(job.repeat_count) - 1;
        await insert_job(job);
    }
}
/**
 *
 * @param {Job} job
 */
async function runner(job) {
    try {
        log("Running Job", job.job_id, job.title);
        !!job.cron_schedule && (await schedule_next(job));
        await client.jobs.update({
            where: {
                job_id: job.job_id,
            },
            data: {
                status: "DONE",
            },
        });
        try {
            let output = "";
            if (job.handler_type == "FUNCTION_STRING") {
                const handler = eval(job.handler);
                const args = JSON.parse(Buffer.from(job.argument_json, "base64").toString("utf-8"));
                output = await handler(...args);
            } else {
                const handler = handlers[job.handler];
                if (!handler) {
                    throw {
                        msg: "Handler not found",
                        handler: job.handler,
                        title: job.title,
                        schedule: job.cron_schedule,
                    };
                }
                const args = !job.argument_json ? [] : JSON.parse(Buffer.from(job.argument_json, "base64").toString("utf-8"));

                output = await handler(...args);
            }
            await client.jobs.update({
                where: {
                    job_id: job.job_id,
                },
                data: {
                    output: Buffer.from(JSON.stringify(output || {})).toString("base64"),
                },
            });
        } catch (error) {
            console.log(error);
            log(error);
            await client.jobs.update({
                where: {
                    job_id: job.job_id,
                },
                data: {
                    status: "FAILED",
                    output: Buffer.from(JSON.stringify(error || {})).toString("base64"),
                },
            });
        }
    } catch (error) {
        console.log(error);
        log(error);
    }
}

async function maintain() {
    for (const job of env.jobs?.initial_jobs || []) {
        if (job.repeat_count != -1) {
            continue;
        }

        const scheduled_job = await client.jobs.findFirst({
            where: {
                designated_date: {
                    gt: new Date(),
                },
                title: job.title,
            },
        });
        if (!scheduled_job) {
            await insert_job(job, null, false);
        }
    }
}

async function loop() {
    try {
        log("running jobs");
        const jobs = await client.jobs.findMany({
            where: {
                status: "PENDING",
                deleted: false,
                designated_date: {
                    lte: new Date(),
                },
            },
        });

        for (const job of jobs) {
            await runner(job);
        }

        await maintain();
    } catch (error) {
        console.log(error);
        log(error);
    }
    setTimeout(loop, env.jobs.job_checkout_interval_ms);
}

loop();
