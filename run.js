import cluster from "cluster";
import env from "../../env.js";

export const run = async () => {
    if (env.jobs?.worker) {
        const URL = (await import("url")).URL;
        cluster.setupPrimary({
            exec: new URL("./index.js", import.meta.url).pathname,
        });
        cluster.fork();
        cluster.setupPrimary({
            exec: new URL("../../index.js", import.meta.url).pathname,
        });
    } else {
        (await import("./index.js")).default;
    }
};
