// ES modules style task handler
// (transpiled from: export default { task(event, env, ctx) { ... } })
globalThis.default = {
    task(event, env, ctx) {
        return {
            success: true,
            data: {
                style: 'es-modules',
                taskId: event.taskId
            }
        };
    }
};
