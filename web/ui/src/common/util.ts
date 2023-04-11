export async function sleep(time_ms: number) {
    return new Promise<void>(res => {
        setInterval(() => {
            res();
        }, time_ms);
    });
}
