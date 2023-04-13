export async function sleep(time_ms: number) {
    return new Promise<void>(res => {
        setInterval(() => {
            res();
        }, time_ms);
    });
}

export function getRandomString(length: number):string {
    const dict = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'.split('');
    let result = '';
    for (let i = 0; i < length; i++) {
        result += dict[Math.floor(dict.length*Math.random())];
    }
    return result;
}