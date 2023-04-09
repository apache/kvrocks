async function sleep(time_ms){
    return new Promise(res => {
        setInterval(() => {
            res()
        }, time_ms);
    })
}

async function waitUntil(condition, interval, timeout) {
    return new Promise((res, rej) => {
        async function turn(){
            let result;
            try {
                result = await condition();
            } catch (err) {
                rej();
                return;
            }
            if(result){
                res();
            } else{
                setTimeout(turn, interval);
            }
        }
        turn();
        setTimeout(() => {
            rej();
        }, timeout);
    })
}

module.exports = {
    sleep,
    waitUntil
}