const {createClient} = require('redis');
const express = require('express');
const bodyParser = require('body-parser');
const { waitUntil } = require('./util');
const app = express();
app.use(bodyParser.json());
const client = createClient({
    socket: {
        port: 6666,
        host: '127.0.0.1'
    }
});
client.on('error', err => console.log('redis client error', err));
client.connect().then(console.log('connected'));

async function runTest(){
    // string
    await client.set('kstr','kkk');
    // list
    await client.del('klist')
    await client.lPush('klist', 'abc');
    await client.rPush('klist', 'xyz');
    // hash
    await client.hSet('khset', 'k1', 'v1');
    await client.hSet('khset', 'k2', 'v2');
    // set
    await client.sAdd('kset', ['aa','bb','cc']);
    await client.sAdd('kset2', ['xx','yy','cc', 'zz']);

    const allKeys = await client.KEYS('*');
    console.log(allKeys);
    for (let i = 0; i < allKeys.length; i++) {
        const key = allKeys[i];
        const type = await client.type(key);
        let value = null;
        switch (type) {
            case 'string':
                value = await client.get(key);
                break;
            case 'list':
                value = await client.lRange(key, 0, -1);
                break;
            case 'hash':
                value = await client.hGetAll(key);
                break;
            case 'set':
                value = await client.sMembers(key);
                break;    
            default:
                value = 'unknown'
                break;
        }
        console.log(key, type, value)
    }
    console.log('inter',await client.sInter(['kset', 'kset2']));
    console.log('union',await client.sUnion(['kset', 'kset2']));
    console.log('diff',await client.sDiff(['kset', 'kset2']));
}
// runTest();

async function apiWrapper(cb, req, res, next) {
    await waitUntil(() => client.isReady, 100, 30 * 1000);
    try {
        await cb();
    } catch (err) {
        let errMsg = '';
        if(typeof err == 'string'){
            errMsg = err;
        }else if(err instanceof Error){
            errMsg = err.message;
        } else {
            errMsg = 'Unkwon error';
        }
        res.status(500).send(errMsg)
    }
}

app.all('*', function(req, res, next){
    res.header('Access-Control-Allow-Origin', '*');  
    res.header('Access-Control-Allow-Headers', '*');  
    res.header('Access-Control-Allow-Methods', 'PUT, POST, GET, DELETE, OPTIONS');
    next();
})

app.get('/all', function (req, res) {
    apiWrapper(async () => {
        const allKeys = await client.keys('*');
        let start = parseInt(req.query.from);
        let end = parseInt(req.query.to);
        if(isNaN(start)){
            start = 0;
        }
        if(isNaN(end)){
            end = allKeys.length;
        }
        const keys = allKeys.slice(start, end + 1);
        const result = [];
        for (let i = 0; i < keys.length; i++) {
            const key = keys[i];
            const ttl = await client.ttl(key);
            const type = await client.type(key);
            let value = null;
            switch (type) {
                case 'string':
                    value = await client.get(key);
                    break;
                case 'list':
                    value = await client.lRange(key, 0, -1);
                    break;
                case 'hash':
                    value = await client.hGetAll(key);
                    break;
                case 'set':
                    value = await client.sMembers(key);
                    break;    
                default:
                    value = 'unknown'
                    break;
            }
            result.push({
                key,
                type,
                value,
                ttl,
            })
        }
        res.send({
            data: result,
            totalCount: allKeys.length
        });
    }, ...arguments)
})

app.get('/allKeys', function(req, res) {
    apiWrapper(async () => {
        const keys = await client.keys('*');
        res.send(keys);
    }, ...arguments)
})

app.post('/create', function(req, res) {
    apiWrapper(async () => {
        const body = req.body;
        if(typeof body !== 'object'){
            throw 'No body'
        }
        const key = body['key']
        if(typeof key !== 'string' || key === ''){
            throw 'No key';
        }
        const relatedKeys = await client.keys(`${key}*`);
        if(Array.isArray(relatedKeys) && relatedKeys.includes(key)){
            throw 'Duplicate key'
        }
        const type = body['type'];
        const value = body['value'];
        if(typeof type !== 'string' || !['string', 'list', 'hash', 'set'].includes(type)){
            throw 'Unknown type'
        }
        if(type == 'string' && typeof value == 'string'){
            // create string
            await client.set(key, value);
        } else if(type == 'list' && Array.isArray(value)){
            // create list
            await client.rPush(key, value);
        } else if(type == 'hash' && typeof value === 'object' && !Array.isArray(value)){
            // create hash
        } else if(type == 'set' && Array.isArray(value)){
            // create set
            await client.sAdd(key, value);
        } else {
            throw 'Bad request'
        }
        if('ttl' in body && typeof body['ttl'] === 'number' && body['ttl'] > 0){
            await client.expire(key, body['ttl'])
        }
        res.send('OK');
    }, ...arguments)
})

app.listen(8888, () => console.log('api on 8888'))