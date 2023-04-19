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
client.on('error', err => console.error('redis client error', err));
client.connect().then(console.log('connected'));

async function apiWrapper(cb, req, res, next) {
    await waitUntil(() => client.isReady, 100, 30 * 1000);
    try {
        await cb();
    } catch (err) {
        console.error(err);
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
                case 'zset':{
                    value = {};
                    const arr = await client.zRangeWithScores(key, 0, -1);
                    arr.forEach(item => value[item.value] = item.score)
                    break;
                }
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
        if(typeof type !== 'string' || !['string', 'list', 'hash', 'set', 'zset'].includes(type)){
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
            for (const field in value) {
                if (Object.hasOwnProperty.call(value, field)) {
                    await client.hSet(key, field, value[field]);
                }
            }
        } else if(type == 'set' && Array.isArray(value)){
            // create set
            await client.sAdd(key, value);
        } else if(type == 'zset' && typeof value === 'object' && !Array.isArray(value)) {
            // create zset
            const member = [];
            for (const field in value) {
                if (Object.hasOwnProperty.call(value, field) && typeof value[field] == 'number') {
                    member.push({
                        value: field,
                        score: value[field]
                    })
                }
            }
            await client.zAdd(key, member);
        } else {
            throw 'Bad request'
        }
        if('ttl' in body && typeof body['ttl'] === 'number' && body['ttl'] > 0){
            await client.expire(key, body['ttl'])
        }
        res.send('OK');
    }, ...arguments)
})

app.put('/update', function(req, res) {
    apiWrapper(async () => {
        const body = req.body;
        if(typeof body !== 'object'){
            throw 'No body'
        }
        const key = body['key']
        if(typeof key !== 'string' || key === ''){
            throw 'No key';
        }
        const type = body['type'];
        const value = body['value'];
        if(typeof type !== 'string' || !['string', 'list', 'hash', 'set', 'zset'].includes(type)){
            throw 'Unknown type'
        }
        if(type == 'string' && typeof value == 'string'){
            // update string
            await client.set(key, value);
        } else if(type == 'list' && Array.isArray(value)){
            // update list
            await client.del(key);
            await client.rPush(key, value);
        } else if(type == 'hash' && typeof value === 'object' && !Array.isArray(value)){
            // update hash
            await client.del(key);
            for (const field in value) {
                if (Object.hasOwnProperty.call(value, field)) {
                    await client.hSet(key, field, value[field]);
                }
            }
        } else if(type == 'set' && Array.isArray(value)){
            // update set
            await client.del(key);
            await client.sAdd(key, value);
        } else if(type == 'zset' && typeof value === 'object' && !Array.isArray(value)) {
            // update zset
            const member = [];
            for (const field in value) {
                if (Object.hasOwnProperty.call(value, field) && typeof value[field] == 'number') {
                    member.push({
                        value: field,
                        score: value[field]
                    })
                }
            }
            await client.del(key);
            await client.zAdd(key, member);
        } else {
            throw 'Bad request'
        }
        if('ttl' in body && typeof body['ttl'] === 'number' && body['ttl'] > 0){
            await client.expire(key, body['ttl'])
        }
        res.send('OK');
    }, ...arguments)
})

app.delete('/delete', function(req, res) {
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
            await client.del(key);
        }
        res.send('OK')
    }, ...arguments)
})

app.listen(8888, () => console.log('api on 8888'))