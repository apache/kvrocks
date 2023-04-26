const {createClient} = require('redis');
const express = require('express');
const bodyParser = require('body-parser');
const { waitUntil } = require('./util');
const path = require('path')
const app = express();
const client = createClient({
    socket: {
        port: 6666,
        host: '127.0.0.1',
        reconnectStrategy: 1000,
    }
});
client.on('error', err => console.error('redis client error', err));
client.connect().then(console.log('db connected'));

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

app.use(bodyParser.json());

app.get('/api/all', function (req, res) {
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

app.get('/api/allKeys', function(req, res) {
    apiWrapper(async () => {
        const keys = await client.keys('*');
        res.send(keys);
    }, ...arguments)
})

app.post('/api/create', function(req, res) {
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

app.put('/api/update', function(req, res) {
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

app.delete('/api/delete', function(req, res) {
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

app.get('/api/info', function (req, res) {
    apiWrapper(async () => {
        let rawInfo = await client.info();
        const result = {};
        rawInfo.split('\r\n\r\n# ').forEach(infoSection => {
            const section = infoSection.split('\r\n');
            const sectionKey = section[0].startsWith('# ') ? section[0].replace('# ', '') : section[0];
            const resultItem = {};
            section.filter(i => !i.startsWith('# ') && i.includes(':')).forEach(i => {
                const kv = i.split(':');
                resultItem[kv[0]] = kv[1];
            })
            result[sectionKey] = resultItem;
        })
        res.send(result);
    }, ...arguments)
})

app.use('/', express.static(path.join(__dirname, './website')))

app.use((req, res) => {
    res.sendFile(path.join(__dirname, './website/index.html'))
})

const port = process.env.WEB_PORT || 6677;
app.listen(port, () => console.log(`website on ${port}`));