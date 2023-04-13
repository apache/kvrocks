import { Card, Form, Input, InputNumber, Radio, Space, Switch } from 'antd';
import { KvRow } from './entity';
import { useEffect, useState } from 'react';
import { ListEditor } from '../../components/ListEditor';
import { HashRow, ListRow, SetRow, StringRow } from '../../types/types';

function useBinding(record: KvRow) {
    const [type, _setType] = useState<KvRow['type']>(record.type);
    useEffect(() => {record.type = type;}, [type]);
    const [key, setKey] = useState<KvRow['key']>(record.key);
    useEffect(() => {record.key = key;}, [key]);
    const [value, setValue] = useState<KvRow['rawValue']>(record.rawValue);
    useEffect(() => {
        record.rawValue=value;
        switch (type) {
        case 'string':
            setCachedValueForString(value as StringRow['value']);
            break;
        case 'list':
            setCachedValueForList(value as ListRow['value']);
            break;
        case 'hash':
            setCachedValueForHash(value as HashRow['value']);
            break;
        case 'set':
            setCachedValueForSet(value as SetRow['value']);
            break;
        default:
            break;
        }
    }, [value]);
    const [ttl, setTtl] = useState<KvRow['ttl']>(record.ttl);
    useEffect(() => {record.ttl=ttl;}, [ttl]);
    const [cachedValueForString, setCachedValueForString] = useState<StringRow['value']>(typeof value == 'string' ? value : '');
    const [cachedValueForList, setCachedValueForList] = useState<ListRow['value']>(Array.isArray(value)?value:['']);
    const [cachedValueForHash, setCachedValueForHash] = useState<HashRow['value']>(typeof value == 'object' && !Array.isArray(value) ? value : {});
    const [cachedValueForSet, setCachedValueForSet] = useState<SetRow['value']>(Array.isArray(value)?value:['']);
    function setType(v: KvRow['type']) {
        _setType(v);
        switch (v) {
        case 'string':
            setValue(cachedValueForString);
            break;
        case 'list':
            setValue(cachedValueForList);
            break;
        case 'hash':
            setValue(cachedValueForHash);
            break;
        case 'set':
            setValue(cachedValueForSet);
            break;
        default:
            break;
        }
    }
    return {
        type,
        setType,
        key, 
        setKey,
        value,
        setValue,
        ttl,
        setTtl,
    };
}

export function RecordCreation (prop:{record: KvRow}) {
    const {type, setType, key, setKey, value, setValue,ttl,setTtl} = useBinding(prop.record);
    return (
        <div>
            <Form
                layout='horizontal'
                labelCol={{span:3}}
                wrapperCol={{span:20}}
            >
                <Form.Item label="Type">
                    <Radio.Group value={type} onChange={e => setType(e.target.value)}>
                        <Radio.Button value='string'>string</Radio.Button>
                        <Radio.Button value='list'>list</Radio.Button>
                        <Radio.Button value='hash'>hash</Radio.Button>
                        <Radio.Button value='set'>set</Radio.Button>
                    </Radio.Group>
                </Form.Item>
                <Form.Item label="Key">
                    <Input placeholder='Input key...' value={key} onChange={e => setKey(e.target.value)}></Input>
                </Form.Item>
                <Form.Item label="Value">
                    {
                        (() => {
                            switch (type) {
                            case 'string':
                                return <Input placeholder='Input value...' value={value as string} onChange={e => setValue(e.target.value)}></Input>;
                            case 'list':
                                return (<Card key={'list'}>
                                    <ListEditor value={value as string[]} onChange={e => setValue(e)} allowDragSorting/>
                                </Card>
                                );
                            case 'set':
                                return (<Card key={'set'}>
                                    <ListEditor value={value as string[]} onChange={e => setValue(e)}/>
                                </Card>
                                );
                            default:
                                break;
                            }
                        })()
                    }
                </Form.Item>
                <Form.Item label="TTL">
                    <Space>
                        <Switch onChange={e => setTtl(e ? 0 : -1)}></Switch>
                        {
                            ttl != -1 &&
                            <>
                                <InputNumber min={0} value={ttl} onChange={e => e && setTtl(Math.round(e))}></InputNumber>
                                second(s)
                            </>
                        }
                    </Space>
                </Form.Item>
            </Form>
        </div>
    );
}