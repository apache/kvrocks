import { Form, Input, InputNumber, Radio, Space, Switch } from 'antd';
import { KvRow } from './entity';
import { useEffect, useState } from 'react';

export function RecordCreation (prop:{record: KvRow}) {
    const [type, setType] = useState<KvRow['type']>(prop.record.type);
    useEffect(() => {prop.record.type = type;}, [type]);
    const [key, setKey] = useState<KvRow['key']>(prop.record.key);
    useEffect(() => {prop.record.key = key;}, [key]);
    const [value, setValue] = useState<KvRow['rawValue']>(prop.record.rawValue);
    useEffect(() => {prop.record.rawValue=value;}, [value]);
    const [ttl, setTtl] = useState<KvRow['ttl']>(prop.record.ttl);
    useEffect(() => {prop.record.ttl=ttl;}, [ttl]);
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