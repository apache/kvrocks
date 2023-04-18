import { Card, Form, Input, InputNumber, Radio, Space, Switch } from 'antd';
import { KvRow, KvRowAny } from './entity';
import { MutableRefObject, useCallback, useEffect, useImperativeHandle, useRef, useState } from 'react';
import { ListEditor } from '../../components/ListEditor';
import { RowDataAny, typeOfRow, valueOfRow, RowData } from '../../types/types';
import { HashEditor } from '../../components/HashEditor';

function useBinding<T extends typeOfRow>(record: KvRow<T>) {
    const [type, _setType] = useState<T>(record.type);
    useEffect(() => {record.type = type;}, [type]);
    const [key, setKey] = useState<RowDataAny['key']>(record.key);
    useEffect(() => {record.key = key;}, [key]);
    const [value, setValue] = useState<RowData<T>['value']>(record.value);
    useEffect(() => {
        record.value=value;
        switch (type) {
        case 'string':
            setCachedValueForString(value as valueOfRow<'string'>);
            break;
        case 'list':
            setCachedValueForList(value as valueOfRow<'list'>);
            break;
        case 'hash':
            setCachedValueForHash(value as valueOfRow<'hash'>);
            break;
        case 'set':
            setCachedValueForSet(value as valueOfRow<'set'>);
            break;
        default:
            break;
        }
    }, [value]);
    const [ttl, setTtl] = useState<RowDataAny['ttl']>(record.ttl);
    useEffect(() => {record.ttl=ttl;}, [ttl]);
    const [cachedValueForString, setCachedValueForString] = useState<valueOfRow<'string'>>(typeof value == 'string' ? value : '');
    const [cachedValueForList, setCachedValueForList] = useState<valueOfRow<'list'>>(Array.isArray(value)?value:[]);
    const [cachedValueForHash, setCachedValueForHash] = useState<valueOfRow<'hash'>>(typeof value == 'object' && !Array.isArray(value) ? value : {});
    const [cachedValueForSet, setCachedValueForSet] = useState<valueOfRow<'set'>>(Array.isArray(value)?value:[]);
    function setType(v: T) {
        _setType(v);
        switch (v) {
        case 'string':
            setValue(cachedValueForString as valueOfRow<T>);
            break;
        case 'list':
            setValue(cachedValueForList as valueOfRow<T>);
            break;
        case 'hash':
            setValue(cachedValueForHash as valueOfRow<T>);
            break;
        case 'set':
            setValue(cachedValueForSet as valueOfRow<T>);
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

export function RecordCreation (props:{
    record: KvRowAny,
    editMode?: boolean,
    event?:MutableRefObject<{valid?:() => boolean}>
}) {
    const {type, setType, key, setKey, value, setValue,ttl,setTtl} = useBinding(props.record);
    useImperativeHandle(props.event,() => ({
        valid: () => {
            const  validedArr = [
                validKey(),
                validValue()
            ];
            return !validedArr.some(item => item == false);
        }
    }));
    const [keyErrorText, setKeyErrorText] = useState('');
    const validKey = useCallback(() => {
        if(!key) {
            setKeyErrorText('Please input key');
            return false;
        } else {
            setKeyErrorText('');
            return true;
        }
    }, [key]);
    const [strValueErrorText, setStrValueErrorText] = useState('');
    const listValueRef = useRef<{valid?:() => boolean}>({});
    const validValue = useCallback(() => {
        if(type == 'string') {
            if(typeof value == 'string' && !value) {
                setStrValueErrorText('Please input value');
                return false;
            } else {
                setStrValueErrorText('');
                return true;
            }
        } else {
            const validFun = listValueRef?.current?.valid;
            if(!validFun) {
                return true;
            }
            return validFun();
        }
    },[value]);
    return (
        <div>
            <Form
                layout='horizontal'
                labelCol={{span:3}}
                wrapperCol={{span:20}}
            >
                {!props.editMode && <Form.Item label="Type">
                    <Radio.Group value={type} onChange={e => setType(e.target.value)}>
                        <Radio.Button value='string'>string</Radio.Button>
                        <Radio.Button value='list'>list</Radio.Button>
                        <Radio.Button value='hash'>hash</Radio.Button>
                        <Radio.Button value='set'>set</Radio.Button>
                    </Radio.Group>
                </Form.Item>}
                <Form.Item 
                    label="Key"
                    help={keyErrorText}
                    validateStatus={keyErrorText && 'error'}
                >
                    <Input 
                        placeholder='Input key...'
                        onBlur={validKey}
                        value={key}
                        onChange={e => setKey(e.target.value)}
                        disabled={props.editMode}
                    ></Input>
                </Form.Item>
                <Form.Item 
                    label="Value"
                    help={type=='string' && strValueErrorText}
                    validateStatus={(type=='string' && strValueErrorText) ? 'error' : ''}
                >
                    {
                        (() => {
                            switch (type) {
                            case 'string':
                                return <Input
                                    placeholder='Input value...'
                                    onBlur={validValue}
                                    value={value as valueOfRow<'string'>}
                                    onChange={e => setValue(e.target.value)}
                                ></Input>;
                            case 'list':
                                return (<Card key={'list'}>
                                    <ListEditor 
                                        value={value as valueOfRow<'list'>} 
                                        onChange={e => setValue(e)} 
                                        allowDragSorting
                                        allowReproduce
                                        event={listValueRef}
                                    />
                                </Card>
                                );
                            case 'set':
                                return (<Card key={'set'}>
                                    <ListEditor
                                        value={value as valueOfRow<'set'>}
                                        event={listValueRef}
                                    />
                                </Card>
                                );
                            case 'hash':
                                return <Card key={'hash'}>
                                    <HashEditor
                                        value={value as valueOfRow<'hash'>}
                                        event={listValueRef}
                                    />
                                </Card>;
                            default:
                                break;
                            }
                        })()
                    }
                </Form.Item>
                <Form.Item label="TTL">
                    <Space>
                        <Switch defaultChecked={ttl != -1} onChange={e => setTtl(e ? 0 : -1)}></Switch>
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