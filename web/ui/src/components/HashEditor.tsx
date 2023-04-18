import React, { MutableRefObject, useCallback, useEffect, useImperativeHandle, useState } from 'react';
import { RowType as DndTableRowType } from './DndTable';
import { getRandomString } from '../common/util';
import { Button, Form, Input, Table } from 'antd';
import { ColumnsType } from 'antd/es/table/interface';
import { DeleteOutlined,PlusOutlined } from '@ant-design/icons';
import { valueOfRow } from '../types/types';

interface RowType extends DndTableRowType{
    key: string,
    value: string,
    _row_key: string,
    errMsgOnKey: string,
    errMsgOnValue: string,
    validKey?: () => boolean,
    validValue?: () => boolean,
}
function transformValueToRowType(values: valueOfRow<'hash'>, previousDataSource?: RowType[]): RowType[] {
    const result:RowType[] = [];
    const keys: string[] = [];
    values = JSON.parse(JSON.stringify(values));
    if(previousDataSource) {
        previousDataSource.forEach(data => {
            if(data.key in values && values[data.key] == data.value) {
                result.push(data);
                delete values[data.key];
            }
        });
    }
    for (const key in values) {
        if (Object.prototype.hasOwnProperty.call(values, key)) {
            const value = values[key];
            let uniqueKey: string = previousDataSource?.find(item => item.key == key && item.value == value)?._row_key || '';
            while (!uniqueKey || keys.includes(uniqueKey)) {
                uniqueKey = getRandomString(32);
            }
            keys.push(uniqueKey);
            result.push({
                key,
                value,
                errMsgOnKey: '',
                errMsgOnValue: '',
                _row_key: uniqueKey
            });
        }
    }
    return result;
}
interface CellProps {
    children: React.ReactNode,
    isKeyColumn: boolean,
    isValueColumn: boolean,
    record: RowType,
    index: number,
    allDataSource: RowType[],
    checkAddBtnDisable: () => void,
    handleValueChange: (newValue: string) => void,
    handleKeyChange: (newKey: string, ondKey: string) => void,
}
const Cell: React.FC<CellProps> = ({
    children,
    record,
    index,
    isKeyColumn,
    isValueColumn,
    handleValueChange,
    handleKeyChange,
    checkAddBtnDisable,
    allDataSource,
    ...restProps
}) => {
    if(!isValueColumn && !isKeyColumn) {
        return (<td {...restProps}>
            {children}
        </td>);
    }
    const [errMsgOnKey, setErrMsgOnKey] = useState(record.errMsgOnKey);
    const [errMsgOnValue, setErrMsgOnValue] = useState(record.errMsgOnValue);
    const validValue = useCallback(() => {
        if(record.value) {
            record.errMsgOnValue = '';
        } else {
            record.errMsgOnValue = 'Please input value';
        }
        if(record.errMsgOnValue != errMsgOnValue) {
            setErrMsgOnValue(record.errMsgOnValue);
        }
        return record.errMsgOnValue == '';
    },[record.value, errMsgOnValue]);
    const validKey = useCallback(() => {
        if(!record.key) {
            record.errMsgOnKey = 'Please input field';
        } else if (allDataSource.filter(d => d.key == record.key).length > 1) {
            record.errMsgOnKey = 'Duplicate key';
        } else {
            record.errMsgOnKey = '';
        }
        if(record.errMsgOnKey != errMsgOnKey) {
            setErrMsgOnKey(record.errMsgOnKey);
        }
        return record.errMsgOnKey == '';
    },[record.key,errMsgOnKey]);
    isValueColumn && (record.validValue = validValue);
    isKeyColumn && (record.validKey = validKey);
    return (<td {...restProps}>
        { isKeyColumn && 
            <Form.Item 
                style={{margin: 0}}
                help={errMsgOnKey}
                validateStatus={errMsgOnKey ? 'error' : ''}
            >
                <Input
                    placeholder='field'
                    key={record._row_key}
                    defaultValue={record.key}
                    onChange={e => handleKeyChange(e.target.value, record.key)}
                    onBlur={checkAddBtnDisable}
                    onMouseLeave={checkAddBtnDisable}
                ></Input>
            </Form.Item>}
        { isValueColumn && 
            <Form.Item 
                style={{margin: 0}}
                help={errMsgOnValue}
                validateStatus={errMsgOnValue ? 'error' : ''}
            >
                <Input
                    placeholder='value'
                    key={record._row_key}
                    defaultValue={record.value}
                    onChange={e => handleValueChange(e.target.value)}
                ></Input>
            </Form.Item>}
    </td>);
};
export function HashEditor(props: {
    value: valueOfRow<'hash'>,
    event?: MutableRefObject<{valid?:() => boolean}>
}) {
    const [dataSource, setDataSource] = useState<RowType[]>([]);
    const [disableAddBtn, setDisableAddBtn] = useState(false);
    const checkAddBtnDisable = useCallback(() => {
        setDisableAddBtn('' in props.value);
    }, [props.value]);
    useImperativeHandle(props.event, () => ({
        valid: () => dataSource.map(d => {
            if(!(d.key in props.value)) {
                props.value[d.key] = d.value;
            }
            const keyValied = (d.validKey ? d.validKey() : true);
            const valueValied = (d.validValue ? d.validValue() : true);
            return keyValied && valueValied;
        }).filter(d => !d).length == 0
    }));
    const columns: ColumnsType<RowType>=[
        {
            dataIndex: 'key',
            onCell: (record: RowType, index: number | undefined) => ({
                record: record,
                index: index,
                isKeyColumn: true,
                allDataSource: dataSource,
                checkAddBtnDisable: checkAddBtnDisable,
                handleKeyChange: (newKey: string, oldKey: string) => {
                    delete props.value[oldKey];
                    record.key = newKey;
                    props.value[newKey] = record.value;
                }
            } as any)
        },{
            dataIndex: 'value',
            onCell: (record: RowType, index: number | undefined) => ({
                record: record,
                index: index,
                isValueColumn: true,
                handleValueChange: (newValue: string) => {
                    record.value = newValue;
                    props.value[record.key] = record.value;
                }
            } as any)
        },{
            width: '50px',
            render: (value, record) => (<Button 
                type='link'
                danger
                onClick={() => onDelete(record.key)}
            >
                <DeleteOutlined />
            </Button>)
        }
    ];
    const updateDataSource = useCallback(() => {
        setDataSource(transformValueToRowType(props.value, dataSource));
    },[props.value, dataSource]);
    useEffect(updateDataSource,[props.value]);
    const onDelete = useCallback((key: string) => {
        delete props.value[key];
        updateDataSource();
        checkAddBtnDisable();
    }, [props.value]);
    const onAdd = useCallback(() => {
        setDisableAddBtn(true);
        props.value[''] = '';
        updateDataSource();
    }, [props.value]);
    return (<div>
        { dataSource && dataSource.length 
            ? 
            <Table
                showHeader={false}
                columns={columns}
                dataSource={dataSource}
                pagination={false}
                rowKey={'_row_key'}
                components={{
                    body: {
                        cell: Cell
                    }
                }}
            ></Table>
            : null  }
        <Button
            icon={<PlusOutlined />}
            style={{marginTop: '10px'}}
            onClick={onAdd}
            disabled={disableAddBtn}
        >
            Add
        </Button>
    </div>);
}