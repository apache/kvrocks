import React, { MutableRefObject, useCallback, useEffect, useImperativeHandle, useState } from 'react';
import { DndTable, RowType as DndTableRowType } from './DndTable';
import { getRandomString } from '../common/util';
import { Button, Form, Input, Table } from 'antd';
import { ColumnsType } from 'antd/es/table/interface';
import { DeleteOutlined,PlusOutlined } from '@ant-design/icons';

interface RowType extends DndTableRowType{
    key: string,
    value: string,
    errMsg: string,
    valid?: () => boolean,
}
function transformValueToRowType(values: string[], previousDataSource?: RowType[]): RowType[] {
    const keys: string[] = [];
    return values.map(value => {
        const exists = previousDataSource?.find(p => p.value == value);
        let uniqueKey = exists?.key;
        while (!uniqueKey || keys.includes(uniqueKey)) {
            uniqueKey = getRandomString(32);
        }
        keys.push(uniqueKey);
        return {
            key:uniqueKey,
            value: value,
            errMsg: exists?.errMsg || '',
        };
    });
}


interface CellProps {
    children: React.ReactNode,
    isValueColumn: boolean
    record: RowType,
    index: number,
    handleValueChange: (newValue: string) => void
}
const Cell: React.FC<CellProps> = ({
    children,
    record,
    index,
    isValueColumn,
    handleValueChange,
    ...restProps
}) => {
    if(!isValueColumn) {
        return (<td {...restProps}>
            {children}
        </td>);
    }
    const [errMsg, setErrMsg] = useState(record.errMsg);
    const valid = useCallback(() => {
        if(record.value) {
            record.errMsg = '';
        } else {
            record.errMsg = 'Please input value';
        }
        if(record.errMsg != errMsg) {
            setErrMsg(record.errMsg);
        }
        return record.errMsg == '';
    },[record.value]);
    record.valid = valid;
    return (<td {...restProps}>
        <Form.Item 
            style={{margin: 0}}
            help={errMsg}
            validateStatus={errMsg ? 'error' : ''}
        >
            <Input
                defaultValue={record.value}
                onChange={e => handleValueChange(e.target.value)}
                onBlur={valid}
            ></Input>
        </Form.Item>
    </td>);
};
export function ListEditor(props: {
    value: string[],
    onChange?: (list:string[]) => any,
    allowDragSorting?: boolean,
    allowReproduce?: boolean,
    event?: MutableRefObject<{valid?:() => boolean}>
}) {
    useImperativeHandle(props.event, () => ({
        valid: () => dataSource.map(d => d.valid ? d.valid() : true).filter(d => !d).length == 0
    }));
    const [dataSource, setDataSource] = useState<RowType[]>(transformValueToRowType(props.value));
    const columns: ColumnsType<RowType>=[
        {
            dataIndex: 'value',
            onCell: (record: RowType, index: number | undefined) => ({
                record: record,
                index: index,
                isValueColumn: true,
                handleValueChange: (newValue: string) => {
                    record.value = newValue;
                    props.value[index as number] = newValue;
                }
            } as any)
        },{
            width: '50px',
            render: (value, record, index) => (<Button 
                type='link'
                danger
                onClick={() => onDelete(index)}
            >
                <DeleteOutlined />
            </Button>)
        }
    ];
    const updateDataSource = useCallback(() => {
        setDataSource(previous => transformValueToRowType(props.value, previous));
    },[props.value]);
    useEffect(updateDataSource,[props.value]);
    const onTableDataChange = useCallback((data: RowType[]) => {
        props.onChange && props.onChange(data.map(item => item.value));
    },[]);
    const onDelete = useCallback((index: number) => {
        props.value.splice(index,1);
        updateDataSource();
    }, [props.value]);
    const onAdd = useCallback(() => {
        props.value.push('');
        updateDataSource();
    }, [props.value]);
    return (<div>
        { dataSource && dataSource.length 
            ? (
                props.allowDragSorting
                    ? 
                    <DndTable
                        components={{
                            body: {
                                cell: Cell
                            }
                        }}
                        showHeader={false}
                        columns={columns}
                        dataSource={dataSource}
                        pagination={false}
                        onDataChange={onTableDataChange}
                    ></DndTable>
                    : 
                    <Table
                        components={{
                            body: {
                                cell: Cell
                            }
                        }}
                        showHeader={false}
                        columns={columns}
                        dataSource={dataSource}
                        pagination={false}
                    ></Table>
            )
            : null  }
        <Button
            icon={<PlusOutlined />}
            style={{marginTop: '10px'}}
            onClick={onAdd}
        >
            Add
        </Button>
    </div>);
}