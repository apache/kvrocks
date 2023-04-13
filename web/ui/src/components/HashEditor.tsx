import { MutableRefObject, useCallback, useEffect, useState } from 'react';
import { RowType as DndTableRowType } from './DndTable';
import { getRandomString } from '../common/util';
import { Button, Input, Table } from 'antd';
import { ColumnsType } from 'antd/es/table/interface';
import { DeleteOutlined,PlusOutlined } from '@ant-design/icons';
import { HashRow } from '../types/types';

interface RowType extends DndTableRowType{
    key: string,
    value: string,
    _row_key: string,
}
function transformValueToRowType(values: HashRow['value'], previousDataSource?: RowType[]): RowType[] {
    const result:RowType[] = [];
    const keys: string[] = [];
    values = JSON.parse(JSON.stringify(values));
    if(previousDataSource) {
        previousDataSource.forEach(data => {
            if(data.key in values && values[data.key] == data.value) {
                result.push({
                    key: data.key,
                    value: values[data.key],
                    _row_key: data._row_key
                });
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
                _row_key: uniqueKey
            });
        }
    }
    return result;
}
export function HashEditor(props: {
    value: HashRow['value'],
    event?: MutableRefObject<{valid?:() => boolean}>
}) {
    const [dataSource, setDataSource] = useState<RowType[]>([]);
    const [disableAddBtn, setDisableAddBtn] = useState(false);
    const checkAddBtnDisable = useCallback(() => {
        setDisableAddBtn('' in props.value);
    }, [props.value]);
    const columns: ColumnsType<RowType>=[
        {
            dataIndex: 'key',
            render: (key, record) => (<Input
                placeholder='field'
                key={record._row_key}
                defaultValue={key}
                onChange={e => {
                    delete props.value[record.key];
                    record.key = e.target.value;
                    props.value[record.key] = record.value;
                }}
                onBlur={checkAddBtnDisable}
                onMouseLeave={checkAddBtnDisable}
            ></Input>)
        },{
            dataIndex: 'value',
            render: (value, record) => (<Input
                placeholder='value'
                key={record._row_key}
                defaultValue={value}
                onChange={e => {
                    record.value = e.target.value;
                    props.value[record.key] = record.value;
                }}
            ></Input>)
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