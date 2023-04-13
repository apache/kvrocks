import { useCallback, useEffect, useState } from 'react';
import { DndTable, RowType as DndTableRowType } from './DndTable';
import { getRandomString } from '../common/util';
import { Button, Input, Table } from 'antd';
import { ColumnsType } from 'antd/es/table/interface';
import { DeleteOutlined,PlusOutlined } from '@ant-design/icons';

interface RowType extends DndTableRowType{
    key: string,
    value: string
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
            value: value
        };
    });
}
export function ListEditor(props: {
    value: string[],
    onChange: (list:string[]) => any,
    allowDragSorting?: boolean,
}) {
    const [dataSource, setDataSource] = useState<RowType[]>(transformValueToRowType(props.value));
    const columns: ColumnsType<RowType>=[
        {
            dataIndex: 'value',
            render: (value, record, index) => (<Input
                defaultValue={value}
                onChange={e => {
                    record.value = e.target.value;
                    props.value[index] = record.value;
                }}
            ></Input>)
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
        props.onChange(data.map(item => item.value));
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
            ? (props.allowDragSorting
                ? 
                <DndTable
                    showHeader={false}
                    columns={columns}
                    dataSource={dataSource}
                    pagination={false}
                    onDataChange={onTableDataChange}
                ></DndTable>
                : 
                <Table
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