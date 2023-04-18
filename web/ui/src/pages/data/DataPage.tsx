import React, { useCallback, useEffect, useRef, useState } from 'react';
import axiosService from '../../service/axiosService';
import { KvRow, KvRowAny, kvDataTable } from './entity';
import { Button, Card, Modal, Popconfirm, Space, Table, Tag, Tooltip, notification } from 'antd';
import { ColumnsType, TablePaginationConfig } from 'antd/es/table';
import { EditOutlined, DeleteOutlined, PlusOutlined, ReloadOutlined } from '@ant-design/icons';
import { FilterValue, SorterResult, TableCurrentDataSource } from 'antd/es/table/interface';
import { RecordCreation } from './RecordCreation';
import { DataTypeColor } from '../../common/color';
import { valueOfRow } from '../../types/types';

export const DataPage = function() {
    const [data, setData] = useState<kvDataTable>();
    const [totalCount, setTotalCount] = useState<number>(0);
    const [loading, setLoading] = useState<boolean>(true);
    const [pageSize, setPageSize] = useState<number>(10);
    const [currentPage, setCurrentPage] = useState<number>(1);
    const creationRef = useRef<{valid?:() => boolean}>({});
    const [creationModalOpen, setCreationModalOpen] = useState<boolean>(false);
    const [editMode, setEditMode] = useState<boolean>(false);
    const [creationModalConfirmLoading, setCreationModalConfirmLoading] = useState<boolean>(false);
    const [recordForCreation, setRecordForCreation] = useState<KvRowAny>(new KvRow());
    const [recordForEdit, setRecordForEdit] = useState<KvRowAny | null>(null);
    const [columns, setColumns] = useState<ColumnsType<KvRowAny>>([]);
    useEffect(() => {
        setColumns([
            {
                title: 'Key',
                dataIndex: 'key'
            },
            {
                title: 'Value',
                dataIndex: 'value',
                ellipsis: true,
                render: (rawValue, {type}) => {
                    switch (type) {
                    case 'string':
                        return (<div>{rawValue as valueOfRow<'string'>}</div>);
                    case 'list':
                    case 'set':
                        return (<div>{(rawValue as valueOfRow<'list' | 'set'>).map((value,index) => (
                            <Tag
                                color={DataTypeColor[type]}
                                key={type == 'set' ? value : `${value}-${index}`}
                            >
                                {value}
                            </Tag>
                        ))}</div>);
                    case 'hash':{
                        const children: string[] = [];
                        for (const key in (rawValue as valueOfRow<'hash'>)) {
                            if (Object.prototype.hasOwnProperty.call(rawValue, key)) {
                                const value = rawValue[key];
                                children.push(`${key}: ${value}`);
                            }
                        }
                        return (<div>{(children).map(child => (
                            <Tag color={DataTypeColor[type]} key={child}>{child}</Tag>
                        ))}</div>);
                    }
                    default:
                        return (<div>{JSON.stringify(rawValue)}</div>);
                    }
                }
            },    
            {
                title: 'TTL',
                dataIndex: 'ttl',
                render: value => {
                    let displayValue = `${value}s`;
                    if(value == -1) {
                        displayValue = String.fromCharCode(8734);
                    }
                    return (displayValue);
                }
            },
            {
                title: 'Type',
                dataIndex: 'type',
                render: (type:KvRowAny['type']) => {
                    const color = DataTypeColor[type] || DataTypeColor.string;
                    return <Tag color={color}>{type}</Tag>;
                }
            },
            {
                title: 'Action',
                render: (value, record, index) => {
                    return (<div>
                        <Tooltip title='Edit'>
                            <Button type="link" shape='circle' onClick={() => onEdit(record)}><EditOutlined/></Button>
                        </Tooltip>
                        <Tooltip title='Delete'>
                            <Popconfirm
                                title="Delete record"
                                description="Are you sure to delete this record?"
                                onConfirm={() => onDelete(record)}
                            >
                                <Button type="link" shape='circle' danger><DeleteOutlined/></Button>
                            </Popconfirm>
                        </Tooltip>
                    </div>);
                }
            }
        ]);
    },[]);
    const refresh = useCallback(() => {
        const from = Math.max(0, pageSize * (currentPage - 1));
        const to = Math.max(from, pageSize * currentPage - 1);
        fetchData(from, to);
    },[pageSize,currentPage]);
    useEffect(refresh, [refresh]);
    const fetchData = useCallback(async (from: number, to: number) => {
        setLoading(true);
        const data = await axiosService.getAll(from,to);
        if(data) {
            setData(new kvDataTable(data.data));
            setTotalCount(data.totalCount);
            setLoading(false);
        }
    },[]);
    const onTableChange = useCallback((
        pagination: TablePaginationConfig, 
        // filters: Record<string, FilterValue | null>, 
        // sorter: SorterResult<KvRowAny> | SorterResult<KvRowAny>[], 
        // extra: TableCurrentDataSource<KvRowAny>
    ) => {
        setPageSize(pagination.pageSize || pageSize);
        setCurrentPage(pagination.current || currentPage);
    },[]);
    const onCreate = useCallback(() => {
        setCreationModalOpen(true);
    },[]);
    const onCreationCancel = useCallback(() => {
        setCreationModalOpen(false);
        setCreationModalConfirmLoading(false);
        setEditMode(false);
        setRecordForEdit(null);
    },[]);
    const onCreationConfirm = useCallback(async () => {
        setCreationModalConfirmLoading(true);
        const validFun = creationRef?.current?.valid;
        const valided = validFun && validFun();
        if(!valided) {
            setCreationModalConfirmLoading(false);
            return;
        }
        const edit = editMode;
        const record = edit ? recordForEdit : recordForCreation;
        if(!record) {
            return;
        }
        const key = record.key;
        let success;
        if(edit) {
            success = await axiosService.update(record.getPostBody());
        } else {
            success = await axiosService.create(record.getPostBody());
        }
        setCreationModalConfirmLoading(false);
        if(success) {
            setCreationModalOpen(false);
            setRecordForEdit(null);
            notification.success({
                message: edit ? 'Edit successful' : 'Create successful',
                description: key,
                placement: 'bottomRight'
            });
        }
    },[editMode, recordForEdit, recordForCreation, creationRef]);
    const onEdit = useCallback((record: KvRowAny) => {
        setEditMode(true);
        setCreationModalOpen(true);
        setRecordForEdit(record);
    },[]);
    const onDelete = useCallback(async (record: KvRowAny) => {
        const key = record.key;
        const success = await axiosService.delete(record);
        if(success) {
            notification.success({
                message: 'Delete successful',
                description: key,
                placement: 'bottomRight'
            });
        }
    }, []);
    return (
        <div>
            <Card>
                <div style={{marginBottom: '20px'}}>
                    <Space>
                        <Button onClick={onCreate} ghost type='primary' icon={<PlusOutlined />}>Create</Button>
                        <Button onClick={refresh} ghost type='primary' icon={<ReloadOutlined />}>Refresh</Button>
                    </Space>
                </div>
                <Table
                    columns={columns}
                    dataSource={data?.rows}
                    loading={loading}
                    pagination={data ? {
                        pageSize: pageSize,
                        current: currentPage,
                        pageSizeOptions: Array.from(new Set([pageSize,10,20,50,100])),
                        showSizeChanger: true,
                        total: totalCount
                    } : {}}
                    onChange={onTableChange}
                ></Table>
            </Card>
            <Modal 
                title={editMode ? 'Edit record' : 'Create record'} 
                open={creationModalOpen}
                onCancel={onCreationCancel}
                onOk={onCreationConfirm}
                width="50vw"
                confirmLoading={creationModalConfirmLoading}
                destroyOnClose
            >
                <RecordCreation
                    event={creationRef}
                    record={editMode && recordForEdit ? recordForEdit : recordForCreation}
                    editMode={editMode}
                ></RecordCreation>
            </Modal>
        </div>
    );
};