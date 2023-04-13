import React, { useCallback, useEffect, useRef, useState } from 'react';
import axiosService from '../../service/axiosService';
import { KvRow, RowType, kvDataTable } from './entity';
import { Button, Card, Modal, Space, Table, Tag, Tooltip, notification } from 'antd';
import { ColumnsType, TablePaginationConfig } from 'antd/es/table';
import { EditOutlined, DeleteOutlined, PlusOutlined, ReloadOutlined } from '@ant-design/icons';
import { FilterValue, SorterResult, TableCurrentDataSource } from 'antd/es/table/interface';
import { RecordCreation } from './RecordCreation';

const columns:ColumnsType<RowType> = [
    {
        title: 'Key',
        dataIndex: 'key'
    },
    {
        title: 'Value',
        dataIndex: 'value',
        ellipsis: true
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
        render: (value:RowType['type']) => {
            let color = 'green';
            switch (value) {
            case 'string':
                color = 'green';
                break;
            case 'list':
                color = 'cyan';
                break;
            case 'hash':
                color = 'blue';
                break;
            case 'set':
                color = 'purple';
                break;
            
            default:
                break;
            }
            return <Tag color={color}>{value}</Tag>;
        }
    },
    {
        title: 'Action',
        render: (value, record, index) => {
            return (<div>
                <Tooltip title='Edit'>
                    <Button type="link" shape='circle' ><EditOutlined/></Button>
                </Tooltip>
                <Tooltip title='Delete'>
                    <Button type="link" shape='circle' danger><DeleteOutlined/></Button>
                </Tooltip>
            </div>);
        }
    }
];

export const DataPage = function() {
    const [data, setData] = useState<kvDataTable>();
    const [totalCount, setTotalCount] = useState<number>(0);
    const [loading, setLoading] = useState<boolean>(true);
    const [pageSize, setPageSize] = useState<number>(10);
    const [currentPage, setCurrentPage] = useState<number>(1);
    const creationRef = useRef<{valid?:() => boolean}>({});
    const [creationModalOpen, setCreationModalOpen] = useState<boolean>(false);
    const [creationModalConfirmLoading, setCreationModalConfirmLoading] = useState<boolean>(false);
    const [recordForCreation, setRecordForCreation] = useState<KvRow>(new KvRow());
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
        filters: Record<string, FilterValue | null>, 
        sorter: SorterResult<RowType> | SorterResult<RowType>[], 
        extra: TableCurrentDataSource<RowType>
    ) => {
        setPageSize(pagination.pageSize || pageSize);
        setCurrentPage(pagination.current || currentPage);
    },[]);
    const onCreate = useCallback(() => {
        setCreationModalOpen(true);
    },[]);
    const onCreationCancel = useCallback(() => {
        setCreationModalOpen(false);
    },[]);
    const onCreationConfirm = useCallback(async () => {
        setCreationModalConfirmLoading(true);
        const validFun = creationRef?.current?.valid;
        const valided = validFun && validFun();
        if(!valided) {
            setCreationModalConfirmLoading(false);
            return;
        }
        const key = recordForCreation.key;
        const success = await axiosService.create(recordForCreation.getPostBody());
        setCreationModalConfirmLoading(false);
        if(success) {
            setCreationModalOpen(false);
            notification.success({
                message: 'Create successful',
                description: key,
                placement: 'bottomRight'
            });
        }
    },[]);
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
                title="Create record" 
                open={creationModalOpen}
                onCancel={onCreationCancel}
                onOk={onCreationConfirm}
                width="50vw"
                confirmLoading={creationModalConfirmLoading}
            >
                <RecordCreation event={creationRef} record={recordForCreation} ></RecordCreation>
            </Modal>
        </div>
    );
};