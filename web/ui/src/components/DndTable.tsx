import { Table } from 'antd';
import { MenuOutlined } from '@ant-design/icons';
import type { DragEndEvent } from '@dnd-kit/core';
import { DndContext } from '@dnd-kit/core';
import {
    arrayMove,
    SortableContext,
    useSortable,
    verticalListSortingStrategy,
} from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import type { ColumnsType, ColumnType, TableProps } from 'antd/es/table';
import React, { useEffect, useState } from 'react';

interface RowProps extends React.HTMLAttributes<HTMLTableRowElement> {
    'data-row-key': string;
}
const Row = ({ children, ...props }: RowProps) => {
    const {
        attributes,
        listeners,
        setNodeRef,
        setActivatorNodeRef,
        transform,
        transition,
        isDragging,
    } = useSortable({
        id: props['data-row-key'],
    });

    const style: React.CSSProperties = {
        ...props.style,
        transform: CSS.Transform.toString(transform && { ...transform, scaleY: 1 })?.replace(
            /translate3d\(([^,]+),/,
            'translate3d(0,',
        ),
        transition,
        ...(isDragging ? { position: 'relative', zIndex: 9999 } : {}),
    };

    return (
        <tr {...props} ref={setNodeRef} style={style} {...attributes}>
            {React.Children.map(children, (child) => {
                if ((child as React.ReactElement).key === '__sort_icon_in_dnd__') {
                    return React.cloneElement(child as React.ReactElement, {
                        children: (
                            <MenuOutlined
                                ref={setActivatorNodeRef}
                                style={{ touchAction: 'none', cursor: 'move' }}
                                {...listeners}
                            />
                        ),
                    });
                }
                return child;
            })}
        </tr>
    );
};
export interface RowType{
    key: string,
    [key: string]: any
}
interface DndTableProps<RT> extends TableProps<RT>{
    onDataChange?: (data: RT[]) => void
}
export function DndTable<RT extends RowType> (props: DndTableProps<RT>) {
    const [dataSource, setDataSource] = useState<RT[]>(props.dataSource? [...props.dataSource]:[]);
    const [columns, setColumns] = useState<ColumnsType<RT>>([]);
    useEffect(() => {
        const addCol:ColumnType<RT>={
            key: '__sort_icon_in_dnd__',
            width: '50px'
        };
        const col:ColumnsType<RT> = [addCol,...(props.columns || [])];
        setColumns(col);
    },[props.columns]);
    useEffect(() => setDataSource(props.dataSource? [...props.dataSource]:[]),[props.dataSource]);
    const onDragEnd = ({ active, over }: DragEndEvent) => {
        if (active.id !== over?.id) {
            setDataSource((previous) => {
                const activeIndex = previous.findIndex((i) => i.key === active.id);
                const overIndex = previous.findIndex((i) => i.key === over?.id);
                const valueToSet = arrayMove(previous, activeIndex, overIndex);
                props.onDataChange && props.onDataChange(valueToSet);
                return valueToSet;
            });
        }
    };
    return (<div>
        {(dataSource && dataSource.length)
            ? <DndContext onDragEnd={onDragEnd}>
                <SortableContext
                // rowKey array
                    items={dataSource.map((i) => i.key)}
                    strategy={verticalListSortingStrategy}
                >
                    <Table
                        {...props}
                        components={{
                            body: {
                                row: Row,
                            },
                        }}
                        rowKey="key"
                        columns={columns}
                        dataSource={dataSource}
                    />
                </SortableContext>
            </DndContext>
            : null}
    </div>);
}