import { RowData, RowDataAny, typeOfRow, valueOfRow } from '../../types/types';

export type KvRowAny = KvRow<typeOfRow>;
export class KvRow<T extends typeOfRow> implements RowData<T> {
    key: string;
    value: valueOfRow<T>;
    type: T;
    ttl: number;
    constructor(row?: RowData<T>) {
        if(!row) {
            this.key = '';
            this.type = 'string' as T;
            this.value = '' as valueOfRow<T>;
            this.ttl = -1;
            return;
        }
        this.key = row.key;
        this.type = row.type;
        this.ttl = row.ttl;
        this.value = row.value;
    }
    generateDefaultValueForType() {
        switch (this.type) {
        case 'string':
            (this as RowData<'string'>).value = '';
            break;
        case 'list':
        case 'set':
            (this as RowData<'list' | 'set'>).value = [];
            break;
        case 'hash':
            (this as RowData<'hash'>).value = {};
            break;
        }
    }
    getPostBody(): RowData<T> {
        const result = {
            key: this.key,
            type: this.type,
            value: this.value,
            ttl: this.ttl
        };
        return result;
    }
}

export class kvDataTable {
    rows: KvRow<any>[];
    constructor(data: RowDataAny[]) {
        this.rows = data.map(r => new KvRow(r));
    }
}